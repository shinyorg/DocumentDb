using Json.Schema;

namespace Shiny.DocumentDb;

/// <summary>
/// Validates the exact JSON about to be persisted (<c>ctx.GetJson()</c>) against the document type's mapped
/// JSON Schema, on <c>BeforeWrite</c>. Throwing aborts the write and rolls back the surrounding unit.
/// Deletes (no document) and unmapped types pass through. Set-based writes never materialize a document, so
/// they are out of scope by nature.
/// </summary>
sealed class JsonSchemaInterceptor : IDocumentInterceptor
{
    readonly JsonSchemaOptions options;
    readonly EvaluationOptions evaluationOptions;

    public JsonSchemaInterceptor(JsonSchemaOptions options)
    {
        this.options = options;
        // Always evaluate format so we can produce format errors; whether they COUNT is decided per-write
        // by EnableFormatAssertion (json-everything asserts known formats regardless of this flag, so the
        // opt-out is implemented by filtering format errors rather than by the evaluator option).
        this.evaluationOptions = new EvaluationOptions
        {
            OutputFormat = OutputFormat.List,
            RequireFormatValidation = true
        };
    }

    public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
    {
        if (ctx.Operation == DocumentOperation.Delete)
            return Task.CompletedTask;

        var schema = this.options.Resolve(ctx.DocumentType);
        if (schema == null)
            return Task.CompletedTask;

        using var doc = ctx.GetJsonDocument();
        if (doc == null)
            return Task.CompletedTask;

        var results = schema.Evaluate(doc.RootElement, this.evaluationOptions);
        if (results.IsValid)
            return Task.CompletedTask;

        var errors = Collect(results);
        if (!this.options.EnableFormatAssertion)
            errors = errors.Where(e => !string.Equals(e.Keyword, "format", StringComparison.OrdinalIgnoreCase)).ToList();

        if (errors.Count > 0)
            throw new DocumentSchemaValidationException(ctx.DocumentType, ctx.TypeName, errors);

        return Task.CompletedTask;
    }

    public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;

    static IReadOnlyList<SchemaValidationError> Collect(EvaluationResults results)
    {
        var errors = new List<SchemaValidationError>();
        Walk(results, errors);
        return errors;
    }

    static void Walk(EvaluationResults node, List<SchemaValidationError> errors)
    {
        if (!node.IsValid && node.Errors is { } nodeErrors)
        {
            var location = node.InstanceLocation.ToString();
            foreach (var kv in nodeErrors)
                errors.Add(new SchemaValidationError(location, kv.Key, kv.Value));
        }
        if (node.Details is { } details)
        {
            foreach (var detail in details)
                Walk(detail, errors);
        }
    }
}
