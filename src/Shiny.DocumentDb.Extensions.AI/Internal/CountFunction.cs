using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

sealed class CountFunction<T> : DocumentAIFunctionBase<T> where T : class
{
    public CountFunction(
        IDocumentStore store,
        DocumentAITypeRegistration<T> registration,
        IReadOnlyList<DocumentField> fields,
        string name,
        string description,
        JsonElement schema)
        : base(store, registration, fields, name, description, schema) { }

    protected override async ValueTask<object?> InvokeCoreAsync(
        AIFunctionArguments arguments,
        CancellationToken cancellationToken)
    {
        arguments.TryGetValue("filter", out var filterRaw);
        var filter = filterRaw is JsonElement je ? (JsonElement?)je : null;

        var query = this.Store.Query(this.Registration.JsonTypeInfo);
        var predicate = FilterTranslator.Translate<T>(filter, this.Fields);
        if (predicate != null)
            query = query.Where(predicate);

        var count = await query.Count(cancellationToken).ConfigureAwait(false);
        return new { count };
    }

    public static JsonElement BuildSchema(IReadOnlyList<DocumentField> fields)
    {
        var node = new JsonObject
        {
            ["type"] = "object",
            ["properties"] = new JsonObject
            {
                ["filter"] = SchemaBuilder.BuildFilterSchema(fields)
            }
        };
        return SchemaBuilder.ToJsonElement(node);
    }
}
