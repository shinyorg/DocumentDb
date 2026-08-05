using System.Text.Json;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

/// <summary>
/// Frozen configuration for a schema-free JSON collection exposed to the LLM.
/// </summary>
sealed class DocumentAICollectionRegistration : DocumentAIRegistration
{
    public required string CollectionName { get; init; }
    public required string IdProperty { get; init; }
    public required string? Description { get; init; }

    /// <summary>The declared fields, or <c>null</c> when the registration opted into <c>AllowAnyField</c>.</summary>
    public required IReadOnlyList<JsonCollectionField>? Fields { get; init; }

    /// <summary>
    /// Fixed, non-removable string-grammar clauses. AND-combined into every read the tools perform. Empty
    /// when none were registered — and never non-empty alongside an insert/update capability, which the
    /// builder refuses.
    /// </summary>
    public required IReadOnlyList<string> Filters { get; init; }

    /// <summary>
    /// Clauses resolved from the call's <c>IServiceProvider</c> on every invocation. Same read-only
    /// restriction as <see cref="Filters"/>.
    /// </summary>
    public required IReadOnlyList<DynamicCollectionFilter> DynamicFilters { get; init; }
    public required int MaxPageSize { get; init; }

    IReadOnlyList<DocumentField>? schemaFields;

    /// <summary>The declared fields projected onto the shared schema shape, or <c>null</c> in open mode.</summary>
    public IReadOnlyList<DocumentField>? SchemaFields
        => this.schemaFields ??= this.Fields?.Select(f => f.ToDocumentField()).ToArray();

    public bool HasFilters => this.Filters.Count > 0 || this.DynamicFilters.Count > 0;

    /// <summary>Opens the collection this registration points at.</summary>
    public IJsonDocumentCollection Open(IDocumentStore store)
        => store.Collection(this.CollectionName, this.IdProperty);

    /// <summary>
    /// Resolves a model-supplied field name to its declaration, or throws when it was never declared. In
    /// open mode there is nothing to resolve — the path is validated and returned untyped.
    /// </summary>
    public JsonCollectionField ResolveField(string path)
    {
        if (this.Fields is null)
            return new JsonCollectionField(PathGuard.Validate(path), null, null);

        foreach (var f in this.Fields)
        {
            if (string.Equals(f.Path, path, StringComparison.Ordinal))
                return f;
        }
        throw new InvalidOperationException($"Field '{path}' is not allowed by the tool configuration.");
    }

    public override IEnumerable<global::Microsoft.Extensions.AI.AITool> CreateTools(IDocumentStore store)
        => JsonCollectionFunctionFactory.Build(store, this);

    public override DocumentAIDescriptor Describe() => new(
        this.Slug,
        "collection",
        this.Description,
        this.Capabilities,
        SchemaBuilder.BuildDocumentSchema(this.SchemaFields, this.Description),
        this.Fields?.Select(f => f.Path).ToArray() ?? Array.Empty<string>(),
        this.MaxPageSize,
        this.HasFilters
    );
}

/// <summary>
/// One declared field on a schema-free collection. <see cref="Type"/> is <c>null</c> only in open mode,
/// where nothing declares it and the grammar infers from the supplied value instead.
/// </summary>
readonly record struct JsonCollectionField(string Path, DocumentFieldType? Type, string? Description)
{
    public DocumentField ToDocumentField()
        => new(this.Path, DocumentFieldTypes.ToClrType(this.Type), ClrProperty: null, this.Description);

    /// <summary>The grammar path, carrying a <c>:type</c> hint when one is known and worth emitting.</summary>
    public string HintedPath
    {
        get
        {
            var hint = DocumentFieldTypes.ToGrammarHint(this.Type);
            return hint is null ? this.Path : $"{this.Path}:{hint}";
        }
    }

    public bool IsNumeric => this.Type is DocumentFieldType.Number or DocumentFieldType.Int
        or DocumentFieldType.Long or DocumentFieldType.Double or DocumentFieldType.Decimal;
}

static class DocumentFieldTypes
{
    /// <summary>The CLR type used to describe the field in JSON schema and to bind comparison values.</summary>
    public static Type ToClrType(DocumentFieldType? type) => type switch
    {
        DocumentFieldType.Number or DocumentFieldType.Double => typeof(double),
        DocumentFieldType.Int => typeof(int),
        DocumentFieldType.Long => typeof(long),
        DocumentFieldType.Decimal => typeof(decimal),
        DocumentFieldType.Bool => typeof(bool),
        DocumentFieldType.Date => typeof(DateTimeOffset),
        DocumentFieldType.Guid => typeof(Guid),
        _ => typeof(string)
    };

    /// <summary>
    /// The grammar's <c>:type</c> suffix, or <c>null</c> when none should be emitted — an undeclared field
    /// (open mode) and a string field both extract as the plain untyped path.
    /// </summary>
    public static string? ToGrammarHint(DocumentFieldType? type) => type switch
    {
        DocumentFieldType.Number => "number",
        DocumentFieldType.Int => "int",
        DocumentFieldType.Long => "long",
        DocumentFieldType.Double => "double",
        DocumentFieldType.Decimal => "decimal",
        DocumentFieldType.Bool => "bool",
        DocumentFieldType.Date => "date",
        DocumentFieldType.Guid => "guid",
        _ => null
    };

    /// <summary>Parses the hint the LLM may supply in open mode. Unknown or absent hints mean "infer".</summary>
    public static DocumentFieldType? FromGrammarHint(string? hint) => hint?.ToLowerInvariant() switch
    {
        "string" => DocumentFieldType.String,
        "number" => DocumentFieldType.Number,
        "int" => DocumentFieldType.Int,
        "long" => DocumentFieldType.Long,
        "double" => DocumentFieldType.Double,
        "decimal" => DocumentFieldType.Decimal,
        "bool" => DocumentFieldType.Bool,
        "date" => DocumentFieldType.Date,
        "guid" => DocumentFieldType.Guid,
        _ => null
    };
}
