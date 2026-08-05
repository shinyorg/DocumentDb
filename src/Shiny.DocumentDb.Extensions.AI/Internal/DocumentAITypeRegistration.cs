using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

/// <summary>
/// Frozen, type-erased configuration produced by the builder once <c>AddType</c> / <c>AddCollection</c>
/// completes. One subclass per lane: a CLR document type, or a schema-free JSON collection.
/// </summary>
abstract class DocumentAIRegistration
{
    public required string Slug { get; init; }
    public required DocumentAICapabilities Capabilities { get; init; }

    /// <summary>
    /// Services a <c>Where&lt;TService&gt;</c> filter will resolve per call. Recorded so a host can assert at
    /// startup that each one is registered, instead of discovering it as a failed tool call in production.
    /// </summary>
    public required IReadOnlyList<Type> RequiredServiceTypes { get; init; }

    public abstract IEnumerable<global::Microsoft.Extensions.AI.AITool> CreateTools(IDocumentStore store);

    /// <summary>
    /// Type-erased metadata about this registration, for hosts that need to <b>describe</b> it rather than
    /// call it — the MCP server's <c>documentdb://types</c> and <c>.../schema</c> resources. It exposes only
    /// what the model is already allowed to see: never the scope predicates, never an ignored property.
    /// </summary>
    public abstract DocumentAIDescriptor Describe();
}

/// <summary>What a host may say about a registration without reaching into its generics.</summary>
sealed record DocumentAIDescriptor(
    string Slug,
    string Kind,
    string? Description,
    DocumentAICapabilities Capabilities,
    JsonElement DocumentSchema,
    IReadOnlyList<string> Fields,
    int MaxPageSize,
    bool IsScoped
);

abstract class DocumentAITypeRegistration : DocumentAIRegistration
{
    public required Type DocumentType { get; init; }
}

sealed class DocumentAITypeRegistration<T> : DocumentAITypeRegistration where T : class
{
    public required JsonTypeInfo<T> JsonTypeInfo { get; init; }
    public string? TypeDescriptionOverride { get; init; }
    public required IReadOnlyDictionary<string, string> PropertyDescriptionOverrides { get; init; }
    public required IReadOnlyList<string>? AllowedProperties { get; init; }
    public required IReadOnlyList<string>? IgnoredProperties { get; init; }

    /// <summary>
    /// Fixed, non-removable predicates registered via <see cref="IDocumentAITypeBuilder{T}.Where(Expression{Func{T, bool}})"/>. AND-combined
    /// and applied to every tool for this type. Empty when none were registered.
    /// </summary>
    public required IReadOnlyList<Expression<Func<T, bool>>> Filters { get; init; }

    /// <summary>
    /// Filters resolved from the call's <c>IServiceProvider</c> on every invocation — the per-caller scope.
    /// Empty for a registration that only uses the static form, which then pays nothing.
    /// </summary>
    public required IReadOnlyList<DynamicDocumentFilter<T>> DynamicFilters { get; init; }
    public required int MaxPageSize { get; init; }

    public override IEnumerable<global::Microsoft.Extensions.AI.AITool> CreateTools(IDocumentStore store)
        => DocumentAIFunctionFactory.Build(store, this);

    public override DocumentAIDescriptor Describe()
    {
        var fields = SchemaBuilder.BuildFields(this);
        return new DocumentAIDescriptor(
            this.Slug,
            "type",
            SchemaBuilder.ResolveTypeDescription(this),
            this.Capabilities,
            SchemaBuilder.BuildDocumentSchema(fields, SchemaBuilder.ResolveTypeDescription(this)),
            fields.Select(f => f.JsonName).ToArray(),
            this.MaxPageSize,
            this.Filters.Count > 0 || this.DynamicFilters.Count > 0
        );
    }
}
