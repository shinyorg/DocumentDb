using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

/// <summary>
/// Frozen, type-erased configuration produced by the builder once <c>AddType</c> completes.
/// </summary>
abstract class DocumentAITypeRegistration
{
    public required string Slug { get; init; }
    public required DocumentAICapabilities Capabilities { get; init; }
    public required Type DocumentType { get; init; }

    public abstract IEnumerable<global::Microsoft.Extensions.AI.AITool> CreateTools(IDocumentStore store);
}

sealed class DocumentAITypeRegistration<T> : DocumentAITypeRegistration where T : class
{
    public required JsonTypeInfo<T> JsonTypeInfo { get; init; }
    public string? TypeDescriptionOverride { get; init; }
    public required IReadOnlyDictionary<string, string> PropertyDescriptionOverrides { get; init; }
    public required IReadOnlyList<string>? AllowedProperties { get; init; }
    public required IReadOnlyList<string>? IgnoredProperties { get; init; }
    public required int MaxPageSize { get; init; }

    public override IEnumerable<global::Microsoft.Extensions.AI.AITool> CreateTools(IDocumentStore store)
        => DocumentAIFunctionFactory.Build(store, this);
}
