using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Extensions.AI;

/// <summary>
/// Builder used to register the document types the AI agent is allowed to access.
/// A type that is not registered here is invisible to the LLM.
/// </summary>
public interface IDocumentAIToolBuilder
{
    /// <summary>
    /// Registers a document type for AI tool exposure.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="jsonTypeInfo">STJ type metadata. Required for AOT-safe serialization.</param>
    /// <param name="name">Tool/type slug used in generated tool names. Must be unique. Defaults to lower-cased type name.</param>
    /// <param name="capabilities">Set of operations the LLM may perform on this type.</param>
    /// <param name="configure">Optional per-type configuration callback (descriptions, ignored properties, page caps).</param>
    IDocumentAIToolBuilder AddType<T>(
        JsonTypeInfo<T> jsonTypeInfo,
        string? name = null,
        DocumentAICapabilities capabilities = DocumentAICapabilities.ReadOnly,
        Action<IDocumentAITypeBuilder<T>>? configure = null
    ) where T : class;
}
