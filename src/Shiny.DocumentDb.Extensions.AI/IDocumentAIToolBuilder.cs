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

    /// <summary>
    /// Registers a <b>schema-free</b> JSON collection (<see cref="IDocumentStore.Collection(string, string)"/>)
    /// for AI tool exposure. Use this when there is no CLR document type — a form builder, a multi-tenant
    /// custom object, an intake or ETL landing collection. When you do have a document type, prefer
    /// <see cref="AddType{T}"/>: it discovers fields and enforces filters that this lane cannot.
    /// </summary>
    /// <param name="collectionName">The collection name, as passed to <c>store.Collection(name)</c>.</param>
    /// <param name="name">Tool slug used in generated tool names. Must be unique. Defaults to the collection name.</param>
    /// <param name="capabilities">Set of operations the LLM may perform on this collection.</param>
    /// <param name="configure">
    /// Configuration callback. Declare the LLM-visible fields here — unlike a type, a collection carries no
    /// metadata to discover them from, so a registration that declares none and does not call
    /// <see cref="IDocumentAICollectionBuilder.AllowAnyField"/> is rejected.
    /// </param>
    /// <remarks>
    /// JSON collections are provider-scoped: the tools throw <see cref="NotSupportedException"/> at call time
    /// on a store whose provider does not implement the lane (the document-native and key-partitioned
    /// backends).
    /// </remarks>
    IDocumentAIToolBuilder AddCollection(
        string collectionName,
        string? name = null,
        DocumentAICapabilities capabilities = DocumentAICapabilities.ReadOnly,
        Action<IDocumentAICollectionBuilder>? configure = null
    );
}
