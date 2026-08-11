using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.Hosting;

/// <summary>
/// Reads the mapping state a store was built with, for hosts that have to honour a mapping the store applies
/// implicitly.
/// </summary>
/// <remarks>
/// The concrete case is optimistic concurrency: a transport exposes it as <c>ETag</c>/<c>If-Match</c>, and to do
/// that it has to know whether the type has a mapped version property and how to read it. That lives in the
/// store's options, which are otherwise a provider-specific type a host cannot name. These accessors return the
/// provider-agnostic slice — the mapping registry — so one host works against every backend.
/// </remarks>
public static class DocumentStoreAccessor
{
    /// <summary>
    /// The per-type mapping registry <paramref name="store"/> was configured with, or <c>null</c> when the
    /// instance does not expose one (a custom <see cref="IDocumentStore"/> implementation from outside this
    /// library). Every provider in this library returns its registry.
    /// </summary>
    /// <remarks>
    /// A <c>null</c> result means "unknown", not "nothing mapped" — a host should degrade rather than assume the
    /// type has no mappings.
    /// </remarks>
    public static DocumentMappingRegistry? GetMappings(this IDocumentStore store)
    {
        ArgumentNullException.ThrowIfNull(store);

        return store switch
        {
            IQueryExecutor executor => executor.Options.Mappings,
            DocumentProviderBase provider => provider.MappingRegistry,
            _ => null
        };
    }

    /// <summary>
    /// The version (optimistic-concurrency) mapping for <paramref name="documentType"/>, or <c>null</c> when the
    /// type has none — which is a host's signal that this type has no ETag to publish.
    /// </summary>
    /// <param name="store">The store the documents come from.</param>
    /// <param name="documentType">The document type.</param>
    public static VersionMapping? GetVersionMapping(this IDocumentStore store, Type documentType)
    {
        ArgumentNullException.ThrowIfNull(documentType);

        return store.GetMappings()?.ResolveVersionMapping(documentType);
    }
}
