namespace Shiny.DocumentDb;

/// <summary>
/// Optional store capability for development, testing, and reset scenarios. Generalises the SQLite-only
/// <c>ClearAllAsync</c> precedent to every backend. Implemented by the relational <see cref="DocumentStore"/>
/// and the MongoDB and CosmosDB stores.
/// <para>
/// Like <see cref="ITemporalDocumentStore"/>, this is a separate capability surface rather than part of
/// <see cref="IDocumentStore"/>; probe for it with <c>store is IDocumentMaintenance</c>.
/// </para>
/// </summary>
public interface IDocumentMaintenance
{
    /// <summary>
    /// Removes every document of every type managed by this store, including temporal-history, spatial,
    /// and vector sidecars. This is a whole-store wipe intended for tests and dev resets — it is NOT
    /// tenant-scoped or type-scoped (use <see cref="IDocumentStore.Clear{T}"/> for a single type). On a
    /// shared-table multi-tenant store this clears ALL tenants.
    /// </summary>
    Task ClearAll(CancellationToken cancellationToken = default);
}
