using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// A document store that records append-only system-time temporal history for types mapped via the
/// provider's <c>MapTemporal&lt;T&gt;</c>. Every Insert/Update/Upsert/Remove writes a versioned snapshot
/// to a per-type history sidecar, so a document's state can be read back as of any point in time.
/// <para>
/// These methods live on this interface rather than <see cref="IDocumentStore"/> because history is a
/// provider-specific surface (matching the <c>Backup</c> / <c>ClearAllAsync</c> precedent). Every store
/// that supports temporal history implements this interface; the relational <see cref="DocumentStore"/>
/// and the LiteDB, MongoDB, CosmosDB, and IndexedDB stores all do.
/// </para>
/// </summary>
public interface ITemporalDocumentStore : IDocumentStore, IDisposable
{
    /// <summary>
    /// Returns every recorded version of a document, oldest first. Requires the type to be mapped via
    /// <c>MapTemporal&lt;T&gt;</c>.
    /// </summary>
    Task<IReadOnlyList<DocumentVersion<T>>> History<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Returns the document's state as of <paramref name="asOf"/>, or null if it did not exist (or had
    /// been removed) at that instant. Requires the type to be mapped via <c>MapTemporal&lt;T&gt;</c>.
    /// </summary>
    Task<T?> AsOf<T>(object id, DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Returns the live state of every document of the type as of <paramref name="asOf"/> — a fleet-wide
    /// point-in-time snapshot. Documents that did not yet exist, or had been removed, at that instant are
    /// excluded. Requires the type to be mapped via <c>MapTemporal&lt;T&gt;</c>.
    /// </summary>
    Task<IReadOnlyList<T>> AsOfAll<T>(DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Returns every version authored by <paramref name="actor"/> across all documents of the type,
    /// oldest first — a per-user audit trail. Requires <c>MapTemporal&lt;T&gt;</c> and a configured
    /// <see cref="TemporalOptions.CaptureActor"/>.
    /// </summary>
    Task<IReadOnlyList<DocumentVersion<T>>> ChangesByActor<T>(string actor, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Returns every version whose <see cref="DocumentVersion{T}.ValidFrom"/> falls in <c>[from, to)</c>
    /// across all documents of the type, oldest first — an audit log over a time window. Requires the
    /// type to be mapped via <c>MapTemporal&lt;T&gt;</c>.
    /// </summary>
    Task<IReadOnlyList<DocumentVersion<T>>> ChangesBetween<T>(DateTimeOffset from, DateTimeOffset to, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Restores a prior version as the current document state. This writes a new current version (it does
    /// not rewrite history): the document is re-inserted if it had been removed, otherwise overwritten.
    /// Returns the restored document, or null if the version does not exist (or was a removal tombstone).
    /// Requires the type to be mapped via <c>MapTemporal&lt;T&gt;</c>.
    /// </summary>
    Task<T?> Restore<T>(object id, long version, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Returns an RFC 6902 <see cref="JsonPatchDocument{T}"/> describing the change from
    /// <paramref name="fromVersion"/> to <paramref name="toVersion"/> of a document — the temporal
    /// analogue of <see cref="IDocumentStore.GetDiff{T}"/>. Returns null if either version is missing or
    /// is a removal tombstone (no body to diff). Requires the type to be mapped via <c>MapTemporal&lt;T&gt;</c>.
    /// </summary>
    Task<JsonPatchDocument<T>?> GetDiffBetween<T>(object id, long fromVersion, long toVersion, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;
}
