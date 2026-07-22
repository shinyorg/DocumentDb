using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.RavenDb;

// Blobs use RavenDB's native document attachments — the bytes attach to the wrapper document, keyed by the
// blob key; only metadata rides along in the document body. Attachments store and commit in the same session
// as the document (atomic) and RavenDB deletes them automatically when the document is deleted, so cascade
// needs no code and orphans cannot occur (SweepOrphanedBlobs is a no-op via the default implementation).
public partial class RavenDbDocumentStore : IBlobDocumentStore
{
    /// <summary>RavenDB attachments have no small per-item cap; 2 GB is a practical single-attachment ceiling.</summary>
    public long MaxBlobSize => int.MaxValue;

    IReadOnlyList<BlobMapping> BlobMappings<T>() => this.options.ResolveBlobMappings(typeof(T));

    IReadOnlyList<PreparedBlobRow> PrepareBlobs<T>(T document) where T : class
        => BlobSupport.Prepare(this.BlobMappings<T>(), this.MaxBlobSize, document);

    // Queues attachment stores/deletes on the caller's session so they commit atomically with the document.
    void AttachInSession<T>(IAsyncDocumentSession session, string ravenId, object? wrapper, IReadOnlyList<PreparedBlobRow> blobs, bool prune) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;

        var existing = wrapper != null
            ? session.Advanced.Attachments.GetNames(wrapper)
            : Array.Empty<AttachmentName>();
        var existingNames = new HashSet<string>(existing.Select(n => n.Name), StringComparer.Ordinal);
        var keep = new HashSet<string>(StringComparer.Ordinal);

        foreach (var b in blobs)
        {
            keep.Add(b.Key);
            if (!b.IsPendingWrite)
                continue;
            // Store overwrites an existing server-side attachment (PUT). Do NOT delete-then-store in the same
            // session — RavenDB rejects a Store that conflicts with a deferred Delete of the same name.
            session.Advanced.Attachments.Store(ravenId, b.Key, new MemoryStream(b.RawBytes!), b.Blob.ContentType);
            b.Blob.IsPendingWrite = false;
        }

        if (!prune)
            return;
        foreach (var name in existingNames)
            if (!keep.Contains(name))
                session.Advanced.Attachments.Delete(ravenId, name);
    }

    // Deserialize + stamp blob loaders — the materialization seam for RavenDB's read paths.
    T? Materialize<T>(string json, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? typeInfo) where T : class
    {
        var doc = Deserialize(json, typeInfo, this.jsonOptions);
        if (doc != null)
            this.AttachBlobLoaders(doc);
        return doc;
    }

    internal void AttachBlobLoaders<T>(T document) where T : class
    {
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0 || document == null)
            return;
        var id = this.idCache.GetOrCreate<T>(null).GetIdAsString(document);
        BlobSupport.AttachLoaders(mappings, document, new RavenBlobLoader(this, RavenDbDocument.RavenId(this.ResolveTypeName<T>(), id)));
    }

    async Task<byte[]?> ReadOneBlobAsync(string ravenId, string key, CancellationToken ct)
    {
        using var session = this.NewRavenSession();
        try
        {
            using var result = await session.Advanced.Attachments.GetAsync(ravenId, key, ct).ConfigureAwait(false);
            if (result == null)
                return null;
            using var ms = new MemoryStream();
            await result.Stream.CopyToAsync(ms, ct).ConfigureAwait(false);
            return ms.ToArray();
        }
        catch (Raven.Client.Exceptions.Documents.DocumentDoesNotExistException)
        {
            return null;
        }
    }

    async Task ReadManyBlobsAsync(string ravenId, IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
    {
        foreach (var blob in blobs)
        {
            if (blob.IsLoaded || string.IsNullOrEmpty(blob.Key))
                continue;
            var bytes = await this.ReadOneBlobAsync(ravenId, blob.Key, ct).ConfigureAwait(false);
            if (bytes != null)
                blob.Attach(bytes);
        }
    }

    /// <inheritdoc />
    public Task<byte[]?> GetBlob<T>(object id, string key, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        return this.ReadOneBlobAsync(RavenDbDocument.RavenId(this.ResolveTypeName<T>(), resolvedId), key, cancellationToken);
    }

    /// <inheritdoc />
    public async Task BatchLoadBlobs<T>(IEnumerable<T> documents, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(documents);
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0)
            throw new InvalidOperationException($"'{typeof(T).Name}' has no mapped blob properties.");

        var accessor = this.idCache.GetOrCreate<T>(null);
        var typeName = this.ResolveTypeName<T>();
        foreach (var document in documents)
        {
            if (document == null)
                continue;
            var pending = BlobSupport.Enumerate(mappings, document).Where(b => !b.IsLoaded).ToList();
            if (pending.Count > 0)
                await this.ReadManyBlobsAsync(RavenDbDocument.RavenId(typeName, accessor.GetIdAsString(document)), pending, cancellationToken).ConfigureAwait(false);
        }
    }

    sealed class RavenBlobLoader(RavenDbDocumentStore store, string ravenId) : IBlobLoader
    {
        public Task<byte[]?> LoadOneAsync(string blobKey, CancellationToken ct)
            => store.ReadOneBlobAsync(ravenId, blobKey, ct);

        public Task LoadManyAsync(IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
            => store.ReadManyBlobsAsync(ravenId, blobs, ct);
    }
}
