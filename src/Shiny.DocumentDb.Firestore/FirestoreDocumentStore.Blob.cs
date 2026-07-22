using Google.Cloud.Firestore;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.Firestore;

// Blob payloads live in a "{collection}_blobs" sibling collection (doc id "{docId}__{key}", a Firestore Blob
// field). Only metadata rides along in the document body. Writes are blobs-first; cascade delete keeps them
// consistent; SweepOrphanedBlobs reclaims TTL/out-of-band orphans.
public partial class FirestoreDocumentStore : IBlobDocumentStore
{
    /// <summary>A Firestore document caps at ~1 MiB; leave headroom for metadata.</summary>
    public long MaxBlobSize => 1_000_000;

    IReadOnlyList<BlobMapping> BlobMappings<T>() => this.options.ResolveBlobMappings(typeof(T));

    CollectionReference BlobCollection<T>() => this.db.Collection(this.ResolveCollectionName<T>() + "_blobs");
    CollectionReference BlobCollection(string collectionName) => this.db.Collection(collectionName + "_blobs");

    static string BlobDocId(string docId, string key) => $"{docId}__{key}";

    IReadOnlyList<PreparedBlobRow> PrepareBlobs<T>(T document) where T : class
        => BlobSupport.Prepare(this.BlobMappings<T>(), this.MaxBlobSize, document);

    async Task SyncBlobsAsync<T>(string id, string typeName, IReadOnlyList<PreparedBlobRow> blobs, bool prune, CancellationToken ct) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;

        var col = this.BlobCollection<T>();
        foreach (var b in blobs)
        {
            if (!b.IsPendingWrite)
                continue;
            var map = new Dictionary<string, object?>
            {
                ["docId"] = id,
                ["typeName"] = typeName,
                ["blobKey"] = b.Key,
                ["data"] = Blob.CopyFrom(b.RawBytes!),
                ["length"] = b.Blob.Length,
                ["contentType"] = b.Blob.ContentType,
                ["fileName"] = b.Blob.FileName,
                ["hash"] = b.Blob.Hash,
                ["createdAt"] = b.Blob.CreatedAt.ToString("o"),
                ["updatedAt"] = b.Blob.UpdatedAt.ToString("o")
            };
            await col.Document(BlobDocId(id, b.Key)).SetAsync(map, SetOptions.Overwrite, ct).ConfigureAwait(false);
            b.Blob.IsPendingWrite = false;
        }

        if (!prune)
            return;

        var keep = new HashSet<string>(blobs.Select(x => x.Key), StringComparer.Ordinal);
        foreach (var key in await this.QueryBlobKeysAsync(col, id, ct).ConfigureAwait(false))
            if (!keep.Contains(key))
                await col.Document(BlobDocId(id, key)).DeleteAsync(Precondition.None, ct).ConfigureAwait(false);
    }

    async Task<List<string>> QueryBlobKeysAsync(CollectionReference col, string docId, CancellationToken ct)
    {
        var keys = new List<string>();
        var snap = await col.WhereEqualTo("docId", docId).GetSnapshotAsync(ct).ConfigureAwait(false);
        foreach (var doc in snap.Documents)
            keys.Add(doc.GetValue<string>("blobKey"));
        return keys;
    }

    async Task DeleteBlobsAsync<T>(string id, CancellationToken ct) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;
        var col = this.BlobCollection<T>();
        foreach (var key in await this.QueryBlobKeysAsync(col, id, ct).ConfigureAwait(false))
            await col.Document(BlobDocId(id, key)).DeleteAsync(Precondition.None, ct).ConfigureAwait(false);
    }

    internal void AttachBlobLoaders<T>(T document) where T : class
    {
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0 || document == null)
            return;
        var id = this.idCache.GetOrCreate<T>(null).GetIdAsString(document);
        BlobSupport.AttachLoaders(mappings, document, new FirestoreBlobLoader(this, this.ResolveCollectionName<T>(), id));
    }

    async Task<byte[]?> ReadOneBlobAsync(string collectionName, string id, string key, CancellationToken ct)
    {
        var snap = await this.BlobCollection(collectionName).Document(BlobDocId(id, key)).GetSnapshotAsync(ct).ConfigureAwait(false);
        return snap.Exists && snap.TryGetValue<Blob>("data", out var blob) ? blob.ByteString.ToByteArray() : null;
    }

    async Task ReadManyBlobsAsync(string collectionName, string id, IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
    {
        var wanted = new Dictionary<string, List<DocumentBlob>>(StringComparer.Ordinal);
        foreach (var b in blobs)
        {
            if (b.IsLoaded || string.IsNullOrEmpty(b.Key))
                continue;
            if (!wanted.TryGetValue(b.Key, out var list))
                wanted[b.Key] = list = new List<DocumentBlob>();
            list.Add(b);
        }
        if (wanted.Count == 0)
            return;

        var snap = await this.BlobCollection(collectionName).WhereEqualTo("docId", id).GetSnapshotAsync(ct).ConfigureAwait(false);
        foreach (var doc in snap.Documents)
        {
            if (!wanted.TryGetValue(doc.GetValue<string>("blobKey"), out var targets) || !doc.TryGetValue<Blob>("data", out var blob))
                continue;
            var data = blob.ByteString.ToByteArray();
            foreach (var b in targets)
                b.Attach(data);
        }
    }

    /// <inheritdoc />
    public Task<byte[]?> GetBlob<T>(object id, string key, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        return this.ReadOneBlobAsync(this.ResolveCollectionName<T>(), resolvedId, key, cancellationToken);
    }

    /// <inheritdoc />
    public async Task BatchLoadBlobs<T>(IEnumerable<T> documents, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(documents);
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0)
            throw new InvalidOperationException($"'{typeof(T).Name}' has no mapped blob properties.");

        var accessor = this.idCache.GetOrCreate<T>(null);
        var collectionName = this.ResolveCollectionName<T>();
        foreach (var document in documents)
        {
            if (document == null)
                continue;
            var pending = BlobSupport.Enumerate(mappings, document).Where(b => !b.IsLoaded).ToList();
            if (pending.Count > 0)
                await this.ReadManyBlobsAsync(collectionName, accessor.GetIdAsString(document), pending, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<int> SweepOrphanedBlobs<T>(CancellationToken cancellationToken = default) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return 0;

        var mainCol = this.GetCollection<T>();
        var blobCol = this.BlobCollection<T>();

        var docIds = new HashSet<string>(StringComparer.Ordinal);
        var all = await blobCol.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        foreach (var doc in all.Documents)
            docIds.Add(doc.GetValue<string>("docId"));

        var removed = 0;
        foreach (var docId in docIds)
        {
            var snap = await mainCol.Document(docId).GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
            if (!snap.Exists)
            {
                foreach (var key in await this.QueryBlobKeysAsync(blobCol, docId, cancellationToken).ConfigureAwait(false))
                    await blobCol.Document(BlobDocId(docId, key)).DeleteAsync(Precondition.None, cancellationToken).ConfigureAwait(false);
                removed++;
            }
        }
        return removed;
    }

    sealed class FirestoreBlobLoader(FirestoreDocumentStore store, string collectionName, string id) : IBlobLoader
    {
        public Task<byte[]?> LoadOneAsync(string blobKey, CancellationToken ct)
            => store.ReadOneBlobAsync(collectionName, id, blobKey, ct);

        public Task LoadManyAsync(IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
            => store.ReadManyBlobsAsync(collectionName, id, blobs, ct);
    }
}
