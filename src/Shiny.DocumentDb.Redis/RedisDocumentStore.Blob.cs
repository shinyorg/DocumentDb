using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;
using StackExchange.Redis;

namespace Shiny.DocumentDb.Redis;

// Blob payloads are stored one per key ("{prefix}blob:{typeName}:{id}:{key}", raw binary) with a per-document
// set ("{prefix}blobset:{typeName}:{id}") of blob keys for cascade/prune/load enumeration. Only metadata
// rides along in the document JSON. Writes are blobs-first; cascade delete keeps them consistent; a TTL that
// expires a document leaves orphans that SweepOrphanedBlobs reclaims.
public partial class RedisDocumentStore : IBlobDocumentStore
{
    /// <summary>A Redis string value can hold up to 512 MB.</summary>
    public long MaxBlobSize => 512L * 1024 * 1024;

    IReadOnlyList<BlobMapping> BlobMappings<T>() => this.options.ResolveBlobMappings(typeof(T));

    string BlobKey(string typeName, string id, string key) => $"{this.keyPrefix}blob:{typeName}:{id}:{key}";
    string BlobSetKey(string typeName, string id) => $"{this.keyPrefix}blobset:{typeName}:{id}";
    string BlobSetScanPattern(string typeName) => $"{this.keyPrefix}blobset:{typeName}:*";

    IReadOnlyList<PreparedBlobRow> PrepareBlobs<T>(T document) where T : class
        => BlobSupport.Prepare(this.BlobMappings<T>(), this.MaxBlobSize, document);

    async Task SyncBlobsAsync<T>(string id, string typeName, IReadOnlyList<PreparedBlobRow> blobs, bool prune, CancellationToken ct) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;

        var setKey = this.BlobSetKey(typeName, id);
        foreach (var b in blobs)
        {
            if (!b.IsPendingWrite)
                continue;
            await this.db.StringSetAsync(this.BlobKey(typeName, id, b.Key), b.RawBytes!).ConfigureAwait(false);
            await this.db.SetAddAsync(setKey, b.Key).ConfigureAwait(false);
            b.Blob.IsPendingWrite = false;
        }

        if (!prune)
            return;

        var keep = new HashSet<string>(blobs.Select(x => x.Key), StringComparer.Ordinal);
        foreach (var member in await this.db.SetMembersAsync(setKey).ConfigureAwait(false))
        {
            var k = (string)member!;
            if (keep.Contains(k))
                continue;
            await this.db.KeyDeleteAsync(this.BlobKey(typeName, id, k)).ConfigureAwait(false);
            await this.db.SetRemoveAsync(setKey, k).ConfigureAwait(false);
        }
    }

    async Task DeleteBlobsAsync<T>(string id, string typeName) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;
        await this.DeleteBlobsForDocAsync(typeName, id).ConfigureAwait(false);
    }

    async Task DeleteBlobsForDocAsync(string typeName, string id)
    {
        var setKey = this.BlobSetKey(typeName, id);
        foreach (var member in await this.db.SetMembersAsync(setKey).ConfigureAwait(false))
            await this.db.KeyDeleteAsync(this.BlobKey(typeName, id, (string)member!)).ConfigureAwait(false);
        await this.db.KeyDeleteAsync(setKey).ConfigureAwait(false);
    }

    internal void AttachBlobLoaders<T>(T document) where T : class
    {
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0 || document == null)
            return;
        var id = this.idCache.GetOrCreate<T>(null).GetIdAsString(document);
        BlobSupport.AttachLoaders(mappings, document, new RedisBlobLoader(this, this.ResolveTypeName<T>(), id));
    }

    // Deserialize + stamp blob loaders — the materialization seam for Redis's read paths.
    T? Materialize<T>(string json, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var doc = Deserialize(json, typeInfo, this.jsonOptions);
        if (doc != null)
            this.AttachBlobLoaders(doc);
        return doc;
    }

    async Task<byte[]?> ReadOneBlobAsync(string typeName, string id, string key)
    {
        var value = await this.db.StringGetAsync(this.BlobKey(typeName, id, key)).ConfigureAwait(false);
        return value.IsNull ? null : (byte[]?)value;
    }

    async Task ReadManyBlobsAsync(string typeName, string id, IReadOnlyList<DocumentBlob> blobs)
    {
        foreach (var blob in blobs)
        {
            if (blob.IsLoaded || string.IsNullOrEmpty(blob.Key))
                continue;
            var bytes = await this.ReadOneBlobAsync(typeName, id, blob.Key).ConfigureAwait(false);
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
        return this.ReadOneBlobAsync(this.ResolveTypeName<T>(), resolvedId, key);
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
                await this.ReadManyBlobsAsync(typeName, accessor.GetIdAsString(document), pending).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<int> SweepOrphanedBlobs<T>(CancellationToken cancellationToken = default) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return 0;

        var typeName = this.ResolveTypeName<T>();
        var prefix = $"{this.keyPrefix}blobset:{typeName}:";
        var server = this.GetServer();
        var removed = 0;
        await foreach (var setKey in server.KeysAsync(database: this.db.Database, pattern: this.BlobSetScanPattern(typeName)).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            var id = ((string)setKey!)[prefix.Length..];
            if (!await this.db.KeyExistsAsync(this.DocKey(typeName, id)).ConfigureAwait(false))
            {
                await this.DeleteBlobsForDocAsync(typeName, id).ConfigureAwait(false);
                removed++;
            }
        }
        return removed;
    }

    sealed class RedisBlobLoader(RedisDocumentStore store, string typeName, string id) : IBlobLoader
    {
        public Task<byte[]?> LoadOneAsync(string blobKey, CancellationToken ct)
            => store.ReadOneBlobAsync(typeName, id, blobKey);

        public Task LoadManyAsync(IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
            => store.ReadManyBlobsAsync(typeName, id, blobs);
    }
}
