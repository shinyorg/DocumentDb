using System.Text.Json.Serialization.Metadata;
using Azure;
using Azure.Data.Tables;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.AzureTable;

// Blob payloads live in a sibling "{table}blobs" table: PartitionKey = "{typeName}:{id}", RowKey = blobKey,
// binary Data property. Only metadata rides along in the document body. Writes are blobs-first; cascade
// delete keeps them consistent; SweepOrphanedBlobs reclaims rows left by out-of-band deletes / TTL.
public partial class AzureTableDocumentStore : IBlobDocumentStore
{
    /// <summary>Azure Table caps a single binary property at 64 KiB.</summary>
    public long MaxBlobSize => 64 * 1024;

    bool blobTableEnsured;

    IReadOnlyList<BlobMapping> BlobMappings<T>() => this.options.ResolveBlobMappings(typeof(T));

    static string BlobPartition(string typeName, string id) => $"{typeName}:{id}";

    async Task<TableClient> GetBlobTableAsync(CancellationToken ct)
    {
        if (!this.blobTableEnsured)
        {
            await this.blobTable.CreateIfNotExistsAsync(ct).ConfigureAwait(false);
            this.blobTableEnsured = true;
        }
        return this.blobTable;
    }

    IReadOnlyList<PreparedBlobRow> PrepareBlobs<T>(T document) where T : class
        => BlobSupport.Prepare(this.BlobMappings<T>(), this.MaxBlobSize, document);

    async Task SyncBlobsAsync<T>(string id, string typeName, IReadOnlyList<PreparedBlobRow> blobs, bool prune, CancellationToken ct) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;

        var partition = BlobPartition(typeName, id);
        var table = await this.GetBlobTableAsync(ct).ConfigureAwait(false);
        foreach (var b in blobs)
        {
            if (!b.IsPendingWrite)
                continue;
            var entity = new TableEntity(partition, b.Key)
            {
                ["Data"] = new BinaryData(b.RawBytes!).ToArray(),
                ["Length"] = b.Blob.Length,
                ["ContentType"] = b.Blob.ContentType,
                ["FileName"] = b.Blob.FileName,
                ["Hash"] = b.Blob.Hash,
                ["CreatedAt"] = b.Blob.CreatedAt.ToString("o"),
                ["UpdatedAt"] = b.Blob.UpdatedAt.ToString("o")
            };
            await table.UpsertEntityAsync(entity, TableUpdateMode.Replace, ct).ConfigureAwait(false);
            b.Blob.IsPendingWrite = false;
        }

        if (!prune)
            return;

        var keep = new HashSet<string>(blobs.Select(x => x.Key), StringComparer.Ordinal);
        await foreach (var e in table.QueryAsync<TableEntity>(x => x.PartitionKey == partition, cancellationToken: ct).ConfigureAwait(false))
            if (!keep.Contains(e.RowKey))
                await table.DeleteEntityAsync(partition, e.RowKey, ETag.All, ct).ConfigureAwait(false);
    }

    async Task DeleteBlobsAsync<T>(string id, string typeName, CancellationToken ct) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;
        await this.DeleteBlobPartitionAsync(typeName, id, ct).ConfigureAwait(false);
    }

    async Task DeleteBlobPartitionAsync(string typeName, string id, CancellationToken ct)
    {
        var partition = BlobPartition(typeName, id);
        var table = await this.GetBlobTableAsync(ct).ConfigureAwait(false);
        await foreach (var e in table.QueryAsync<TableEntity>(x => x.PartitionKey == partition, cancellationToken: ct).ConfigureAwait(false))
            await table.DeleteEntityAsync(partition, e.RowKey, ETag.All, ct).ConfigureAwait(false);
    }

    internal void AttachBlobLoaders<T>(T document) where T : class
    {
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0 || document == null)
            return;
        var id = this.idCache.GetOrCreate<T>(null).GetIdAsString(document);
        BlobSupport.AttachLoaders(mappings, document, new AzureTableBlobLoader(this, this.ResolveTypeName<T>(), id));
    }

    T? Materialize<T>(string json, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var doc = Deserialize(json, typeInfo, this.jsonOptions);
        if (doc != null)
            this.AttachBlobLoaders(doc);
        return doc;
    }

    async Task<byte[]?> ReadOneBlobAsync(string typeName, string id, string key, CancellationToken ct)
    {
        var table = await this.GetBlobTableAsync(ct).ConfigureAwait(false);
        try
        {
            var resp = await table.GetEntityAsync<TableEntity>(BlobPartition(typeName, id), key, cancellationToken: ct).ConfigureAwait(false);
            return resp.Value.GetBinary("Data");
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    async Task ReadManyBlobsAsync(string typeName, string id, IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
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

        var partition = BlobPartition(typeName, id);
        var table = await this.GetBlobTableAsync(ct).ConfigureAwait(false);
        await foreach (var e in table.QueryAsync<TableEntity>(x => x.PartitionKey == partition, cancellationToken: ct).ConfigureAwait(false))
        {
            if (!wanted.TryGetValue(e.RowKey, out var targets))
                continue;
            var data = e.GetBinary("Data");
            if (data == null)
                continue;
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
        return this.ReadOneBlobAsync(this.ResolveTypeName<T>(), resolvedId, key, cancellationToken);
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
                await this.ReadManyBlobsAsync(typeName, accessor.GetIdAsString(document), pending, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<int> SweepOrphanedBlobs<T>(CancellationToken cancellationToken = default) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return 0;

        var typeName = this.ResolveTypeName<T>();
        var prefix = typeName + ":";
        var blobs = await this.GetBlobTableAsync(cancellationToken).ConfigureAwait(false);

        // group blob rows by their document partition, then drop partitions whose document is gone
        var partitions = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var e in blobs.QueryAsync<TableEntity>(cancellationToken: cancellationToken).ConfigureAwait(false))
            if (e.PartitionKey.StartsWith(prefix, StringComparison.Ordinal))
                partitions.Add(e.PartitionKey);

        var removed = 0;
        foreach (var partition in partitions)
        {
            var docId = partition[prefix.Length..];
            var exists = false;
            try
            {
                await this.table.GetEntityAsync<TableEntity>(typeName, docId, cancellationToken: cancellationToken).ConfigureAwait(false);
                exists = true;
            }
            catch (RequestFailedException ex) when (ex.Status == 404) { }

            if (!exists)
            {
                await this.DeleteBlobPartitionAsync(typeName, docId, cancellationToken).ConfigureAwait(false);
                removed++;
            }
        }
        return removed;
    }

    sealed class AzureTableBlobLoader(AzureTableDocumentStore store, string typeName, string id) : IBlobLoader
    {
        public Task<byte[]?> LoadOneAsync(string blobKey, CancellationToken ct)
            => store.ReadOneBlobAsync(typeName, id, blobKey, ct);

        public Task LoadManyAsync(IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
            => store.ReadManyBlobsAsync(typeName, id, blobs, ct);
    }
}
