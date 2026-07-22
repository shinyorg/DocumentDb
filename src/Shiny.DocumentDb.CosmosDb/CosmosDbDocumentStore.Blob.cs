using System.Net;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

// Blob payloads live in a "{container}_blobs" sidecar container, partitioned by /typeName; the payload is
// base64 (a Cosmos item is JSON, 2 MB max). Only metadata rides along in the document body. Writes are
// blobs-first; cascade delete keeps them consistent; SweepOrphanedBlobs reclaims TTL/out-of-band orphans.
public partial class CosmosDbDocumentStore : IBlobDocumentStore
{
    /// <summary>A Cosmos item caps at 2 MB; base64 inflates ~1.33×, so ~1.4 MB of raw payload fits with metadata.</summary>
    public long MaxBlobSize => 1_400_000;

    IReadOnlyList<BlobMapping> BlobMappings<T>() => this.options.ResolveBlobMappings(typeof(T));

    async Task<Container> EnsureBlobContainerAsync<T>(CancellationToken ct)
    {
        var name = this.ResolveContainerName<T>() + "_blobs";
        if (this.initializedContainers.ContainsKey(name))
            return this.database!.GetContainer(name);

        await this.initSemaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (this.initializedContainers.ContainsKey(name))
                return this.database!.GetContainer(name);
            if (this.database == null)
            {
                var dbResponse = await this.client.CreateDatabaseIfNotExistsAsync(this.options.DatabaseName, cancellationToken: ct).ConfigureAwait(false);
                this.database = dbResponse.Database;
            }
            await this.database.CreateContainerIfNotExistsAsync(
                new ContainerProperties(name, "/typeName"), this.options.DefaultThroughput, cancellationToken: ct).ConfigureAwait(false);
            this.initializedContainers.TryAdd(name, 0);
            return this.database.GetContainer(name);
        }
        finally
        {
            this.initSemaphore.Release();
        }
    }

    static string BlobItemId(string docId, string key) => $"{docId}__{key}";

    IReadOnlyList<PreparedBlobRow> PrepareBlobs<T>(T document) where T : class
        => BlobSupport.Prepare(this.BlobMappings<T>(), this.MaxBlobSize, document);

    async Task SyncBlobsAsync<T>(string id, string typeName, IReadOnlyList<PreparedBlobRow> blobs, bool prune, CancellationToken ct) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;

        var container = await this.EnsureBlobContainerAsync<T>(ct).ConfigureAwait(false);
        var pk = new PartitionKey(typeName);
        foreach (var b in blobs)
        {
            if (!b.IsPendingWrite)
                continue;
            var item = new CosmosBlobDocument
            {
                Id = BlobItemId(id, b.Key),
                TypeName = typeName,
                DocId = id,
                BlobKey = b.Key,
                Data = Convert.ToBase64String(b.RawBytes!),
                Length = b.Blob.Length,
                ContentType = b.Blob.ContentType,
                FileName = b.Blob.FileName,
                Hash = b.Blob.Hash,
                CreatedAt = b.Blob.CreatedAt.ToString("o"),
                UpdatedAt = b.Blob.UpdatedAt.ToString("o")
            };
            await container.UpsertItemAsync(item, pk, cancellationToken: ct).ConfigureAwait(false);
            b.Blob.IsPendingWrite = false;
        }

        if (!prune)
            return;

        var keep = new HashSet<string>(blobs.Select(x => x.Key), StringComparer.Ordinal);
        foreach (var key in await this.QueryBlobKeysAsync(container, typeName, id, ct).ConfigureAwait(false))
            if (!keep.Contains(key))
                await container.DeleteItemAsync<CosmosBlobDocument>(BlobItemId(id, key), pk, cancellationToken: ct).ConfigureAwait(false);
    }

    async Task<List<string>> QueryBlobKeysAsync(Container container, string typeName, string docId, CancellationToken ct)
    {
        var keys = new List<string>();
        var query = new QueryDefinition("SELECT c.blobKey FROM c WHERE c.docId = @docId").WithParameter("@docId", docId);
        using var iterator = container.GetItemQueryIterator<CosmosBlobDocument>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(typeName) });
        while (iterator.HasMoreResults)
            foreach (var row in await iterator.ReadNextAsync(ct).ConfigureAwait(false))
                keys.Add(row.BlobKey);
        return keys;
    }

    async Task DeleteBlobsAsync<T>(string id, string typeName, CancellationToken ct) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;
        var container = await this.EnsureBlobContainerAsync<T>(ct).ConfigureAwait(false);
        var pk = new PartitionKey(typeName);
        foreach (var key in await this.QueryBlobKeysAsync(container, typeName, id, ct).ConfigureAwait(false))
            await container.DeleteItemAsync<CosmosBlobDocument>(BlobItemId(id, key), pk, cancellationToken: ct).ConfigureAwait(false);
    }

    internal void AttachBlobLoaders<T>(T document) where T : class
    {
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0 || document == null)
            return;
        var id = this.idCache.GetOrCreate<T>(null).GetIdAsString(document);
        BlobSupport.AttachLoaders(mappings, document, new CosmosBlobLoader(this, this.ResolveContainerName<T>() + "_blobs", this.ResolveTypeName<T>(), id));
    }

    T? Materialize<T>(string json, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var doc = Deserialize(json, typeInfo, this.jsonOptions);
        if (doc != null)
            this.AttachBlobLoaders(doc);
        return doc;
    }

    async Task<byte[]?> ReadOneBlobAsync(string containerName, string typeName, string id, string key, CancellationToken ct)
    {
        var container = this.database!.GetContainer(containerName);
        try
        {
            var resp = await container.ReadItemAsync<CosmosBlobDocument>(BlobItemId(id, key), new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);
            return Convert.FromBase64String(resp.Resource.Data);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    async Task ReadManyBlobsAsync(string containerName, string typeName, string id, IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
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

        var container = this.database!.GetContainer(containerName);
        var query = new QueryDefinition("SELECT * FROM c WHERE c.docId = @docId").WithParameter("@docId", id);
        using var iterator = container.GetItemQueryIterator<CosmosBlobDocument>(query,
            requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(typeName) });
        while (iterator.HasMoreResults)
        {
            foreach (var row in await iterator.ReadNextAsync(ct).ConfigureAwait(false))
            {
                if (!wanted.TryGetValue(row.BlobKey, out var targets))
                    continue;
                var data = Convert.FromBase64String(row.Data);
                foreach (var b in targets)
                    b.Attach(data);
            }
        }
    }

    /// <inheritdoc />
    public async Task<byte[]?> GetBlob<T>(object id, string key, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        await this.EnsureBlobContainerAsync<T>(cancellationToken).ConfigureAwait(false);
        return await this.ReadOneBlobAsync(this.ResolveContainerName<T>() + "_blobs", this.ResolveTypeName<T>(), resolvedId, key, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task BatchLoadBlobs<T>(IEnumerable<T> documents, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(documents);
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0)
            throw new InvalidOperationException($"'{typeof(T).Name}' has no mapped blob properties.");

        await this.EnsureBlobContainerAsync<T>(cancellationToken).ConfigureAwait(false);
        var accessor = this.idCache.GetOrCreate<T>(null);
        var containerName = this.ResolveContainerName<T>() + "_blobs";
        var typeName = this.ResolveTypeName<T>();
        foreach (var document in documents)
        {
            if (document == null)
                continue;
            var pending = BlobSupport.Enumerate(mappings, document).Where(b => !b.IsLoaded).ToList();
            if (pending.Count > 0)
                await this.ReadManyBlobsAsync(containerName, typeName, accessor.GetIdAsString(document), pending, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<int> SweepOrphanedBlobs<T>(CancellationToken cancellationToken = default) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return 0;

        var typeName = this.ResolveTypeName<T>();
        var blobContainer = await this.EnsureBlobContainerAsync<T>(cancellationToken).ConfigureAwait(false);
        var mainContainer = await this.GetContainerAsync<T>(cancellationToken).ConfigureAwait(false);
        var pk = new PartitionKey(typeName);

        var docIds = new HashSet<string>(StringComparer.Ordinal);
        using (var iterator = blobContainer.GetItemQueryIterator<CosmosBlobDocument>(
            new QueryDefinition("SELECT DISTINCT c.docId FROM c"), requestOptions: new QueryRequestOptions { PartitionKey = pk }))
        {
            while (iterator.HasMoreResults)
                foreach (var row in await iterator.ReadNextAsync(cancellationToken).ConfigureAwait(false))
                    docIds.Add(row.DocId);
        }

        var removed = 0;
        foreach (var docId in docIds)
        {
            var exists = true;
            try
            {
                await mainContainer.ReadItemAsync<CosmosDocument>(docId, pk, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                exists = false;
            }
            if (!exists)
            {
                foreach (var key in await this.QueryBlobKeysAsync(blobContainer, typeName, docId, cancellationToken).ConfigureAwait(false))
                    await blobContainer.DeleteItemAsync<CosmosBlobDocument>(BlobItemId(docId, key), pk, cancellationToken: cancellationToken).ConfigureAwait(false);
                removed++;
            }
        }
        return removed;
    }

    sealed class CosmosBlobLoader(CosmosDbDocumentStore store, string containerName, string typeName, string id) : IBlobLoader
    {
        public Task<byte[]?> LoadOneAsync(string blobKey, CancellationToken ct)
            => store.ReadOneBlobAsync(containerName, typeName, id, blobKey, ct);

        public Task LoadManyAsync(IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
            => store.ReadManyBlobsAsync(containerName, typeName, id, blobs, ct);
    }
}
