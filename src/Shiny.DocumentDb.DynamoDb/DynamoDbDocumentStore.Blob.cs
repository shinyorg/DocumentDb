using System.Text.Json.Serialization.Metadata;
using Amazon.DynamoDBv2.Model;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.DynamoDb;

// Blob payloads live in the SAME table under a distinct partition key ("blob#{typeName}#{id}", sort key =
// blobKey, binary Data attribute) so document reads never fetch them. Only metadata rides along in the body.
// Writes are blobs-first; cascade delete keeps them consistent; SweepOrphanedBlobs reclaims TTL/out-of-band
// orphans (a full-table scan, so it is anti-entropy insurance, not a hot path).
public partial class DynamoDbDocumentStore : IBlobDocumentStore
{
    /// <summary>DynamoDB caps an item at 400 KB; 390 KB leaves headroom for the blob item's metadata.</summary>
    public long MaxBlobSize => 390 * 1024;

    IReadOnlyList<BlobMapping> BlobMappings<T>() => this.options.ResolveBlobMappings(typeof(T));

    static string BlobPk(string typeName, string id) => $"blob#{typeName}#{id}";

    IReadOnlyList<PreparedBlobRow> PrepareBlobs<T>(T document) where T : class
        => BlobSupport.Prepare(this.BlobMappings<T>(), this.MaxBlobSize, document);

    async Task SyncBlobsAsync<T>(string id, string typeName, IReadOnlyList<PreparedBlobRow> blobs, bool prune, CancellationToken ct) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;

        var pk = BlobPk(typeName, id);
        foreach (var b in blobs)
        {
            if (!b.IsPendingWrite)
                continue;
            var item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() { S = pk },
                ["sk"] = new() { S = b.Key },
                ["Data"] = new() { B = new MemoryStream(b.RawBytes!) },
                ["Length"] = new() { N = b.Blob.Length.ToString() },
                ["ContentType"] = b.Blob.ContentType == null ? new AttributeValue { NULL = true } : new AttributeValue { S = b.Blob.ContentType },
                ["FileName"] = b.Blob.FileName == null ? new AttributeValue { NULL = true } : new AttributeValue { S = b.Blob.FileName },
                ["Hash"] = b.Blob.Hash == null ? new AttributeValue { NULL = true } : new AttributeValue { S = b.Blob.Hash },
                ["CreatedAt"] = new() { S = b.Blob.CreatedAt.ToString("o") },
                ["UpdatedAt"] = new() { S = b.Blob.UpdatedAt.ToString("o") }
            };
            await this.client.PutItemAsync(new PutItemRequest { TableName = this.TableName, Item = item }, ct).ConfigureAwait(false);
            b.Blob.IsPendingWrite = false;
        }

        if (!prune)
            return;

        var keep = new HashSet<string>(blobs.Select(x => x.Key), StringComparer.Ordinal);
        foreach (var existing in await this.QueryBlobKeysAsync(pk, ct).ConfigureAwait(false))
            if (!keep.Contains(existing))
                await this.DeleteBlobItemAsync(pk, existing, ct).ConfigureAwait(false);
    }

    async Task<List<string>> QueryBlobKeysAsync(string pk, CancellationToken ct)
    {
        var keys = new List<string>();
        var resp = await this.client.QueryAsync(new QueryRequest
        {
            TableName = this.TableName,
            KeyConditionExpression = "pk = :p",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":p"] = new() { S = pk } },
            ProjectionExpression = "sk"
        }, ct).ConfigureAwait(false);
        foreach (var item in resp.Items)
            keys.Add(item["sk"].S);
        return keys;
    }

    Task DeleteBlobItemAsync(string pk, string key, CancellationToken ct)
        => this.client.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = this.TableName,
            Key = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = pk }, ["sk"] = new() { S = key } }
        }, ct);

    async Task DeleteBlobsAsync<T>(string id, string typeName, CancellationToken ct) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;
        var pk = BlobPk(typeName, id);
        foreach (var key in await this.QueryBlobKeysAsync(pk, ct).ConfigureAwait(false))
            await this.DeleteBlobItemAsync(pk, key, ct).ConfigureAwait(false);
    }

    internal void AttachBlobLoaders<T>(T document) where T : class
    {
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0 || document == null)
            return;
        var id = this.idCache.GetOrCreate<T>(null).GetIdAsString(document);
        BlobSupport.AttachLoaders(mappings, document, new DynamoBlobLoader(this, this.ResolveTypeName<T>(), id));
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
        var resp = await this.client.GetItemAsync(new GetItemRequest
        {
            TableName = this.TableName,
            Key = new Dictionary<string, AttributeValue> { ["pk"] = new() { S = BlobPk(typeName, id) }, ["sk"] = new() { S = key } }
        }, ct).ConfigureAwait(false);
        if (!resp.IsItemSet || !resp.Item.TryGetValue("Data", out var data) || data.B == null)
            return null;
        return data.B.ToArray();
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

        var resp = await this.client.QueryAsync(new QueryRequest
        {
            TableName = this.TableName,
            KeyConditionExpression = "pk = :p",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":p"] = new() { S = BlobPk(typeName, id) } }
        }, ct).ConfigureAwait(false);
        foreach (var item in resp.Items)
        {
            if (!wanted.TryGetValue(item["sk"].S, out var targets) || !item.TryGetValue("Data", out var data) || data.B == null)
                continue;
            var bytes = data.B.ToArray();
            foreach (var b in targets)
                b.Attach(bytes);
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
        var partitionKey = this.options.ResolvePartitionKey(typeof(T), typeName);
        var prefix = $"blob#{typeName}#";

        // full-table scan for blob partitions, grouped by pk; drop those whose document is gone
        var pks = new HashSet<string>(StringComparer.Ordinal);
        Dictionary<string, AttributeValue>? lastKey = null;
        do
        {
            var scan = await this.client.ScanAsync(new ScanRequest
            {
                TableName = this.TableName,
                FilterExpression = "begins_with(pk, :prefix)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> { [":prefix"] = new() { S = prefix } },
                ProjectionExpression = "pk",
                ExclusiveStartKey = lastKey
            }, cancellationToken).ConfigureAwait(false);
            foreach (var item in scan.Items)
                pks.Add(item["pk"].S);
            lastKey = scan.LastEvaluatedKey is { Count: > 0 } ? scan.LastEvaluatedKey : null;
        }
        while (lastKey != null);

        var removed = 0;
        foreach (var pk in pks)
        {
            var docId = pk[prefix.Length..];
            if (await this.GetItemAsync(partitionKey, docId, cancellationToken).ConfigureAwait(false) == null)
            {
                foreach (var key in await this.QueryBlobKeysAsync(pk, cancellationToken).ConfigureAwait(false))
                    await this.DeleteBlobItemAsync(pk, key, cancellationToken).ConfigureAwait(false);
                removed++;
            }
        }
        return removed;
    }

    sealed class DynamoBlobLoader(DynamoDbDocumentStore store, string typeName, string id) : IBlobLoader
    {
        public Task<byte[]?> LoadOneAsync(string blobKey, CancellationToken ct)
            => store.ReadOneBlobAsync(typeName, id, blobKey, ct);

        public Task LoadManyAsync(IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
            => store.ReadManyBlobsAsync(typeName, id, blobs, ct);
    }
}
