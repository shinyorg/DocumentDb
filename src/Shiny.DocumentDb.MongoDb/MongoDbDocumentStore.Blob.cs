using System.Text.Json.Serialization.Metadata;
using MongoDB.Bson;
using MongoDB.Driver;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.MongoDb;

// Blob payloads live in a {collection}_blobs sidecar (BsonDocuments keyed "{typeName}:{id}:{key}", binary
// Data) so document reads never materialize them; only metadata rides along in the document body. Writes are
// blobs-first so a crash can only orphan bytes, never dangle metadata; cascade delete keeps them consistent.
public partial class MongoDbDocumentStore : IBlobDocumentStore
{
    /// <summary>A BSON document is capped at 16 MB; 15 MB leaves headroom for the sidecar row's metadata.</summary>
    public long MaxBlobSize => 15L * 1024 * 1024;

    IMongoCollection<BsonDocument> BlobCollection<T>()
        => this.database.GetCollection<BsonDocument>(this.ResolveCollectionName<T>() + "_blobs");

    IReadOnlyList<BlobMapping> BlobMappings<T>() => this.options.ResolveBlobMappings(typeof(T));

    static FilterDefinition<BsonDocument> BlobDocFilter(string id, string typeName)
        => Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq("DocId", id),
            Builders<BsonDocument>.Filter.Eq("TypeName", typeName));

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

            var rowId = $"{typeName}:{id}:{b.Key}";
            var doc = new BsonDocument
            {
                ["_id"] = rowId,
                ["DocId"] = id,
                ["TypeName"] = typeName,
                ["BlobKey"] = b.Key,
                ["Data"] = new BsonBinaryData(b.RawBytes!),
                ["Length"] = b.Blob.Length,
                ["ContentType"] = b.Blob.ContentType == null ? BsonNull.Value : new BsonString(b.Blob.ContentType),
                ["FileName"] = b.Blob.FileName == null ? BsonNull.Value : new BsonString(b.Blob.FileName),
                ["Hash"] = b.Blob.Hash == null ? BsonNull.Value : new BsonString(b.Blob.Hash),
                ["CreatedAt"] = b.Blob.CreatedAt.UtcDateTime,
                ["UpdatedAt"] = b.Blob.UpdatedAt.UtcDateTime
            };
            await col.ReplaceOneAsync(
                Builders<BsonDocument>.Filter.Eq("_id", rowId), doc,
                new ReplaceOptions { IsUpsert = true }, ct).ConfigureAwait(false);
            b.Blob.IsPendingWrite = false;
        }

        if (!prune)
            return;

        var keep = blobs.Select(x => x.Key).ToList();
        await col.DeleteManyAsync(
            Builders<BsonDocument>.Filter.And(BlobDocFilter(id, typeName), Builders<BsonDocument>.Filter.Nin("BlobKey", keep)), ct)
            .ConfigureAwait(false);
    }

    async Task DeleteBlobsAsync<T>(string id, string typeName, CancellationToken ct) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;
        await this.BlobCollection<T>().DeleteManyAsync(BlobDocFilter(id, typeName), ct).ConfigureAwait(false);
    }

    internal void AttachBlobLoaders<T>(T document) where T : class
    {
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0 || document == null)
            return;
        var id = this.idCache.GetOrCreate<T>(null).GetIdAsString(document);
        BlobSupport.AttachLoaders(mappings, document,
            new MongoBlobLoader(this, this.ResolveCollectionName<T>() + "_blobs", this.ResolveTypeName<T>(), id));
    }

    // Deserialize + stamp blob loaders — the materialization seam for Mongo's read paths.
    T? Materialize<T>(BsonDocument dataDoc, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var doc = Deserialize(dataDoc, typeInfo, this.jsonOptions);
        if (doc != null)
            this.AttachBlobLoaders(doc);
        return doc;
    }

    async Task<byte[]?> ReadOneBlobAsync(string blobCollection, string typeName, string id, string key, CancellationToken ct)
    {
        var row = await this.database.GetCollection<BsonDocument>(blobCollection)
            .Find(Builders<BsonDocument>.Filter.Eq("_id", $"{typeName}:{id}:{key}"))
            .FirstOrDefaultAsync(ct).ConfigureAwait(false);
        return row != null && !row["Data"].IsBsonNull ? row["Data"].AsByteArray : null;
    }

    async Task ReadManyBlobsAsync(string blobCollection, string typeName, string id, IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
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

        var cursor = await this.database.GetCollection<BsonDocument>(blobCollection)
            .Find(Builders<BsonDocument>.Filter.And(BlobDocFilter(id, typeName), Builders<BsonDocument>.Filter.In("BlobKey", wanted.Keys)))
            .ToListAsync(ct).ConfigureAwait(false);
        foreach (var row in cursor)
        {
            if (!wanted.TryGetValue(row["BlobKey"].AsString, out var targets) || row["Data"].IsBsonNull)
                continue;
            var data = row["Data"].AsByteArray;
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
        return this.ReadOneBlobAsync(this.ResolveCollectionName<T>() + "_blobs", this.ResolveTypeName<T>(), resolvedId, key, cancellationToken);
    }

    /// <inheritdoc />
    public async Task BatchLoadBlobs<T>(IEnumerable<T> documents, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(documents);
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0)
            throw new InvalidOperationException($"'{typeof(T).Name}' has no mapped blob properties.");

        var accessor = this.idCache.GetOrCreate<T>(null);
        var blobCollection = this.ResolveCollectionName<T>() + "_blobs";
        var typeName = this.ResolveTypeName<T>();
        foreach (var document in documents)
        {
            if (document == null)
                continue;
            var pending = BlobSupport.Enumerate(mappings, document).Where(b => !b.IsLoaded).ToList();
            if (pending.Count > 0)
                await this.ReadManyBlobsAsync(blobCollection, typeName, accessor.GetIdAsString(document), pending, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<int> SweepOrphanedBlobs<T>(CancellationToken cancellationToken = default) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return 0;

        var typeName = this.ResolveTypeName<T>();
        var blobCol = this.BlobCollection<T>();
        var mainCol = this.GetCollection<T>();

        var removed = 0;
        var rows = await blobCol.Find(Builders<BsonDocument>.Filter.Eq("TypeName", typeName))
            .Project(Builders<BsonDocument>.Projection.Include("_id").Include("DocId"))
            .ToListAsync(cancellationToken).ConfigureAwait(false);
        foreach (var row in rows)
        {
            var exists = await mainCol.Find(Builders<BsonDocument>.Filter.Eq(MongoFields.Id, CompositeId(typeName, row["DocId"].AsString)))
                .AnyAsync(cancellationToken).ConfigureAwait(false);
            if (!exists)
            {
                await blobCol.DeleteOneAsync(Builders<BsonDocument>.Filter.Eq("_id", row["_id"]), cancellationToken).ConfigureAwait(false);
                removed++;
            }
        }
        return removed;
    }

    sealed class MongoBlobLoader(MongoDbDocumentStore store, string blobCollection, string typeName, string id) : IBlobLoader
    {
        public Task<byte[]?> LoadOneAsync(string blobKey, CancellationToken ct)
            => store.ReadOneBlobAsync(blobCollection, typeName, id, blobKey, ct);

        public Task LoadManyAsync(IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
            => store.ReadManyBlobsAsync(blobCollection, typeName, id, blobs, ct);
    }
}
