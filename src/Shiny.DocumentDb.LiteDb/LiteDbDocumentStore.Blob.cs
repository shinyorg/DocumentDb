using System.Text.Json.Serialization.Metadata;
using LiteDB;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.LiteDb;

// Blob payloads live in a {collection}_blobs sidecar (BsonDocuments keyed "{typeName}:{id}:{key}") so document
// reads never materialize them; only metadata rides along in the document body. Writes are blobs-first so a
// crash can only orphan bytes, never leave the document body's metadata pointing at a row that was never
// written. Cascade delete keeps them consistent; LiteDB has no TTL, so no orphan sweep is needed.
public partial class LiteDbDocumentStore : IBlobDocumentStore
{
    /// <summary>LiteDB stores a payload inline in the document's page chain; 16 MB is a safe single-blob cap.</summary>
    public long MaxBlobSize => 16L * 1024 * 1024;

    ILiteCollection<BsonDocument> BlobCollection<T>()
        => this.db.GetCollection<BsonDocument>(this.ResolveCollectionName<T>() + "_blobs");

    IReadOnlyList<BlobMapping> BlobMappings<T>() => this.options.ResolveBlobMappings(typeof(T));

    IReadOnlyList<PreparedBlobRow> PrepareBlobs<T>(T document) where T : class
        => BlobSupport.Prepare(this.BlobMappings<T>(), this.MaxBlobSize, document);

    void SyncBlobs<T>(string id, string typeName, IReadOnlyList<PreparedBlobRow> blobs, bool prune) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;

        var col = this.BlobCollection<T>();
        foreach (var b in blobs)
        {
            if (!b.IsPendingWrite)
                continue;

            col.Upsert(new BsonDocument
            {
                ["_id"] = $"{typeName}:{id}:{b.Key}",
                ["DocId"] = id,
                ["TypeName"] = typeName,
                ["BlobKey"] = b.Key,
                ["Data"] = new BsonValue(b.RawBytes!),
                ["Length"] = b.Blob.Length,
                ["ContentType"] = b.Blob.ContentType == null ? BsonValue.Null : new BsonValue(b.Blob.ContentType),
                ["FileName"] = b.Blob.FileName == null ? BsonValue.Null : new BsonValue(b.Blob.FileName),
                ["Hash"] = b.Blob.Hash == null ? BsonValue.Null : new BsonValue(b.Blob.Hash),
                ["CreatedAt"] = b.Blob.CreatedAt.ToString("o"),
                ["UpdatedAt"] = b.Blob.UpdatedAt.ToString("o")
            });
            b.Blob.IsPendingWrite = false;
        }

        if (!prune)
            return;

        var keep = new HashSet<string>(blobs.Select(x => x.Key), StringComparer.Ordinal);
        foreach (var row in col.Find(LiteDB.Query.And(LiteDB.Query.EQ("DocId", id), LiteDB.Query.EQ("TypeName", typeName))).ToList())
            if (!keep.Contains(row["BlobKey"].AsString))
                col.Delete(row["_id"]);
    }

    void DeleteBlobs<T>(string id, string typeName) where T : class
    {
        if (this.BlobMappings<T>().Count == 0)
            return;
        var col = this.BlobCollection<T>();
        foreach (var row in col.Find(LiteDB.Query.And(LiteDB.Query.EQ("DocId", id), LiteDB.Query.EQ("TypeName", typeName))).ToList())
            col.Delete(row["_id"]);
    }

    internal void AttachBlobLoaders<T>(T document) where T : class
    {
        var mappings = this.BlobMappings<T>();
        if (mappings.Count == 0 || document == null)
            return;

        var id = this.idCache.GetOrCreate<T>(null).GetIdAsString(document);
        var loader = new LiteDbBlobLoader(this, this.ResolveCollectionName<T>() + "_blobs", this.ResolveTypeName<T>(), id);
        BlobSupport.AttachLoaders(mappings, document, loader);
    }

    // Deserialize + stamp blob loaders. The single materialization seam for LiteDB's read paths.
    T? Materialize<T>(string json, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var doc = Deserialize(json, typeInfo, this.jsonOptions);
        if (doc != null)
            this.AttachBlobLoaders(doc);
        return doc;
    }

    byte[]? ReadOneBlob(string blobCollection, string typeName, string id, string key)
    {
        var row = this.db.GetCollection<BsonDocument>(blobCollection).FindById($"{typeName}:{id}:{key}");
        return row != null && !row["Data"].IsNull ? row["Data"].AsBinary : null;
    }

    void ReadManyBlobs(string blobCollection, string typeName, string id, IReadOnlyList<DocumentBlob> blobs)
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

        var col = this.db.GetCollection<BsonDocument>(blobCollection);
        foreach (var row in col.Find(LiteDB.Query.And(LiteDB.Query.EQ("DocId", id), LiteDB.Query.EQ("TypeName", typeName))))
        {
            if (!wanted.TryGetValue(row["BlobKey"].AsString, out var targets) || row["Data"].IsNull)
                continue;
            var data = row["Data"].AsBinary;
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
        return Task.FromResult(this.ReadOneBlob(this.ResolveCollectionName<T>() + "_blobs", this.ResolveTypeName<T>(), resolvedId, key));
    }

    /// <inheritdoc />
    public Task BatchLoadBlobs<T>(IEnumerable<T> documents, CancellationToken cancellationToken = default) where T : class
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
                this.ReadManyBlobs(blobCollection, typeName, accessor.GetIdAsString(document), pending);
        }
        return Task.CompletedTask;
    }

    sealed class LiteDbBlobLoader(LiteDbDocumentStore store, string blobCollection, string typeName, string id) : IBlobLoader
    {
        public Task<byte[]?> LoadOneAsync(string blobKey, CancellationToken ct)
            => Task.FromResult(store.ReadOneBlob(blobCollection, typeName, id, blobKey));

        public Task LoadManyAsync(IReadOnlyList<DocumentBlob> blobs, CancellationToken ct)
        {
            store.ReadManyBlobs(blobCollection, typeName, id, blobs);
            return Task.CompletedTask;
        }
    }
}
