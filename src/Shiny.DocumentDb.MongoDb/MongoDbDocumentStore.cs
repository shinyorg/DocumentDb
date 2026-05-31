using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using MongoDB.Bson;
using MongoDB.Driver;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.MongoDb;

public class MongoDbDocumentStore : IDocumentStore, IDisposable
{
    readonly MongoDbDocumentStoreOptions options;
    readonly IMongoClient client;
    readonly IMongoDatabase database;
    readonly JsonSerializerOptions jsonOptions;
    readonly IdAccessorCache idCache;
    readonly Action<string>? logging;

    public MongoDbDocumentStore(MongoDbDocumentStoreOptions options)
    {
        this.options = options;
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        this.logging = options.Logging;
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName);
        this.client = options.MongoClient ?? new MongoClient(options.ConnectionString);
        this.database = this.client.GetDatabase(options.DatabaseName);
        options.ResolveVersionJsonPaths(this.jsonOptions);
    }

    public void Dispose()
    {
        // IMongoClient is process-wide and pooled — only dispose if we own it.
        if (this.options.MongoClient == null && this.client is IDisposable d)
            d.Dispose();
    }

    void Log(string message) => this.logging?.Invoke(message);

    string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);

    internal string ResolveTypeNameFor<T>() => this.ResolveTypeName<T>();

    string ResolveCollectionName<T>() => this.options.ResolveCollectionName(this.ResolveTypeName<T>());

    internal IMongoCollection<BsonDocument> GetCollection<T>()
        => this.database.GetCollection<BsonDocument>(this.ResolveCollectionName<T>());

    internal JsonSerializerOptions JsonOptions => this.jsonOptions;

    internal IdAccessorCache IdCache => this.idCache;

    internal JsonTypeInfo<T>? FindTypeInfo<T>(JsonTypeInfo<T>? provided)
    {
        if (provided != null)
            return provided;

        if (this.jsonOptions.TryGetTypeInfo(typeof(T), out var info) && info is JsonTypeInfo<T> typed)
            return typed;

        if (!this.options.UseReflectionFallback)
            throw new InvalidOperationException(
                $"No JsonTypeInfo registered for type '{typeof(T).FullName}'. " +
                $"Register it in your JsonSerializerContext or pass a JsonTypeInfo<{typeof(T).Name}> explicitly.");

        return null;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    internal static string Serialize<T>(T value, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? JsonSerializer.Serialize(value, typeInfo) : JsonSerializer.Serialize(value, options);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    internal static T? Deserialize<T>(BsonDocument dataDoc, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
    {
        var json = dataDoc.ToJson(new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.RelaxedExtendedJson });
        return typeInfo != null ? JsonSerializer.Deserialize(json, typeInfo) : JsonSerializer.Deserialize<T>(json, options);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static string ResolvePropertyPath<T>(Expression<Func<T, object>> property, JsonSerializerOptions options, JsonTypeInfo<T>? typeInfo)
        => typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, options, typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, options);

    static string CompositeId(string typeName, string id) => $"{typeName}:{id}";

    string GenerateId<T>(IdAccessor<T> accessor, string typeName) where T : class
    {
        switch (accessor.Kind)
        {
            case IdKind.Guid:
                return Guid.NewGuid().ToString("N");

            case IdKind.String:
                return Guid.NewGuid().ToString();

            case IdKind.Int:
            case IdKind.Long:
                var collection = this.GetCollection<T>();
                var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName);
                var cursor = collection.Find(filter).Project(Builders<BsonDocument>.Projection.Include(MongoFields.DocId)).ToList();
                long max = 0;
                foreach (var doc in cursor)
                {
                    var idStr = doc.GetValue(MongoFields.DocId).AsString;
                    if (long.TryParse(idStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) && v > max)
                        max = v;
                }
                return (max + 1).ToString(CultureInfo.InvariantCulture);

            default:
                throw new InvalidOperationException($"Unsupported Id kind: {accessor.Kind}");
        }
    }

    static BsonDocument BuildEnvelope(string id, string typeName, string dataJson, DateTime now, DateTime? createdAt = null)
    {
        return new BsonDocument
        {
            { MongoFields.Id, CompositeId(typeName, id) },
            { MongoFields.DocId, id },
            { MongoFields.TypeName, typeName },
            { MongoFields.Data, BsonDocument.Parse(dataJson) },
            { MongoFields.CreatedAt, createdAt ?? now },
            { MongoFields.UpdatedAt, now }
        };
    }

    // ── IDocumentStore ──────────────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        return new MongoDbDocumentQuery<T>(this, typeInfo);
    }

    public async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));

        string id;
        if (accessor.IsDefaultId(document))
        {
            if (accessor.Kind == IdKind.String)
                throw new InvalidOperationException(
                    $"Insert requires a non-empty string Id on '{typeof(T).Name}'. " +
                    "String Id properties are not auto-generated during Insert.");

            id = this.GenerateId(accessor, typeName);
            accessor.SetId(document, id);
        }
        else
        {
            id = accessor.GetIdAsString(document);
        }

        versionMapping?.SetVersion(document, 1);
        var json = Serialize(document, typeInfo, this.jsonOptions);
        var envelope = BuildEnvelope(id, typeName, json, DateTime.UtcNow);
        var collection = this.GetCollection<T>();

        this.Log($"MongoDB INSERT into {this.ResolveCollectionName<T>()} Id={id}");
        try
        {
            await collection.InsertOneAsync(envelope, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
        {
            throw new InvalidOperationException(
                $"A document of type '{typeName}' with Id '{id}' already exists.", ex);
        }
    }

    public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var collection = this.GetCollection<T>();

        var envelopes = new List<BsonDocument>();
        long nextInt = -1;
        var now = DateTime.UtcNow;

        foreach (var document in documents)
        {
            string id;
            if (accessor.IsDefaultId(document))
            {
                if (accessor.Kind == IdKind.String)
                    throw new InvalidOperationException(
                        $"Insert requires a non-empty string Id on '{typeof(T).Name}'.");

                if (accessor.Kind is IdKind.Int or IdKind.Long)
                {
                    if (nextInt < 0)
                    {
                        var seed = this.GenerateId(accessor, typeName);
                        nextInt = long.Parse(seed, CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        nextInt++;
                    }
                    id = nextInt.ToString(CultureInfo.InvariantCulture);
                }
                else
                {
                    id = this.GenerateId(accessor, typeName);
                }
                accessor.SetId(document, id);
            }
            else
            {
                id = accessor.GetIdAsString(document);
            }

            versionMapping?.SetVersion(document, 1);
            var json = Serialize(document, typeInfo, this.jsonOptions);
            envelopes.Add(BuildEnvelope(id, typeName, json, now));
        }

        if (envelopes.Count == 0)
            return 0;

        this.Log($"MongoDB BATCH INSERT {envelopes.Count} docs into {this.ResolveCollectionName<T>()}");

        try
        {
            await collection.InsertManyAsync(envelopes, new InsertManyOptions { IsOrdered = true }, cancellationToken).ConfigureAwait(false);
        }
        catch (MongoBulkWriteException ex) when (ex.WriteErrors.Any(e => e.Category == ServerErrorCategory.DuplicateKey))
        {
            throw new InvalidOperationException(
                $"A document of type '{typeName}' has a duplicate Id in the batch.", ex);
        }

        return envelopes.Count;
    }

    public async Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));

        if (accessor.IsDefaultId(document))
            throw new InvalidOperationException(
                $"Update requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Update.");

        var id = accessor.GetIdAsString(document);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = CompositeId(typeName, id);

        var existingFilter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId);
        var existing = await collection.Find(existingFilter).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"No document of type '{typeName}' with Id '{id}' was found to update.");

        if (versionMapping != null)
        {
            var expectedVersion = versionMapping.GetVersion(document);
            var storedVersion = ReadVersion(existing[MongoFields.Data].AsBsonDocument, versionMapping.JsonPath);
            if (storedVersion != expectedVersion)
                throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
            versionMapping.SetVersion(document, expectedVersion + 1);
        }

        var json = Serialize(document, typeInfo, this.jsonOptions);
        var update = Builders<BsonDocument>.Update
            .Set(MongoFields.Data, BsonDocument.Parse(json))
            .Set(MongoFields.UpdatedAt, DateTime.UtcNow);

        this.Log($"MongoDB UPDATE {this.ResolveCollectionName<T>()} Id={id}");
        await collection.UpdateOneAsync(existingFilter, update, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));

        if (accessor.IsDefaultId(patch))
            throw new InvalidOperationException(
                $"Upsert requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

        var id = accessor.GetIdAsString(patch);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = CompositeId(typeName, id);

        var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId);
        var existing = await collection.Find(filter).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var now = DateTime.UtcNow;

        if (existing == null)
        {
            versionMapping?.SetVersion(patch, 1);
            var patchJson = Serialize(patch, typeInfo, this.jsonOptions);
            patchJson = StripNullProperties(patchJson);
            var envelope = BuildEnvelope(id, typeName, patchJson, now);
            this.Log($"MongoDB UPSERT (insert) {this.ResolveCollectionName<T>()} Id={id}");
            await collection.InsertOneAsync(envelope, cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }

        if (versionMapping != null)
        {
            var expectedVersion = versionMapping.GetVersion(patch);
            var storedVersion = ReadVersion(existing[MongoFields.Data].AsBsonDocument, versionMapping.JsonPath);
            if (expectedVersion > 0 && storedVersion != expectedVersion)
                throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
            versionMapping.SetVersion(patch, storedVersion + 1);
        }

        var patchJson2 = Serialize(patch, typeInfo, this.jsonOptions);
        patchJson2 = StripNullProperties(patchJson2);
        var originalJson = existing[MongoFields.Data].AsBsonDocument.ToJson();
        var merged = MergeJson(originalJson, patchJson2);

        var update = Builders<BsonDocument>.Update
            .Set(MongoFields.Data, BsonDocument.Parse(merged))
            .Set(MongoFields.UpdatedAt, now);

        this.Log($"MongoDB UPSERT (merge) {this.ResolveCollectionName<T>()} Id={id}");
        await collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    public async Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = CompositeId(typeName, resolvedId);

        var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId);
        var jsonValue = value == null ? null : JsonSerializer.Serialize(value, this.jsonOptions);
        var bsonValue = ConvertJsonToBson(jsonValue);

        var update = Builders<BsonDocument>.Update
            .Set($"{MongoFields.Data}.{jsonPath}", bsonValue)
            .Set(MongoFields.UpdatedAt, DateTime.UtcNow);

        this.Log($"MongoDB SET PROPERTY {this.ResolveCollectionName<T>()} Id={resolvedId} Path={jsonPath}");
        var result = await collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken).ConfigureAwait(false);
        return result.MatchedCount > 0;
    }

    public async Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = CompositeId(typeName, resolvedId);

        var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId);
        var update = Builders<BsonDocument>.Update
            .Unset($"{MongoFields.Data}.{jsonPath}")
            .Set(MongoFields.UpdatedAt, DateTime.UtcNow);

        this.Log($"MongoDB REMOVE PROPERTY {this.ResolveCollectionName<T>()} Id={resolvedId} Path={jsonPath}");
        var result = await collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken).ConfigureAwait(false);
        return result.MatchedCount > 0;
    }

    public async Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = CompositeId(typeName, resolvedId);

        this.Log($"MongoDB GET {this.ResolveCollectionName<T>()} Id={resolvedId}");
        var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId);
        var doc = await collection.Find(filter).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        if (doc == null)
            return null;

        return Deserialize(doc[MongoFields.Data].AsBsonDocument, typeInfo, this.jsonOptions);
    }

    public async Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = CompositeId(typeName, resolvedId);

        var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId);
        var doc = await collection.Find(filter).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        if (doc == null)
            return null;

        var originalJson = doc[MongoFields.Data].AsBsonDocument.ToJson(new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.RelaxedExtendedJson });
        var modifiedJson = Serialize(modified, typeInfo, this.jsonOptions);
        return JsonDiff.CreatePatch<T>(originalJson, modifiedJson, this.jsonOptions);
    }

    public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("MongoDB does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload instead.");

    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("MongoDB does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload instead.");

    public async Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
    {
        if (!string.IsNullOrWhiteSpace(whereClause))
            throw new NotSupportedException("MongoDB does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload instead.");

        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName);

        this.Log($"MongoDB COUNT {this.ResolveCollectionName<T>()}");
        var count = await collection.CountDocumentsAsync(filter, cancellationToken: cancellationToken).ConfigureAwait(false);
        return (int)count;
    }

    public async Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = CompositeId(typeName, resolvedId);

        var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId);
        this.Log($"MongoDB DELETE {this.ResolveCollectionName<T>()} Id={resolvedId}");
        var result = await collection.DeleteOneAsync(filter, cancellationToken).ConfigureAwait(false);
        return result.DeletedCount > 0;
    }

    public async Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName);

        this.Log($"MongoDB CLEAR {this.ResolveCollectionName<T>()}");
        var result = await collection.DeleteManyAsync(filter, cancellationToken).ConfigureAwait(false);
        return (int)result.DeletedCount;
    }

    public async Task RunInTransaction(Func<IDocumentStore, Task> operation, CancellationToken cancellationToken = default)
    {
        // MongoDB's atomic transactions require a replica set. To keep behaviour consistent with
        // a single-node deployment we fall back to a compensating pattern (matching the CosmosDB
        // provider): inserts are tracked and deleted on failure. Updates/removes inside the
        // transaction are NOT compensated.
        var tx = new MongoDbCompensatingStore(this);
        try
        {
            await operation(tx).ConfigureAwait(false);
        }
        catch
        {
            await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    // ── Internal helpers used by MongoDbDocumentQuery ───────────────────

    internal async Task<IReadOnlyList<T>> ExecuteFindAsync<T>(
        FilterDefinition<BsonDocument> filter,
        SortDefinition<BsonDocument>? sort,
        int? skip,
        int? limit,
        JsonTypeInfo<T>? typeInfo,
        CancellationToken ct) where T : class
    {
        var collection = this.GetCollection<T>();
        var typeName = this.ResolveTypeName<T>();
        var typeFilter = Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName);
        var combined = Builders<BsonDocument>.Filter.And(typeFilter, filter);

        var find = collection.Find(combined);
        if (sort != null) find = find.Sort(sort);
        if (skip.HasValue) find = find.Skip(skip.Value);
        if (limit.HasValue) find = find.Limit(limit.Value);

        var docs = await find.ToListAsync(ct).ConfigureAwait(false);
        var results = new List<T>(docs.Count);
        foreach (var doc in docs)
        {
            var item = Deserialize(doc[MongoFields.Data].AsBsonDocument, typeInfo, this.jsonOptions);
            if (item != null)
                results.Add(item);
        }
        return results;
    }

    internal async Task<long> ExecuteCountAsync<T>(FilterDefinition<BsonDocument> filter, CancellationToken ct) where T : class
    {
        var collection = this.GetCollection<T>();
        var typeName = this.ResolveTypeName<T>();
        var typeFilter = Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName);
        var combined = Builders<BsonDocument>.Filter.And(typeFilter, filter);
        return await collection.CountDocumentsAsync(combined, cancellationToken: ct).ConfigureAwait(false);
    }

    internal async Task<int> ExecuteDeleteAsync<T>(FilterDefinition<BsonDocument> filter, CancellationToken ct) where T : class
    {
        var collection = this.GetCollection<T>();
        var typeName = this.ResolveTypeName<T>();
        var typeFilter = Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName);
        var combined = Builders<BsonDocument>.Filter.And(typeFilter, filter);
        var result = await collection.DeleteManyAsync(combined, ct).ConfigureAwait(false);
        return (int)result.DeletedCount;
    }

    internal async Task<int> ExecuteUpdatePropertyAsync<T>(
        FilterDefinition<BsonDocument> filter,
        string jsonPath,
        object? value,
        CancellationToken ct) where T : class
    {
        var collection = this.GetCollection<T>();
        var typeName = this.ResolveTypeName<T>();
        var typeFilter = Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName);
        var combined = Builders<BsonDocument>.Filter.And(typeFilter, filter);

        var jsonValue = value == null ? null : JsonSerializer.Serialize(value, this.jsonOptions);
        var bsonValue = ConvertJsonToBson(jsonValue);

        var update = Builders<BsonDocument>.Update
            .Set($"{MongoFields.Data}.{jsonPath}", bsonValue)
            .Set(MongoFields.UpdatedAt, DateTime.UtcNow);

        var result = await collection.UpdateManyAsync(combined, update, cancellationToken: ct).ConfigureAwait(false);
        return (int)result.MatchedCount;
    }

    // ── Compensating transaction wrapper ────────────────────────────────

    sealed class MongoDbCompensatingStore(MongoDbDocumentStore inner) : IDocumentStore
    {
        readonly List<(string typeName, string id)> insertedDocs = new();

        public async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            await inner.Insert(document, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            var typeInfo = inner.FindTypeInfo(jsonTypeInfo);
            var accessor = inner.IdCache.GetOrCreate(typeInfo);
            insertedDocs.Add((inner.ResolveTypeName<T>(), accessor.GetIdAsString(document)));
        }

        public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var list = documents.ToList();
            var count = await inner.BatchInsert(list, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            var typeInfo = inner.FindTypeInfo(jsonTypeInfo);
            var accessor = inner.IdCache.GetOrCreate(typeInfo);
            var typeName = inner.ResolveTypeName<T>();
            foreach (var doc in list)
                insertedDocs.Add((typeName, accessor.GetIdAsString(doc)));
            return count;
        }

        internal async Task RollbackAsync(CancellationToken ct)
        {
            foreach (var (typeName, id) in insertedDocs)
            {
                try
                {
                    var collectionName = inner.options.ResolveCollectionName(typeName);
                    var collection = inner.database.GetCollection<BsonDocument>(collectionName);
                    var compositeId = CompositeId(typeName, id);
                    var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId);
                    await collection.DeleteOneAsync(filter, ct).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort rollback
                }
            }
        }

        public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class => inner.Query(jsonTypeInfo);
        public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => inner.Update(document, jsonTypeInfo, cancellationToken);
        public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => inner.Upsert(patch, jsonTypeInfo, cancellationToken);
        public Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => inner.SetProperty(id, property, value, jsonTypeInfo, cancellationToken);
        public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => inner.RemoveProperty(id, property, jsonTypeInfo, cancellationToken);
        public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => inner.Get(id, jsonTypeInfo, cancellationToken);
        public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => inner.GetDiff(id, modified, jsonTypeInfo, cancellationToken);
        public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => inner.Query<T>(whereClause, jsonTypeInfo, parameters, cancellationToken);
        public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => inner.QueryStream<T>(whereClause, jsonTypeInfo, parameters, cancellationToken);
        public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => inner.Count<T>(whereClause, parameters, cancellationToken);
        public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class => inner.Remove<T>(id, cancellationToken);
        public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class => inner.Clear<T>(cancellationToken);
        public Task RunInTransaction(Func<IDocumentStore, Task> operation, CancellationToken cancellationToken = default) => operation(this);
    }

    // ── Private helpers ────────────────────────────────────────────────

    static int ReadVersion(BsonDocument data, string jsonPath)
    {
        var parts = jsonPath.Split('.');
        BsonValue current = data;
        foreach (var part in parts)
        {
            if (current is not BsonDocument obj || !obj.TryGetValue(part, out var next))
                return 0;
            current = next;
        }
        return current.IsInt32 ? current.AsInt32 : current.IsInt64 ? (int)current.AsInt64 : 0;
    }

    static BsonValue ConvertJsonToBson(string? json)
    {
        if (json == null) return BsonNull.Value;
        // Wrap so non-object values still parse: {"v": ...}
        var doc = BsonDocument.Parse($"{{\"v\":{json}}}");
        return doc["v"];
    }

    static string StripNullProperties(string json) => JsonMergePatch.StripNullsRecursive(json);

    static string MergeJson(string originalJson, string patchJson)
    {
        var original = JsonNode.Parse(originalJson)?.AsObject();
        var patch = JsonNode.Parse(patchJson)?.AsObject();

        if (original == null || patch == null)
            return patchJson;

        foreach (var prop in patch)
        {
            if (prop.Value is JsonObject patchObj && original[prop.Key] is JsonObject origObj)
                original[prop.Key] = JsonNode.Parse(MergeJson(origObj.ToJsonString(), patchObj.ToJsonString()));
            else
                original[prop.Key] = prop.Value?.DeepClone();
        }

        return original.ToJsonString();
    }

}
