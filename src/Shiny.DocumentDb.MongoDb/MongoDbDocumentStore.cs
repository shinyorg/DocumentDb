using Shiny.DocumentDb.Internal.Query;
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

public partial class MongoDbDocumentStore : DocumentProviderBase, IDocumentStore, ITemporalDocumentStore, IDocumentMaintenance, IUnitOfWorkEngine, IDisposable
{
    /// <inheritdoc />
    public async Task ClearAll(CancellationToken cancellationToken = default)
    {
        using var cursor = await this.database
            .ListCollectionNamesAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        var names = await cursor.ToListAsync(cancellationToken).ConfigureAwait(false);

        foreach (var name in names)
        {
            var collection = this.database.GetCollection<BsonDocument>(name);
            await collection
                .DeleteManyAsync(FilterDefinition<BsonDocument>.Empty, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    readonly MongoDbDocumentStoreOptions options;
    readonly IMongoClient client;
    readonly IMongoDatabase database;
    readonly JsonSerializerOptions jsonOptions;
    readonly IdAccessorCache idCache;
    readonly Action<string>? logging;
    readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> ftsIndexed = new();

    public MongoDbDocumentStore(MongoDbDocumentStoreOptions options)
    {
        this.options = options;
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        this.logging = options.Logging;
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName, options.IdConverters);
        this.client = options.MongoClient ?? new MongoClient(options.ConnectionString);
        this.database = this.client.GetDatabase(options.DatabaseName);
        options.ResolveVersionJsonPaths(this.jsonOptions);
        options.ResolveVectorJsonPaths(this.jsonOptions);
        options.ResolveFullTextJsonPaths(this.jsonOptions);
        options.ResolveSpatialJsonPaths(this.jsonOptions);
        options.ResolveComputedJsonNames(this.jsonOptions);
    }

    public bool SupportsVector => this.options.vectorMappings.Count > 0;

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

    internal MongoDbDocumentStoreOptions Options => this.options;

    bool PassesGlobalFilters<T>(T document) where T : class
    {
        var filters = this.options.ResolveQueryFilters(typeof(T));
        if (filters.Count == 0)
            return true;
        foreach (var f in filters)
        {
            var compiled = ExpressionInterpreter.Interpret((Expression<Func<T, bool>>)f.Predicate);
            if (!compiled(document))
                return false;
        }
        return true;
    }

    bool MongoStoredPassesFilters<T>(BsonDocument existing, JsonTypeInfo<T>? typeInfo) where T : class
    {
        if (this.options.ResolveQueryFilters(typeof(T)).Count == 0)
            return true;
        var data = existing[MongoFields.Data].AsBsonDocument;
        var doc = Deserialize(data, typeInfo, this.jsonOptions);
        return doc != null && this.PassesGlobalFilters(doc);
    }

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

            case IdKind.Custom:
                return accessor.GenerateOrThrow();

            default:
                throw new InvalidOperationException($"Unsupported Id kind: {accessor.Kind}");
        }
    }

    static BsonDocument BuildEnvelope(string id, string typeName, string dataJson, DateTime now, DateTime? createdAt = null, DateTime? updatedAt = null)
    {
        return new BsonDocument
        {
            { MongoFields.Id, CompositeId(typeName, id) },
            { MongoFields.DocId, id },
            { MongoFields.TypeName, typeName },
            { MongoFields.Data, BsonDocument.Parse(dataJson) },
            { MongoFields.CreatedAt, createdAt ?? now },
            { MongoFields.UpdatedAt, updatedAt ?? now }
        };
    }

    internal override InterceptorPipeline Interceptors => this.options.Interceptors;

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

        var ctx = this.NewWriteContext(DocumentOperation.Insert, typeName, null, document);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);
        if (ctx?.Document is T mutated)
            document = mutated;

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
        await this.AppendHistoryAsync<T>(id, typeName, TemporalOperation.Inserted, json, cancellationToken).ConfigureAwait(false);
        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document) ?? 1, cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var collection = this.GetCollection<T>();

        var docList = documents as IReadOnlyList<T> ?? documents.ToList();

        // Per-doc BeforeWrite (before serialization).
        DocumentWriteContext[]? ctxs = null;
        if (this.HasPerDocInterceptors)
        {
            var mutable = docList.ToList();
            ctxs = await this.RunBeforeWriteBatchAsync(mutable, typeName, cancellationToken).ConfigureAwait(false);
            docList = mutable;
        }

        var envelopes = new List<BsonDocument>();
        var history = new List<(string id, string json)>();
        long nextInt = -1;
        var now = DateTime.UtcNow;

        foreach (var document in docList)
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
            history.Add((id, json));
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

        for (var i = 0; i < history.Count; i++)
        {
            await this.AppendHistoryAsync<T>(history[i].id, typeName, TemporalOperation.Inserted, history[i].json, cancellationToken).ConfigureAwait(false);
            if (ctxs != null)
                await this.RunAfterWriteAsync(ctxs[i], history[i].id, versionMapping?.GetVersion(docList[i]) ?? 1, cancellationToken).ConfigureAwait(false);
        }

        return envelopes.Count;
    }

    public async Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var typeName = this.ResolveTypeName<T>();

        var ctx = this.NewWriteContext(DocumentOperation.Update, typeName, null, document);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);
        if (ctx?.Document is T mutated)
            document = mutated;

        if (accessor.IsDefaultId(document))
            throw new InvalidOperationException(
                $"Update requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Update.");

        var id = accessor.GetIdAsString(document);
        var collection = this.GetCollection<T>();
        var compositeId = CompositeId(typeName, id);

        var existingFilter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId);
        var existing = await collection.Find(existingFilter).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"No document of type '{typeName}' with Id '{id}' was found to update.");

        if (!this.MongoStoredPassesFilters<T>(existing, typeInfo))
            throw new InvalidOperationException($"No document of type '{typeName}' with Id '{id}' was found to update.");

        var updateFilter = existingFilter;
        var expectedVersion = 0;
        if (versionMapping != null)
        {
            expectedVersion = versionMapping.GetVersion(document);
            var storedVersion = ReadVersion(existing[MongoFields.Data].AsBsonDocument, versionMapping.JsonPath);
            if (storedVersion != expectedVersion)
                throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
            versionMapping.SetVersion(document, expectedVersion + 1);

            // Push the expected version into the filter so the swap is atomic server-side: if another
            // writer bumps the version between the read above and this update, MatchedCount comes back 0.
            updateFilter = Builders<BsonDocument>.Filter.And(
                existingFilter,
                Builders<BsonDocument>.Filter.Eq($"{MongoFields.Data}.{versionMapping.JsonPath}", expectedVersion));
        }

        var json = Serialize(document, typeInfo, this.jsonOptions);
        var update = Builders<BsonDocument>.Update
            .Set(MongoFields.Data, BsonDocument.Parse(json))
            .Set(MongoFields.UpdatedAt, DateTime.UtcNow);

        this.Log($"MongoDB UPDATE {this.ResolveCollectionName<T>()} Id={id}");
        var result = await collection.UpdateOneAsync(updateFilter, update, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (versionMapping != null && result.MatchedCount == 0)
            throw new ConcurrencyException(typeName, id, expectedVersion);
        await this.AppendHistoryAsync<T>(id, typeName, TemporalOperation.Updated, json, cancellationToken).ConfigureAwait(false);
        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document), cancellationToken).ConfigureAwait(false);
    }

    public async Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var typeName = this.ResolveTypeName<T>();

        var ctx = this.NewWriteContext(DocumentOperation.Upsert, typeName, null, patch);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);
        if (ctx?.Document is T mutated)
            patch = mutated;

        if (accessor.IsDefaultId(patch))
            throw new InvalidOperationException(
                $"Upsert requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

        var id = accessor.GetIdAsString(patch);
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
            await this.AppendHistoryAsync<T>(id, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
            return;
        }

        var updateFilter = filter;
        var guardVersion = 0;
        if (versionMapping != null)
        {
            var expectedVersion = versionMapping.GetVersion(patch);
            var storedVersion = ReadVersion(existing[MongoFields.Data].AsBsonDocument, versionMapping.JsonPath);
            if (expectedVersion > 0 && storedVersion != expectedVersion)
                throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
            versionMapping.SetVersion(patch, storedVersion + 1);

            // Only guard when the caller supplied a version to check against; a blind upsert
            // (version 0) keeps last-write-wins. The version predicate makes the swap atomic.
            if (expectedVersion > 0)
            {
                guardVersion = expectedVersion;
                updateFilter = Builders<BsonDocument>.Filter.And(
                    filter,
                    Builders<BsonDocument>.Filter.Eq($"{MongoFields.Data}.{versionMapping.JsonPath}", expectedVersion));
            }
        }

        var patchJson2 = Serialize(patch, typeInfo, this.jsonOptions);
        patchJson2 = StripNullProperties(patchJson2);
        var originalJson = existing[MongoFields.Data].AsBsonDocument.ToJson();
        var merged = MergeJson(originalJson, patchJson2);

        var update = Builders<BsonDocument>.Update
            .Set(MongoFields.Data, BsonDocument.Parse(merged))
            .Set(MongoFields.UpdatedAt, now);

        this.Log($"MongoDB UPSERT (merge) {this.ResolveCollectionName<T>()} Id={id}");
        var result = await collection.UpdateOneAsync(updateFilter, update, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (guardVersion > 0 && result.MatchedCount == 0)
            throw new ConcurrencyException(typeName, id, guardVersion);
        await this.AppendHistoryAsync<T>(id, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
    }

    // A type is eligible for the bulk-write fast path only when none of the per-document concerns apply —
    // version guards, temporal history, query filters, tenant scoping, or per-doc interceptors. Anything
    // else loops the single-doc method (the proven per-doc path) instead.
    bool MongoBatchEligible(Type documentType)
        => this.options.ResolveVersionMapping(documentType) == null
        && this.options.ResolveTemporalMapping(documentType) == null
        && this.options.ResolveQueryFilters(documentType).Count == 0
        && !this.options.Interceptors.HasPerDoc;

    public async Task<int> BatchUpsert<T>(IEnumerable<T> patches, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var list = patches as IReadOnlyList<T> ?? patches.ToList();
        if (list.Count == 0)
            return 0;

        if (!this.MongoBatchEligible(typeof(T)))
        {
            foreach (var patch in list)
                await this.Upsert(patch, typeInfo, cancellationToken).ConfigureAwait(false);
            return list.Count;
        }

        foreach (var patch in list)
            if (accessor.IsDefaultId(patch))
                throw new InvalidOperationException(
                    $"Upsert requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling BatchUpsert.");

        var collection = this.GetCollection<T>();
        var now = DateTime.UtcNow;

        // Read the existing rows once so the RFC 7396 deep merge can be applied client-side.
        var compositeIds = new List<string>(list.Count);
        foreach (var patch in list)
            compositeIds.Add(CompositeId(typeName, accessor.GetIdAsString(patch)));
        var existingDocs = await collection
            .Find(Builders<BsonDocument>.Filter.In(MongoFields.Id, compositeIds))
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);
        var existingById = new Dictionary<string, BsonDocument>(existingDocs.Count);
        foreach (var d in existingDocs)
            existingById[d[MongoFields.Id].AsString] = d[MongoFields.Data].AsBsonDocument;

        var models = new List<WriteModel<BsonDocument>>(list.Count);
        foreach (var patch in list)
        {
            var id = accessor.GetIdAsString(patch);
            var compositeId = CompositeId(typeName, id);
            var patchJson = StripNullProperties(Serialize(patch, typeInfo, this.jsonOptions));
            if (existingById.TryGetValue(compositeId, out var existingData))
            {
                var merged = MergeJson(existingData.ToJson(), patchJson);
                var update = Builders<BsonDocument>.Update
                    .Set(MongoFields.Data, BsonDocument.Parse(merged))
                    .Set(MongoFields.UpdatedAt, now);
                models.Add(new UpdateOneModel<BsonDocument>(Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId), update));
            }
            else
            {
                models.Add(new InsertOneModel<BsonDocument>(BuildEnvelope(id, typeName, patchJson, now)));
            }
        }

        this.Log($"MongoDB BATCH UPSERT {this.ResolveCollectionName<T>()} ({models.Count})");
        await collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = true }, cancellationToken).ConfigureAwait(false);
        return list.Count;
    }

    public async Task<int> BatchUpdate<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var list = documents as IReadOnlyList<T> ?? documents.ToList();
        if (list.Count == 0)
            return 0;

        if (!this.MongoBatchEligible(typeof(T)))
        {
            foreach (var document in list)
                await this.Update(document, typeInfo, cancellationToken).ConfigureAwait(false);
            return list.Count;
        }

        var collection = this.GetCollection<T>();
        var now = DateTime.UtcNow;
        var models = new List<WriteModel<BsonDocument>>(list.Count);
        foreach (var document in list)
        {
            if (accessor.IsDefaultId(document))
                throw new InvalidOperationException(
                    $"Update requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling BatchUpdate.");
            var id = accessor.GetIdAsString(document);
            var json = Serialize(document, typeInfo, this.jsonOptions);
            var update = Builders<BsonDocument>.Update
                .Set(MongoFields.Data, BsonDocument.Parse(json))
                .Set(MongoFields.UpdatedAt, now);
            models.Add(new UpdateOneModel<BsonDocument>(Builders<BsonDocument>.Filter.Eq(MongoFields.Id, CompositeId(typeName, id)), update));
        }

        this.Log($"MongoDB BATCH UPDATE {this.ResolveCollectionName<T>()} ({models.Count})");
        var result = await collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = true }, cancellationToken).ConfigureAwait(false);
        if (result.MatchedCount != list.Count)
            throw new InvalidOperationException(
                $"BatchUpdate matched {result.MatchedCount} of {list.Count} documents of type '{typeName}'; some Ids were not found.");
        return list.Count;
    }

    public async Task<int> BatchRemove<T>(IEnumerable<object> ids, CancellationToken cancellationToken = default) where T : class
    {
        var accessor = this.idCache.GetOrCreate<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var idList = ids as IReadOnlyList<object> ?? ids.ToList();
        if (idList.Count == 0)
            return 0;

        if (!this.MongoBatchEligible(typeof(T)))
        {
            var removed = 0;
            foreach (var id in idList)
                if (await this.Remove<T>(id, cancellationToken).ConfigureAwait(false))
                    removed++;
            return removed;
        }

        var collection = this.GetCollection<T>();
        var compositeIds = new List<string>(idList.Count);
        foreach (var id in idList)
            compositeIds.Add(CompositeId(typeName, accessor.ResolveId(id)));
        this.Log($"MongoDB BATCH DELETE {this.ResolveCollectionName<T>()} ({compositeIds.Count})");
        var result = await collection
            .DeleteManyAsync(Builders<BsonDocument>.Filter.In(MongoFields.Id, compositeIds), cancellationToken)
            .ConfigureAwait(false);
        return (int)result.DeletedCount;
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
        var existingForFilters = await collection.Find(filter).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        if (existingForFilters == null || !this.MongoStoredPassesFilters<T>(existingForFilters, typeInfo))
            return false;
        var jsonValue = value == null ? null : JsonSerializer.Serialize(value, this.jsonOptions);
        var bsonValue = ConvertJsonToBson(jsonValue);

        var update = Builders<BsonDocument>.Update
            .Set($"{MongoFields.Data}.{jsonPath}", bsonValue)
            .Set(MongoFields.UpdatedAt, DateTime.UtcNow);

        this.Log($"MongoDB SET PROPERTY {this.ResolveCollectionName<T>()} Id={resolvedId} Path={jsonPath}");
        var result = await collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (result.MatchedCount == 0)
            return false;
        await this.AppendHistoryAsync<T>(resolvedId, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
        return true;
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
        var existingForFilters = await collection.Find(filter).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        if (existingForFilters == null || !this.MongoStoredPassesFilters<T>(existingForFilters, typeInfo))
            return false;
        var update = Builders<BsonDocument>.Update
            .Unset($"{MongoFields.Data}.{jsonPath}")
            .Set(MongoFields.UpdatedAt, DateTime.UtcNow);

        this.Log($"MongoDB REMOVE PROPERTY {this.ResolveCollectionName<T>()} Id={resolvedId} Path={jsonPath}");
        var result = await collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (result.MatchedCount == 0)
            return false;
        await this.AppendHistoryAsync<T>(resolvedId, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
        return true;
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

        var deserialized = Deserialize(doc[MongoFields.Data].AsBsonDocument, typeInfo, this.jsonOptions);
        if (deserialized != null && !this.PassesGlobalFilters(deserialized))
            return null;
        return deserialized;
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
        if (doc == null || !this.MongoStoredPassesFilters<T>(doc, typeInfo))
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
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;
        if (!hasFilters)
        {
            var count = await collection.CountDocumentsAsync(filter, cancellationToken: cancellationToken).ConfigureAwait(false);
            return (int)count;
        }
        var docs = await collection.Find(filter).ToListAsync(cancellationToken).ConfigureAwait(false);
        var matched = 0;
        foreach (var d in docs)
            if (this.MongoStoredPassesFilters<T>(d, null))
                matched++;
        return matched;
    }

    public async Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = CompositeId(typeName, resolvedId);

        var ctx = this.NewWriteContext<T>(DocumentOperation.Delete, typeName, id, null);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);

        var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, compositeId);
        var existing = await collection.Find(filter).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        if (existing == null || !this.MongoStoredPassesFilters<T>(existing, null))
            return false;
        this.Log($"MongoDB DELETE {this.ResolveCollectionName<T>()} Id={resolvedId}");
        var result = await collection.DeleteOneAsync(filter, cancellationToken).ConfigureAwait(false);
        if (result.DeletedCount == 0)
            return false;
        await this.AppendHistoryAsync<T>(resolvedId, typeName, TemporalOperation.Removed, null, cancellationToken).ConfigureAwait(false);
        await this.RunAfterWriteAsync(ctx, id, null, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName);
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Clear, typeName);
        await this.RunBeforeBulkAsync(bulkCtx, cancellationToken).ConfigureAwait(false);

        this.Log($"MongoDB CLEAR {this.ResolveCollectionName<T>()}");
        int deleted;
        if (!hasFilters)
        {
            var result = await collection.DeleteManyAsync(filter, cancellationToken).ConfigureAwait(false);
            deleted = (int)result.DeletedCount;
        }
        else
        {
            var docs = await collection.Find(filter).ToListAsync(cancellationToken).ConfigureAwait(false);
            deleted = 0;
            foreach (var d in docs)
            {
                if (this.MongoStoredPassesFilters<T>(d, null))
                {
                    var idFilter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, d[MongoFields.Id]);
                    var r = await collection.DeleteOneAsync(idFilter, cancellationToken).ConfigureAwait(false);
                    deleted += (int)r.DeletedCount;
                }
            }
        }
        await this.RunAfterBulkAsync(bulkCtx, deleted, cancellationToken).ConfigureAwait(false);
        return deleted;
    }

    /// <inheritdoc />
    public UnitOfWork CreateUnitOfWork() => new(this);

    async Task IUnitOfWorkEngine.RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        // MongoDB's atomic transactions require a replica set. To keep behaviour consistent with
        // a single-node deployment we fall back to a compensating pattern (matching the CosmosDB
        // provider): inserts are tracked and deleted on failure. Updates/removes inside the
        // unit are NOT compensated.
        var tx = new MongoDbCompensatingStore(this);
        try
        {
            await work(tx, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    public async Task<IReadOnlyList<VectorResult<T>>> NearestVectors<T>(
        ReadOnlyMemory<float> query,
        int k,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var mapping = this.options.ResolveVectorMapping(typeof(T))
            ?? throw new NotSupportedException(
                $"No vector property mapped for type '{typeof(T).Name}'. Call MapVectorProperty<{typeof(T).Name}>() in options.");

        if (query.Length != mapping.Dimensions)
            throw new ArgumentException(
                $"Query vector has {query.Length} dimensions; mapping expects {mapping.Dimensions}.", nameof(query));

        if (k <= 0)
            throw new ArgumentOutOfRangeException(nameof(k));

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();

        // numCandidates default heuristic for Atlas Vector Search: 10 * k.
        var numCandidates = 10 * k;
        if (mapping.IndexOptions.ProviderHints.TryGetValue("atlas.numCandidates", out var hint) && hint is int n)
            numCandidates = Math.Max(k, n);

        // Atlas Search index name — convention "vector_index_{type}", overridable via hint.
        var indexName = mapping.IndexOptions.ProviderHints.TryGetValue("atlas.indexName", out var nm) && nm is string s
            ? s
            : $"vector_index_{typeName}";

        // Build the queryVector as an array literal in the $vectorSearch stage.
        var qv = new BsonArray();
        var span = query.Span;
        for (var i = 0; i < span.Length; i++) qv.Add(span[i]);

        // Pre-filter: typeName match plus optional user filter.
        var filterDoc = new BsonDocument { { MongoFields.TypeName, typeName } };
        // (User filter expressions are not translated to MongoDB native filter language here;
        // they are post-applied to results to keep parity with the contract.)

        var vectorSearch = new BsonDocument
        {
            { "$vectorSearch", new BsonDocument
                {
                    { "index", indexName },
                    { "path", $"{MongoFields.Data}.{mapping.JsonPath}" },
                    { "queryVector", qv },
                    { "numCandidates", numCandidates },
                    // When a user predicate is post-applied, fetch a wider slice so the post-filter doesn't
                    // truncate below k; results are trimmed to k after filtering.
                    { "limit", filter != null ? numCandidates : k },
                    { "filter", filterDoc }
                }
            }
        };

        var projection = new BsonDocument
        {
            { "$project", new BsonDocument
                {
                    { "data", $"${MongoFields.Data}" },
                    { "score", new BsonDocument("$meta", "vectorSearchScore") }
                }
            }
        };

        var pipeline = new BsonDocument[] { vectorSearch, projection };
        var pipelineDef = PipelineDefinition<BsonDocument, BsonDocument>.Create(pipeline);

        this.Log("$vectorSearch on " + collection.CollectionNamespace.CollectionName);

        List<BsonDocument> rows;
        try
        {
            rows = await collection.Aggregate(pipelineDef).ToListAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (MongoCommandException ex) when (ex.Code == 31082 /* UnsupportedFormat */ || ex.Message.Contains("$vectorSearch"))
        {
            throw new NotSupportedException(
                "MongoDB $vectorSearch is only available on Atlas Vector Search. " +
                "On-prem MongoDB does not support vector queries.",
                ex);
        }

        Func<T, bool>? postFilter = filter == null ? null : ExpressionInterpreter.Interpret(filter);
        var results = new List<VectorResult<T>>(rows.Count);
        foreach (var row in rows)
        {
            if (!row.Contains("data") || row["data"].BsonType != BsonType.Document)
                continue;
            var doc = Deserialize(row["data"].AsBsonDocument, typeInfo, this.jsonOptions);
            if (doc == null) continue;
            if (postFilter != null && !postFilter(doc)) continue;
            var score = row.TryGetValue("score", out var sv) && sv.IsNumeric ? (float)sv.ToDouble() : float.NaN;
            results.Add(new VectorResult<T> { Document = doc, Score = score });
        }
        // Trim the widened, post-filtered candidate set back to the requested k (rows are nearest-first).
        if (filter != null && results.Count > k)
            results = results.GetRange(0, k);
        return results;
    }

    // ── Full-text search (MongoDB $text index + textScore) ───────────────

    public bool SupportsFullText => this.options.fullTextMappings.Count > 0;

    async Task EnsureTextIndexAsync<T>(IMongoCollection<BsonDocument> collection, FullTextMapping mapping, CancellationToken ct) where T : class
    {
        if (!this.ftsIndexed.TryAdd(collection.CollectionNamespace.FullName, 0))
            return;

        var keys = new BsonDocument();
        foreach (var path in mapping.JsonPaths)
            keys[$"{MongoFields.Data}.{path}"] = "text";

        var model = new CreateIndexModel<BsonDocument>(keys, new CreateIndexOptions { Name = "fts_text" });
        try
        {
            await collection.Indexes.CreateOneAsync(model, cancellationToken: ct).ConfigureAwait(false);
        }
        catch
        {
            // Index may already exist (possibly under a different name) — tolerate and let the query run.
            this.ftsIndexed.TryRemove(collection.CollectionNamespace.FullName, out _);
            throw;
        }
    }

    public async Task<IReadOnlyList<FullTextResult<T>>> FullTextSearch<T>(
        string searchText,
        int maxResults = 50,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var mapping = this.options.ResolveFullTextMapping(typeof(T))
            ?? throw new NotSupportedException(
                $"No full-text property mapped for type '{typeof(T).Name}'. Call MapFullTextProperty<{typeof(T).Name}>() in options.");
        ArgumentException.ThrowIfNullOrEmpty(searchText);
        if (maxResults <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxResults));

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        await this.EnsureTextIndexAsync<T>(collection, mapping, cancellationToken).ConfigureAwait(false);

        // A user filter narrows results post-rank; over-fetch so it doesn't starve the top-N.
        var fetch = filter == null ? maxResults : maxResults * 4;

        var match = new BsonDocument
        {
            { MongoFields.TypeName, typeName },
            { "$text", new BsonDocument("$search", searchText) }
        };
        var pipeline = new BsonDocument[]
        {
            new("$match", match),
            new("$addFields", new BsonDocument("_score", new BsonDocument("$meta", "textScore"))),
            new("$sort", new BsonDocument("_score", -1)),
            new("$limit", fetch),
            new("$project", new BsonDocument { { "data", $"${MongoFields.Data}" }, { "score", "$_score" } })
        };
        var pipelineDef = PipelineDefinition<BsonDocument, BsonDocument>.Create(pipeline);

        this.Log("$text search on " + collection.CollectionNamespace.CollectionName);
        var rows = await collection.Aggregate(pipelineDef).ToListAsync(cancellationToken).ConfigureAwait(false);

        var postFilter = filter == null ? null : ExpressionInterpreter.Interpret(filter);
        var results = new List<FullTextResult<T>>(rows.Count);
        foreach (var row in rows)
        {
            if (!row.Contains("data") || row["data"].BsonType != BsonType.Document)
                continue;
            var doc = Deserialize(row["data"].AsBsonDocument, typeInfo, this.jsonOptions);
            if (doc == null) continue;
            if (postFilter != null && !postFilter(doc)) continue;
            var score = row.TryGetValue("score", out var sv) && sv.IsNumeric ? sv.ToDouble() : double.NaN;
            results.Add(new FullTextResult<T> { Document = doc, Score = score });
            if (results.Count >= maxResults) break;
        }
        return results;
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

        // Ensure the 2dsphere index (cached) so a DocumentFunctions spatial predicate in a LINQ Where is
        // index-served from the first call, not just after a dedicated store.Geo* method has run.
        var spatialMapping = this.options.ResolveSpatialMapping(typeof(T));
        if (spatialMapping != null)
            await this.EnsureGeoIndexAsync(collection, spatialMapping, ct).ConfigureAwait(false);

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
        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Delete, typeName);
        await this.RunBeforeBulkAsync(bulkCtx, ct).ConfigureAwait(false);

        var typeFilter = Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName);
        var combined = Builders<BsonDocument>.Filter.And(typeFilter, filter);
        var result = await collection.DeleteManyAsync(combined, ct).ConfigureAwait(false);
        var affected = (int)result.DeletedCount;
        await this.RunAfterBulkAsync(bulkCtx, affected, ct).ConfigureAwait(false);
        return affected;
    }

    internal async Task<int> ExecuteUpdatePropertyAsync<T>(
        FilterDefinition<BsonDocument> filter,
        string jsonPath,
        object? value,
        CancellationToken ct) where T : class
    {
        var collection = this.GetCollection<T>();
        var typeName = this.ResolveTypeName<T>();
        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Update, typeName, assignment: (jsonPath, value));
        await this.RunBeforeBulkAsync(bulkCtx, ct).ConfigureAwait(false);

        var typeFilter = Builders<BsonDocument>.Filter.Eq(MongoFields.TypeName, typeName);
        var combined = Builders<BsonDocument>.Filter.And(typeFilter, filter);

        var jsonValue = value == null ? null : JsonSerializer.Serialize(value, this.jsonOptions);
        var bsonValue = ConvertJsonToBson(jsonValue);

        var update = Builders<BsonDocument>.Update
            .Set($"{MongoFields.Data}.{jsonPath}", bsonValue)
            .Set(MongoFields.UpdatedAt, DateTime.UtcNow);

        var result = await collection.UpdateManyAsync(combined, update, cancellationToken: ct).ConfigureAwait(false);
        var affected = (int)result.MatchedCount;
        await this.RunAfterBulkAsync(bulkCtx, affected, ct).ConfigureAwait(false);
        return affected;
    }

    // ── Compensating transaction wrapper ────────────────────────────────

    sealed class MongoDbCompensatingStore(MongoDbDocumentStore inner) : CompensatingStore
    {
        protected override IDocumentStore Inner => inner;

        public override async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default)
        {
            await inner.Insert(document, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            var accessor = inner.IdCache.GetOrCreate(inner.FindTypeInfo(jsonTypeInfo));
            this.TrackInsert(inner.ResolveTypeNameFor<T>(), accessor.GetIdAsString(document));
        }

        protected override async Task DeleteTrackedAsync(string typeName, string id, CancellationToken ct)
        {
            var collection = inner.database.GetCollection<BsonDocument>(inner.options.ResolveCollectionName(typeName));
            var filter = Builders<BsonDocument>.Filter.Eq(MongoFields.Id, CompositeId(typeName, id));
            await collection.DeleteOneAsync(filter, ct).ConfigureAwait(false);
        }
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
