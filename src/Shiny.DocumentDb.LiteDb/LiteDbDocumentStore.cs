using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using LiteDB;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.LiteDb;

public partial class LiteDbDocumentStore : DocumentProviderBase, IDocumentStore, ITemporalDocumentStore, IObservableDocumentStore, IUnitOfWorkEngine, IDisposable
{
    readonly LiteDatabase db;
    readonly LiteDbDocumentStoreOptions options;
    readonly JsonSerializerOptions jsonOptions;
    readonly IdAccessorCache idCache;
    readonly Action<string>? logging;
    readonly ChangeBroadcaster broadcaster = new();
    // When set, change notifications are buffered until the active transaction commits.
    List<Action>? pendingChanges;

    /// <inheritdoc />
    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange<T>(CancellationToken cancellationToken = default) where T : class
        => this.broadcaster.Observe<T>(cancellationToken);

    internal ChangeBroadcaster Broadcaster => this.broadcaster;

    void PublishChange<T>(DocumentChangeType changeType, string id, T? document) where T : class
    {
        var change = new DocumentChange<T> { ChangeType = changeType, Id = id, Document = document };
        if (this.pendingChanges != null)
            this.pendingChanges.Add(() => this.broadcaster.Publish(change));
        else if (this.broadcaster.HasSubscribers<T>())
            this.broadcaster.Publish(change);
    }

    public LiteDbDocumentStore(LiteDbDocumentStoreOptions options)
    {
        this.options = options;
        this.db = new LiteDatabase(options.ConnectionString);
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        this.logging = options.Logging;
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName, options.IdConverters);
        options.ResolveVersionJsonPaths(this.jsonOptions);
    }

    public void Dispose() => this.db.Dispose();

    void Log(string message) => this.logging?.Invoke(message);

    string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);

    string ResolveCollectionName<T>() => this.options.ResolveCollectionName(this.ResolveTypeName<T>());

    ILiteCollection<BsonDocument> GetCollection<T>() => this.db.GetCollection<BsonDocument>(this.ResolveCollectionName<T>());

    JsonTypeInfo<T>? FindTypeInfo<T>(JsonTypeInfo<T>? provided)
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
    static string Serialize<T>(T value, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? System.Text.Json.JsonSerializer.Serialize(value, typeInfo) : System.Text.Json.JsonSerializer.Serialize(value, options);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static T? Deserialize<T>(string json, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? System.Text.Json.JsonSerializer.Deserialize(json, typeInfo) : System.Text.Json.JsonSerializer.Deserialize<T>(json, options);

    BsonDocument CreateBsonDocument(string id, string typeName, string json)
    {
        var now = DateTimeOffset.UtcNow;
        return new BsonDocument
        {
            ["_id"] = $"{typeName}:{id}",
            ["Id"] = id,
            ["TypeName"] = typeName,
            ["Data"] = json,
            ["CreatedAt"] = now.ToString("o"),
            ["UpdatedAt"] = now.ToString("o")
        };
    }

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
                var maxDoc = collection
                    .Find(LiteDB.Query.EQ("TypeName", typeName))
                    .Select(d => d["Id"].AsString)
                    .Select(s => long.TryParse(s, CultureInfo.InvariantCulture, out var v) ? v : 0L)
                    .DefaultIfEmpty(0L)
                    .Max();
                return (maxDoc + 1).ToString(CultureInfo.InvariantCulture);

            case IdKind.Custom:
                return accessor.GenerateOrThrow();

            default:
                throw new InvalidOperationException($"Unsupported Id kind: {accessor.Kind}");
        }
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static string ResolvePropertyPath<T>(Expression<Func<T, object>> property, JsonSerializerOptions options, JsonTypeInfo<T>? typeInfo)
        => typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, options, typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, options);

    // ── IDocumentStore ──────────────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        return new LiteDbDocumentQuery<T>(this, typeInfo);
    }

    internal override InterceptorPipeline Interceptors => this.options.Interceptors;

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
        var collection = this.GetCollection<T>();
        var compositeId = $"{typeName}:{id}";

        var existing = collection.FindById(compositeId);
        if (existing != null)
            throw new InvalidOperationException(
                $"A document of type '{typeName}' with Id '{id}' already exists.");

        var bson = this.CreateBsonDocument(id, typeName, json);
        this.Log($"LiteDB INSERT into {this.ResolveCollectionName<T>()} Id={id}");
        collection.Insert(bson);
        this.AppendHistory<T>(id, typeName, TemporalOperation.Inserted, json);
        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document) ?? 1, cancellationToken).ConfigureAwait(false);

        this.PublishChange(DocumentChangeType.Inserted, id, document);
    }

    public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();

        var docList = documents as IReadOnlyList<T> ?? documents.ToList();

        // Per-doc BeforeWrite before serialization.
        DocumentWriteContext[]? ctxs = null;
        if (this.HasPerDocInterceptors)
        {
            var mutableDocs = docList.ToList();
            ctxs = await this.RunBeforeWriteBatchAsync(mutableDocs, typeName, cancellationToken).ConfigureAwait(false);
            docList = mutableDocs;
        }

        var bsonDocs = new List<BsonDocument>();
        var inserted = new List<(string id, T document, string json)>();
        long nextInt = -1;

        foreach (var document in docList)
        {
            string id;
            if (accessor.IsDefaultId(document))
            {
                if (accessor.Kind == IdKind.String)
                    throw new InvalidOperationException(
                        $"Insert requires a non-empty string Id on '{typeof(T).Name}'. " +
                        "String Id properties are not auto-generated during Insert.");

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
            bsonDocs.Add(this.CreateBsonDocument(id, typeName, json));
            inserted.Add((id, document, json));
        }

        if (bsonDocs.Count == 0)
            return 0;

        this.db.BeginTrans();
        int count;
        try
        {
            // Check for duplicates
            foreach (var doc in bsonDocs)
            {
                var existing = collection.FindById(doc["_id"]);
                if (existing != null)
                {
                    this.db.Rollback();
                    throw new InvalidOperationException(
                        $"A document of type '{typeName}' has a duplicate Id in the batch.");
                }
            }

            this.Log($"LiteDB BATCH INSERT {bsonDocs.Count} docs into {this.ResolveCollectionName<T>()}");
            count = collection.InsertBulk(bsonDocs);
            for (var i = 0; i < inserted.Count; i++)
            {
                this.AppendHistory<T>(inserted[i].id, typeName, TemporalOperation.Inserted, inserted[i].json);
                if (ctxs != null)
                    await this.RunAfterWriteAsync(ctxs[i], inserted[i].id, versionMapping?.GetVersion(inserted[i].document) ?? 1, cancellationToken).ConfigureAwait(false);
            }
            this.db.Commit();
        }
        catch
        {
            this.db.Rollback();
            throw;
        }

        foreach (var (id, document, _) in inserted)
            this.PublishChange(DocumentChangeType.Inserted, id, document);
        return count;
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
        var compositeId = $"{typeName}:{id}";

        var existing = collection.FindById(compositeId);
        if (existing == null)
            throw new InvalidOperationException(
                $"No document of type '{typeName}' with Id '{id}' was found to update.");

        if (!this.PassesFiltersForStored<T>(existing, typeInfo))
            throw new InvalidOperationException(
                $"No document of type '{typeName}' with Id '{id}' was found to update.");

        if (versionMapping != null)
        {
            var expectedVersion = versionMapping.GetVersion(document);
            var storedData = existing["Data"].AsString;
            var storedNode = JsonNode.Parse(storedData)!.AsObject();
            var storedVersion = storedNode[versionMapping.JsonPath]?.GetValue<int>() ?? 0;
            if (storedVersion != expectedVersion)
                throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
            versionMapping.SetVersion(document, expectedVersion + 1);
        }

        var json = Serialize(document, typeInfo, this.jsonOptions);
        existing["Data"] = json;
        existing["UpdatedAt"] = DateTimeOffset.UtcNow.ToString("o");

        this.Log($"LiteDB UPDATE {this.ResolveCollectionName<T>()} Id={id}");
        collection.Update(existing);
        this.AppendHistory<T>(id, typeName, TemporalOperation.Updated, json);
        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document), cancellationToken).ConfigureAwait(false);

        this.PublishChange(DocumentChangeType.Updated, id, document);
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
        var compositeId = $"{typeName}:{id}";

        var existing = collection.FindById(compositeId);
        var now = DateTimeOffset.UtcNow.ToString("o");

        if (existing == null)
        {
            versionMapping?.SetVersion(patch, 1);
            var patchJson = Serialize(patch, typeInfo, this.jsonOptions);
            patchJson = StripNullProperties(patchJson);
            var bson = this.CreateBsonDocument(id, typeName, patchJson);
            this.Log($"LiteDB UPSERT (insert) {this.ResolveCollectionName<T>()} Id={id}");
            collection.Insert(bson);
        }
        else
        {
            if (versionMapping != null)
            {
                var expectedVersion = versionMapping.GetVersion(patch);
                var storedData = existing["Data"].AsString;
                var storedNode = JsonNode.Parse(storedData)!.AsObject();
                var storedVersion = storedNode[versionMapping.JsonPath]?.GetValue<int>() ?? 0;
                if (expectedVersion > 0 && storedVersion != expectedVersion)
                    throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
                versionMapping.SetVersion(patch, storedVersion + 1);
            }

            var patchJson = Serialize(patch, typeInfo, this.jsonOptions);
            patchJson = StripNullProperties(patchJson);
            var originalJson = existing["Data"].AsString;
            var merged = MergeJson(originalJson, patchJson);
            existing["Data"] = merged;
            existing["UpdatedAt"] = now;

            this.Log($"LiteDB UPSERT (merge) {this.ResolveCollectionName<T>()} Id={id}");
            collection.Update(existing);
        }

        this.AppendHistory<T>(id, typeName, TemporalOperation.Updated, null);
        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Updated, id, patch);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    public Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = $"{typeName}:{resolvedId}";

        var existing = collection.FindById(compositeId);
        if (existing == null || !this.PassesFiltersForStored<T>(existing, typeInfo))
            return Task.FromResult(false);

        var dataJson = existing["Data"].AsString;
        var node = JsonNode.Parse(dataJson)!.AsObject();
        SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(System.Text.Json.JsonSerializer.Serialize(value, this.jsonOptions)));

        existing["Data"] = node.ToJsonString();
        existing["UpdatedAt"] = DateTimeOffset.UtcNow.ToString("o");

        this.Log($"LiteDB SET PROPERTY {this.ResolveCollectionName<T>()} Id={resolvedId} Path={jsonPath}");
        collection.Update(existing);
        this.AppendHistory<T>(resolvedId, typeName, TemporalOperation.Updated, null);
        this.PublishChange<T>(DocumentChangeType.Updated, resolvedId, null);
        return Task.FromResult(true);
    }

    public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = $"{typeName}:{resolvedId}";

        var existing = collection.FindById(compositeId);
        if (existing == null || !this.PassesFiltersForStored<T>(existing, typeInfo))
            return Task.FromResult(false);

        var dataJson = existing["Data"].AsString;
        var node = JsonNode.Parse(dataJson)!.AsObject();
        RemoveNestedProperty(node, jsonPath);

        existing["Data"] = node.ToJsonString();
        existing["UpdatedAt"] = DateTimeOffset.UtcNow.ToString("o");

        this.Log($"LiteDB REMOVE PROPERTY {this.ResolveCollectionName<T>()} Id={resolvedId} Path={jsonPath}");
        collection.Update(existing);
        this.AppendHistory<T>(resolvedId, typeName, TemporalOperation.Updated, null);
        this.PublishChange<T>(DocumentChangeType.Updated, resolvedId, null);
        return Task.FromResult(true);
    }

    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = $"{typeName}:{resolvedId}";

        this.Log($"LiteDB GET {this.ResolveCollectionName<T>()} Id={resolvedId}");
        var doc = collection.FindById(compositeId);
        if (doc == null)
            return Task.FromResult<T?>(null);

        var json = doc["Data"].AsString;
        var deserialized = Deserialize(json, typeInfo, this.jsonOptions);
        if (deserialized != null && !this.PassesGlobalFilters(deserialized))
            return Task.FromResult<T?>(null);
        return Task.FromResult(deserialized);
    }

    public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = $"{typeName}:{resolvedId}";

        var doc = collection.FindById(compositeId);
        if (doc == null || !this.PassesFiltersForStored<T>(doc, typeInfo))
            return Task.FromResult<JsonPatchDocument<T>?>(null);

        var originalJson = doc["Data"].AsString;
        var modifiedJson = Serialize(modified, typeInfo, this.jsonOptions);
        var patch = JsonDiff.CreatePatch<T>(originalJson, modifiedJson, this.jsonOptions);
        return Task.FromResult<JsonPatchDocument<T>?>(patch);
    }

    public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("LiteDB does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload instead.");

    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("LiteDB does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload instead.");

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
    {
        if (!string.IsNullOrWhiteSpace(whereClause))
            throw new NotSupportedException("LiteDB does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload instead.");

        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();

        this.Log($"LiteDB COUNT {this.ResolveCollectionName<T>()}");
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;
        if (!hasFilters)
        {
            var fastCount = collection.Count(LiteDB.Query.EQ("TypeName", typeName));
            return Task.FromResult(fastCount);
        }
        var docs = collection.Find(LiteDB.Query.EQ("TypeName", typeName));
        var count = 0;
        foreach (var d in docs)
            if (this.PassesFiltersForStored<T>(d, null))
                count++;
        return Task.FromResult(count);
    }

    public async Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var compositeId = $"{typeName}:{resolvedId}";

        var ctx = this.NewWriteContext<T>(DocumentOperation.Delete, typeName, id, null);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);

        var existing = collection.FindById(compositeId);
        if (existing == null || !this.PassesFiltersForStored<T>(existing, null))
            return false;

        this.Log($"LiteDB DELETE {this.ResolveCollectionName<T>()} Id={resolvedId}");
        var deleted = collection.Delete(compositeId);
        if (deleted)
        {
            this.AppendHistory<T>(resolvedId, typeName, TemporalOperation.Removed, null);
            await this.RunAfterWriteAsync(ctx, id, null, cancellationToken).ConfigureAwait(false);
            this.PublishChange<T>(DocumentChangeType.Removed, resolvedId, null);
        }
        return deleted;
    }

    public async Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Clear, typeName);
        await this.RunBeforeBulkAsync(bulkCtx, cancellationToken).ConfigureAwait(false);

        this.Log($"LiteDB CLEAR {this.ResolveCollectionName<T>()}");
        int count;
        if (!hasFilters)
        {
            count = collection.DeleteMany(LiteDB.Query.EQ("TypeName", typeName));
        }
        else
        {
            count = 0;
            var docs = collection.Find(LiteDB.Query.EQ("TypeName", typeName)).ToList();
            foreach (var d in docs)
            {
                if (this.PassesFiltersForStored<T>(d, null) && collection.Delete(d["_id"]))
                    count++;
            }
        }
        await this.RunAfterBulkAsync(bulkCtx, count, cancellationToken).ConfigureAwait(false);
        if (count > 0)
            this.PublishChange<T>(DocumentChangeType.Cleared, "", null);
        return count;
    }

    /// <inheritdoc />
    public UnitOfWork CreateUnitOfWork() => new(this);

    Task IUnitOfWorkEngine.RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        this.db.BeginTrans();
        // Buffer change notifications until the transaction commits.
        var buffer = new List<Action>();
        this.pendingChanges = buffer;
        try
        {
            // Create a transactional view that shares the same LiteDatabase instance
            var txStore = new LiteDbTransactionalStore(this);
            work(txStore, cancellationToken).GetAwaiter().GetResult();
            this.db.Commit();
        }
        catch
        {
            this.db.Rollback();
            throw;
        }
        finally
        {
            this.pendingChanges = null;
        }

        foreach (var emit in buffer)
            emit();
        return Task.CompletedTask;
    }

    public Task Backup(string destinationPath, CancellationToken cancellationToken = default)
    {
        this.db.Checkpoint();
        var connectionParts = this.options.ConnectionString.Split(';')
            .Select(p => p.Trim())
            .FirstOrDefault(p => p.StartsWith("Filename=", StringComparison.OrdinalIgnoreCase));

        if (connectionParts == null)
            throw new NotSupportedException("Backup requires a file-based LiteDB connection string with a Filename parameter.");

        var sourcePath = connectionParts["Filename=".Length..];
        File.Copy(sourcePath, destinationPath, overwrite: true);
        return Task.CompletedTask;
    }

    // ── Internal helpers used by LiteDbDocumentQuery ────────────────────

    internal IEnumerable<T> LoadDocuments<T>(string typeName, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var collection = this.GetCollection<T>();
        var docs = collection.Find(LiteDB.Query.EQ("TypeName", typeName));

        foreach (var doc in docs)
        {
            var json = doc["Data"].AsString;
            var obj = Deserialize(json, typeInfo, this.jsonOptions);
            if (obj != null)
                yield return obj;
        }
    }

    // ── Full-text search (in-memory TF-IDF fallback — LiteDB has no native FTS) ──

    public bool SupportsFullText => true;

    public Task<IReadOnlyList<FullTextResult<T>>> FullTextSearch<T>(
        string searchText,
        int maxResults = 50,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var mapping = this.options.ResolveFullTextMapping(typeof(T))
            ?? throw new InvalidOperationException(
                $"No full-text mapping is registered for '{typeof(T).Name}'. " +
                $"Call MapFullTextProperty<{typeof(T).Name}>(...) at startup.");
        ArgumentException.ThrowIfNullOrEmpty(searchText);
        if (maxResults <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxResults), "maxResults must be > 0.");

        var typeInfo = this.FindTypeInfo<T>(null);
        IEnumerable<T> items = this.LoadDocuments(this.ResolveTypeName<T>(), typeInfo);
        if (filter != null)
        {
            var compiled = ExpressionInterpreter.Interpret(filter);
            items = items.Where(compiled);
        }

        var docs = items
            .Select(d => (Document: d, Text: FullTextMappingFactory.ExtractText(mapping, d)))
            .ToList();
        var results = InMemoryFullTextSearch.Rank(docs, searchText, maxResults);
        return Task.FromResult(results);
    }

    internal int DeleteDocuments<T>(string typeName, Func<T, bool> predicate, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var collection = this.GetCollection<T>();
        var docs = collection.Find(LiteDB.Query.EQ("TypeName", typeName));
        var idsToDelete = new List<BsonValue>();

        foreach (var doc in docs)
        {
            var json = doc["Data"].AsString;
            var obj = Deserialize(json, typeInfo, this.jsonOptions);
            if (obj != null && predicate(obj))
                idsToDelete.Add(doc["_id"]);
        }

        foreach (var id in idsToDelete)
            collection.Delete(id);

        return idsToDelete.Count;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    internal int UpdateDocumentProperty<T>(
        string typeName,
        Func<T, bool> predicate,
        string jsonPath,
        object? value,
        JsonTypeInfo<T>? typeInfo) where T : class
    {
        var collection = this.GetCollection<T>();
        var docs = collection.Find(LiteDB.Query.EQ("TypeName", typeName));
        var count = 0;

        foreach (var doc in docs)
        {
            var json = doc["Data"].AsString;
            var obj = Deserialize(json, typeInfo, this.jsonOptions);
            if (obj == null || !predicate(obj))
                continue;

            var node = JsonNode.Parse(json)!.AsObject();
            SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(System.Text.Json.JsonSerializer.Serialize(value, this.jsonOptions)));
            doc["Data"] = node.ToJsonString();
            doc["UpdatedAt"] = DateTimeOffset.UtcNow.ToString("o");
            collection.Update(doc);
            count++;
        }

        return count;
    }

    internal string ResolveTypeNameFor<T>() => this.ResolveTypeName<T>();

    internal LiteDbDocumentStoreOptions Options => this.options;

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

    bool PassesFiltersForStored<T>(LiteDB.BsonDocument existing, JsonTypeInfo<T>? typeInfo) where T : class
    {
        if (this.options.ResolveQueryFilters(typeof(T)).Count == 0)
            return true;
        var json = existing["Data"].AsString;
        var doc = Deserialize(json, typeInfo, this.jsonOptions);
        return doc != null && this.PassesGlobalFilters(doc);
    }

    internal JsonSerializerOptions JsonOptions => this.jsonOptions;
    internal InterceptorPipeline InterceptorPipeline => this.options.Interceptors;

    internal IdAccessorCache IdCache => this.idCache;

    // ── Private helpers ────────────────────────────────────────────────

    static void SetNestedProperty(JsonObject node, string path, JsonNode? value)
    {
        var parts = path.Split('.');
        var current = node;
        for (var i = 0; i < parts.Length - 1; i++)
        {
            if (current[parts[i]] is not JsonObject child)
            {
                child = new JsonObject();
                current[parts[i]] = child;
            }
            current = child;
        }
        current[parts[^1]] = value;
    }

    static void RemoveNestedProperty(JsonObject node, string path)
    {
        var parts = path.Split('.');
        var current = node;
        for (var i = 0; i < parts.Length - 1; i++)
        {
            if (current[parts[i]] is not JsonObject child)
                return;
            current = child;
        }
        current.Remove(parts[^1]);
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
            {
                original[prop.Key] = JsonNode.Parse(MergeJson(origObj.ToJsonString(), patchObj.ToJsonString()));
            }
            else
            {
                original[prop.Key] = prop.Value?.DeepClone();
            }
        }

        return original.ToJsonString();
    }

    // ── Transactional wrapper ──────────────────────────────────────────

    sealed class LiteDbTransactionalStore(LiteDbDocumentStore owner) : IDocumentStore
    {
        public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
            => owner.Query(jsonTypeInfo);

        public Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
            => owner.Insert(document, jsonTypeInfo, cancellationToken);

        public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            // Already inside the unit's open transaction — insert per-document via the owner so
            // interceptors, history, duplicate handling, and change buffering all run. (Per-doc is
            // individually compensatable and keeps semantics identical to single Insert.)
            var count = 0;
            foreach (var document in documents)
            {
                await owner.Insert(document, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
                count++;
            }
            return count;
        }

        public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
            => owner.Update(document, jsonTypeInfo, cancellationToken);

        public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
            => owner.Upsert(patch, jsonTypeInfo, cancellationToken);

        public Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
            => owner.SetProperty(id, property, value, jsonTypeInfo, cancellationToken);

        public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
            => owner.RemoveProperty(id, property, jsonTypeInfo, cancellationToken);

        public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
            => owner.Get(id, jsonTypeInfo, cancellationToken);

        public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
            => owner.GetDiff(id, modified, jsonTypeInfo, cancellationToken);

        public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
            => owner.Query<T>(whereClause, jsonTypeInfo, parameters, cancellationToken);

        public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
            => owner.QueryStream<T>(whereClause, jsonTypeInfo, parameters, cancellationToken);

        public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
            => owner.Count<T>(whereClause, parameters, cancellationToken);

        public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
            => owner.Remove<T>(id, cancellationToken);

        public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
            => owner.Clear<T>(cancellationToken);

        public UnitOfWork CreateUnitOfWork()
            => throw new InvalidOperationException("Nested units of work are not supported.");

    }
}
