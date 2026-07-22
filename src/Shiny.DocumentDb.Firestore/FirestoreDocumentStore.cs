using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Google.Api.Gax;
using Google.Cloud.Firestore;
using Grpc.Core;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Firestore;

/// <summary>
/// Google Firestore implementation of <see cref="IDocumentStore"/>. Each document type maps to its own
/// Firestore collection and the document body is stored as a native Firestore map, so fields auto-index and
/// <c>Where</c>/<c>OrderBy</c> push down. Optimistic concurrency uses a Firestore transaction guarded on the
/// mapped version field; change notifications ride native snapshot listeners.
/// </summary>
public partial class FirestoreDocumentStore : DocumentProviderBase, IDocumentStore, IDocumentMaintenance, IObservableDocumentStore, IChangeFeedDocumentStore, IUnitOfWorkEngine, IDisposable
{
    const int BatchWriteChunkSize = 400; // Firestore caps a WriteBatch at 500 operations; stay under.

    readonly FirestoreDocumentStoreOptions options;
    readonly FirestoreDb db;
    readonly JsonSerializerOptions jsonOptions;
    readonly IdAccessorCache idCache;
    Action<string>? logging;

    /// <summary>Constructs the store and wires DI-registered interceptors from <paramref name="serviceProvider"/>.</summary>
    public FirestoreDocumentStore(FirestoreDocumentStoreOptions options, IServiceProvider serviceProvider) : this(options)
    {
        this.AttachServiceProvider(serviceProvider);
        this.logging = DocumentStoreLogging.Compose(this.logging, this.Logger);
    }

    public FirestoreDocumentStore(FirestoreDocumentStoreOptions options)
    {
        this.options = options;
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        this.logging = options.Logging;
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName, options.IdConverters);

        this.db = options.FirestoreDb ?? BuildDb(options);
        options.ResolveVersionJsonPaths(this.jsonOptions);
    }

    static FirestoreDb BuildDb(FirestoreDocumentStoreOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ProjectId))
            throw new InvalidOperationException(
                "FirestoreDocumentStoreOptions requires either a pre-built FirestoreDb or a ProjectId.");

        return new FirestoreDbBuilder
        {
            ProjectId = options.ProjectId,
            DatabaseId = options.DatabaseId,
            EmulatorDetection = options.EmulatorDetection
        }.Build();
    }

    void Log(string message) => this.logging?.Invoke(message);

    protected override InterceptorPipeline Interceptors => this.options.Interceptors;
    internal InterceptorPipeline InterceptorPipeline => this.options.Interceptors;
    internal FirestoreDocumentStoreOptions Options => this.options;
    internal JsonSerializerOptions JsonOptions => this.jsonOptions;
    internal FirestoreDb Database => this.db;

    /// <summary>
    /// No store-owned unmanaged resources to release — <see cref="FirestoreDb"/> owns and pools its
    /// own gRPC channels. Implemented so callers can deterministically dispose the store like the
    /// other providers; per-query change-feed listeners are disposed via their own subscription handles.
    /// </summary>
    public void Dispose() => GC.SuppressFinalize(this);

    string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);
    internal string ResolveTypeNameFor<T>() => this.ResolveTypeName<T>();
    string ResolveCollectionName<T>() => this.options.ResolveCollectionName(typeof(T), this.ResolveTypeName<T>());
    internal string ResolveCollectionNameFor<T>() => this.ResolveCollectionName<T>();

    internal CollectionReference GetCollection<T>() => this.db.Collection(this.ResolveCollectionName<T>());

    // ── Serialization ────────────────────────────────────────────────────

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
    static string Serialize<T>(T value, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? JsonSerializer.Serialize(value, typeInfo) : JsonSerializer.Serialize(value, options);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static T? Deserialize<T>(string json, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? JsonSerializer.Deserialize(json, typeInfo) : JsonSerializer.Deserialize<T>(json, options);

    internal T? DeserializeSnapshot<T>(DocumentSnapshot snapshot, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var doc = Deserialize<T>(FirestoreDocument.MapToJson(snapshot.ToDictionary()), typeInfo, this.jsonOptions);
        if (doc != null)
            this.AttachBlobLoaders(doc);
        return doc;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static string ResolvePropertyPath<T>(Expression<Func<T, object>> property, JsonSerializerOptions options, JsonTypeInfo<T>? typeInfo)
        => typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, options, typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, options);

    string GenerateId<T>(IdAccessor<T> accessor) where T : class
        => accessor.Kind switch
        {
            IdKind.Guid => Guid.NewGuid().ToString("N"),
            IdKind.String => Guid.NewGuid().ToString(),
            IdKind.Custom => accessor.GenerateOrThrow(),
            IdKind.Int or IdKind.Long => throw new NotSupportedException(
                $"Firestore cannot auto-generate Int/Long Ids for '{typeof(T).Name}' (no cheap monotonic counter). " +
                "Use a Guid or string Id, or assign the Int/Long Id explicitly before Insert."),
            _ => throw new InvalidOperationException($"Unsupported Id kind: {accessor.Kind}")
        };

    // ── Global query filters ─────────────────────────────────────────────

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

    bool StoredPassesFilters<T>(DocumentSnapshot snapshot, JsonTypeInfo<T>? typeInfo) where T : class
    {
        if (this.options.ResolveQueryFilters(typeof(T)).Count == 0)
            return true;
        var doc = this.DeserializeSnapshot(snapshot, typeInfo);
        return doc != null && this.PassesGlobalFilters(doc);
    }

    static int ReadVersion(DocumentSnapshot snapshot, string jsonPath)
    {
        var map = snapshot.ToDictionary();
        var json = FirestoreDocument.MapToJson(map);
        var node = JsonNode.Parse(json);
        foreach (var part in jsonPath.Split('.'))
        {
            if (node is not JsonObject obj || !obj.TryGetPropertyValue(part, out node))
                return 0;
        }
        return node is JsonValue v && v.TryGetValue(out int i) ? i : 0;
    }

    // ── IDocumentStore ──────────────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        return new FirestoreDocumentQuery<T>(this, typeInfo);
    }

    public Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("insert", typeof(T).Name, () => this.InsertImpl(document, jsonTypeInfo, cancellationToken));

    async Task InsertImpl<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
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
            id = this.GenerateId(accessor);
            accessor.SetId(document, id);
        }
        else
        {
            id = accessor.GetIdAsString(document);
        }

        versionMapping?.SetVersion(document, 1);
        var now = DateTime.UtcNow;
        var preparedBlobs = this.PrepareBlobs(document);
        var map = FirestoreDocument.BuildMap(Serialize(document, typeInfo, this.jsonOptions), typeName, now, now);

        this.Log($"Firestore CREATE {this.ResolveCollectionName<T>()}/{id}");
        await this.SyncBlobsAsync<T>(id, typeName, preparedBlobs, prune: false, cancellationToken).ConfigureAwait(false);
        try
        {
            await this.GetCollection<T>().Document(id).CreateAsync(map, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
        {
            throw new InvalidOperationException($"A document of type '{typeName}' with Id '{id}' already exists.", ex);
        }
        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document) ?? 1, cancellationToken).ConfigureAwait(false);
    }

    public Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("batch_insert", typeof(T).Name, () => this.BatchInsertImpl(documents, jsonTypeInfo, cancellationToken), r => r);

    async Task<int> BatchInsertImpl<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));

        var srcList = documents as IReadOnlyList<T> ?? documents.ToList();

        DocumentWriteContext[]? ctxs = null;
        if (this.HasPerDocInterceptors)
        {
            var mutable = srcList.ToList();
            ctxs = await this.RunBeforeWriteBatchAsync(mutable, typeName, cancellationToken).ConfigureAwait(false);
            srcList = mutable;
        }

        var collection = this.GetCollection<T>();
        var pending = new List<(string Id, Dictionary<string, object?> Map)>(srcList.Count);
        foreach (var document in srcList)
        {
            string id;
            if (accessor.IsDefaultId(document))
            {
                if (accessor.Kind == IdKind.String)
                    throw new InvalidOperationException($"Insert requires a non-empty string Id on '{typeof(T).Name}'.");
                id = this.GenerateId(accessor);
                accessor.SetId(document, id);
            }
            else
            {
                id = accessor.GetIdAsString(document);
            }

            versionMapping?.SetVersion(document, 1);
            var now = DateTime.UtcNow;
            pending.Add((id, FirestoreDocument.BuildMap(Serialize(document, typeInfo, this.jsonOptions), typeName, now, now)));
        }

        if (pending.Count == 0)
            return 0;

        this.Log($"Firestore BATCH CREATE {pending.Count} into {this.ResolveCollectionName<T>()}");
        foreach (var chunk in pending.Chunk(BatchWriteChunkSize))
        {
            var batch = this.db.StartBatch();
            foreach (var (id, map) in chunk)
                batch.Create(collection.Document(id), map);
            try
            {
                await batch.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
            {
                throw new InvalidOperationException($"A document of type '{typeName}' has a duplicate Id in the batch.", ex);
            }
        }

        for (var i = 0; i < srcList.Count; i++)
        {
            if (ctxs != null)
                await this.RunAfterWriteAsync(ctxs[i], accessor.GetIdAsString(srcList[i]), versionMapping?.GetVersion(srcList[i]) ?? 1, cancellationToken).ConfigureAwait(false);
        }
        return pending.Count;
    }

    public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("update", typeof(T).Name, () => this.UpdateImpl(document, jsonTypeInfo, cancellationToken));

    async Task UpdateImpl<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
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
        var docRef = this.GetCollection<T>().Document(id);
        this.Log($"Firestore UPDATE {this.ResolveCollectionName<T>()}/{id}");
        await this.SyncBlobsAsync<T>(id, typeName, this.PrepareBlobs(document), prune: true, cancellationToken).ConfigureAwait(false);

        if (versionMapping != null)
        {
            var expectedVersion = versionMapping.GetVersion(document);
            await this.db.RunTransactionAsync(async tx =>
            {
                var snap = await tx.GetSnapshotAsync(docRef, cancellationToken).ConfigureAwait(false);
                if (!snap.Exists || !this.StoredPassesFilters(snap, typeInfo))
                    throw new InvalidOperationException($"No document of type '{typeName}' with Id '{id}' was found to update.");

                var storedVersion = ReadVersion(snap, versionMapping.JsonPath);
                if (storedVersion != expectedVersion)
                    throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);

                versionMapping.SetVersion(document, expectedVersion + 1);
                var now = DateTime.UtcNow;
                var createdAt = FirestoreDocument.ReadTimestamps(snap.ToDictionary()).CreatedAt?.UtcDateTime ?? now;
                var map = FirestoreDocument.BuildMap(Serialize(document, typeInfo, this.jsonOptions), typeName, createdAt, now);
                tx.Set(docRef, map, SetOptions.Overwrite);
            }, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var snap = await docRef.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
            if (!snap.Exists || !this.StoredPassesFilters(snap, typeInfo))
                throw new InvalidOperationException($"No document of type '{typeName}' with Id '{id}' was found to update.");

            var now = DateTime.UtcNow;
            var createdAt = FirestoreDocument.ReadTimestamps(snap.ToDictionary()).CreatedAt?.UtcDateTime ?? now;
            var map = FirestoreDocument.BuildMap(Serialize(document, typeInfo, this.jsonOptions), typeName, createdAt, now);
            await docRef.SetAsync(map, SetOptions.Overwrite, cancellationToken).ConfigureAwait(false);
        }

        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document), cancellationToken).ConfigureAwait(false);
    }

    public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("upsert", typeof(T).Name, () => this.UpsertImpl(patch, jsonTypeInfo, cancellationToken));

    async Task UpsertImpl<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
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
        var docRef = this.GetCollection<T>().Document(id);
        var snap = await docRef.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        var now = DateTime.UtcNow;
        await this.SyncBlobsAsync<T>(id, typeName, this.PrepareBlobs(patch), prune: false, cancellationToken).ConfigureAwait(false);

        if (!snap.Exists)
        {
            versionMapping?.SetVersion(patch, 1);
            var patchJson = StripNullProperties(Serialize(patch, typeInfo, this.jsonOptions));
            var map = FirestoreDocument.BuildMap(patchJson, typeName, now, now);
            this.Log($"Firestore UPSERT (insert) {this.ResolveCollectionName<T>()}/{id}");
            await docRef.SetAsync(map, SetOptions.Overwrite, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            if (versionMapping != null)
            {
                var expectedVersion = versionMapping.GetVersion(patch);
                var storedVersion = ReadVersion(snap, versionMapping.JsonPath);
                if (expectedVersion > 0 && storedVersion != expectedVersion)
                    throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
                versionMapping.SetVersion(patch, storedVersion + 1);
            }

            var patchJson = StripNullProperties(Serialize(patch, typeInfo, this.jsonOptions));
            var createdAt = FirestoreDocument.ReadTimestamps(snap.ToDictionary()).CreatedAt?.UtcDateTime ?? now;
            var merged = MergeJson(FirestoreDocument.MapToJson(snap.ToDictionary()), patchJson);
            var map = FirestoreDocument.BuildMap(merged, typeName, createdAt, now);
            this.Log($"Firestore UPSERT (merge) {this.ResolveCollectionName<T>()}/{id}");
            await docRef.SetAsync(map, SetOptions.Overwrite, cancellationToken).ConfigureAwait(false);
        }

        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
    }

    public Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("set_property", typeof(T).Name, () => this.SetPropertyImpl(id, property, value, jsonTypeInfo, cancellationToken), r => r ? 1 : 0);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    async Task<bool> SetPropertyImpl<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var docRef = this.GetCollection<T>().Document(resolvedId);

        var snap = await docRef.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        if (!snap.Exists || !this.StoredPassesFilters(snap, typeInfo))
            return false;

        var node = JsonNode.Parse(FirestoreDocument.MapToJson(snap.ToDictionary()))!.AsObject();
        SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));
        var createdAt = FirestoreDocument.ReadTimestamps(snap.ToDictionary()).CreatedAt?.UtcDateTime ?? DateTime.UtcNow;
        var map = FirestoreDocument.BuildMap(node.ToJsonString(), typeName, createdAt, DateTime.UtcNow);

        this.Log($"Firestore SET PROPERTY {this.ResolveCollectionName<T>()}/{resolvedId} Path={jsonPath}");
        await docRef.SetAsync(map, SetOptions.Overwrite, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("remove_property", typeof(T).Name, () => this.RemovePropertyImpl(id, property, jsonTypeInfo, cancellationToken), r => r ? 1 : 0);

    async Task<bool> RemovePropertyImpl<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var docRef = this.GetCollection<T>().Document(resolvedId);

        var snap = await docRef.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        if (!snap.Exists || !this.StoredPassesFilters(snap, typeInfo))
            return false;

        var node = JsonNode.Parse(FirestoreDocument.MapToJson(snap.ToDictionary()))!.AsObject();
        RemoveNestedProperty(node, jsonPath);
        var createdAt = FirestoreDocument.ReadTimestamps(snap.ToDictionary()).CreatedAt?.UtcDateTime ?? DateTime.UtcNow;
        var map = FirestoreDocument.BuildMap(node.ToJsonString(), typeName, createdAt, DateTime.UtcNow);

        this.Log($"Firestore REMOVE PROPERTY {this.ResolveCollectionName<T>()}/{resolvedId} Path={jsonPath}");
        await docRef.SetAsync(map, SetOptions.Overwrite, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("get", typeof(T).Name, () => this.GetImpl(id, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    async Task<T?> GetImpl<T>(object id, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var docRef = this.GetCollection<T>().Document(resolvedId);

        this.Log($"Firestore GET {this.ResolveCollectionName<T>()}/{resolvedId}");
        var snap = await docRef.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        if (!snap.Exists)
            return null;

        var doc = this.DeserializeSnapshot(snap, typeInfo);
        if (doc != null && !this.PassesGlobalFilters(doc))
            return null;
        return doc;
    }

    public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("get_diff", typeof(T).Name, () => this.GetDiffImpl(id, modified, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    async Task<JsonPatchDocument<T>?> GetDiffImpl<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var docRef = this.GetCollection<T>().Document(resolvedId);

        var snap = await docRef.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        if (!snap.Exists || !this.StoredPassesFilters(snap, typeInfo))
            return null;

        var originalJson = FirestoreDocument.MapToJson(snap.ToDictionary());
        var modifiedJson = Serialize(modified, typeInfo, this.jsonOptions);
        return JsonDiff.CreatePatch<T>(originalJson, modifiedJson, this.jsonOptions);
    }

    public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Firestore has no server-side query language. Use the LINQ-based Query<T>() overload instead.");

    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Firestore has no server-side query language. Use the LINQ-based Query<T>() overload instead.");

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("count", typeof(T).Name, () => this.CountImpl<T>(whereClause, parameters, cancellationToken), r => r);

    async Task<int> CountImpl<T>(string? whereClause, object? parameters, CancellationToken cancellationToken) where T : class
    {
        if (!string.IsNullOrWhiteSpace(whereClause))
            throw new NotSupportedException("Firestore has no server-side query language. Use the LINQ-based Query<T>() overload instead.");

        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;
        this.Log($"Firestore COUNT {this.ResolveCollectionName<T>()}");

        if (!hasFilters)
        {
            var snapshot = await this.GetCollection<T>().Count().GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
            return (int)(snapshot.Count ?? 0);
        }

        var typeInfo = this.FindTypeInfo<T>(null);
        var matched = 0;
        await foreach (var doc in this.LoadDocumentsAsync(typeInfo, null, cancellationToken).ConfigureAwait(false))
            if (this.PassesGlobalFilters(doc))
                matched++;
        return matched;
    }

    public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("remove", typeof(T).Name, () => this.RemoveImpl<T>(id, cancellationToken), r => r ? 1 : 0);

    async Task<bool> RemoveImpl<T>(object id, CancellationToken cancellationToken) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var docRef = this.GetCollection<T>().Document(resolvedId);

        var ctx = this.NewWriteContext<T>(DocumentOperation.Delete, typeName, id, null);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);

        var snap = await docRef.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        if (!snap.Exists || !this.StoredPassesFilters<T>(snap, null))
            return false;

        this.Log($"Firestore DELETE {this.ResolveCollectionName<T>()}/{resolvedId}");
        await docRef.DeleteAsync(Precondition.None, cancellationToken).ConfigureAwait(false);
        await this.DeleteBlobsAsync<T>(resolvedId, cancellationToken).ConfigureAwait(false);
        await this.RunAfterWriteAsync(ctx, id, null, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("clear", typeof(T).Name, () => this.ClearImpl<T>(cancellationToken), r => r);

    async Task<int> ClearImpl<T>(CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var collection = this.GetCollection<T>();
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Clear, typeName);
        await this.RunBeforeBulkAsync(bulkCtx, cancellationToken).ConfigureAwait(false);

        this.Log($"Firestore CLEAR {this.ResolveCollectionName<T>()}");
        var snapshot = await ((Query)collection).GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        var toDelete = new List<DocumentReference>();
        foreach (var doc in snapshot.Documents)
        {
            if (hasFilters)
            {
                var model = this.DeserializeSnapshot(doc, typeInfo);
                if (model == null || !this.PassesGlobalFilters(model))
                    continue;
            }
            toDelete.Add(doc.Reference);
        }

        var deleted = await this.DeleteRefsAsync(toDelete, cancellationToken).ConfigureAwait(false);
        await this.RunAfterBulkAsync(bulkCtx, deleted, cancellationToken).ConfigureAwait(false);
        return deleted;
    }

    async Task<int> DeleteRefsAsync(IReadOnlyList<DocumentReference> refs, CancellationToken cancellationToken)
    {
        var deleted = 0;
        foreach (var chunk in refs.Chunk(BatchWriteChunkSize))
        {
            var batch = this.db.StartBatch();
            foreach (var reference in chunk)
                batch.Delete(reference);
            await batch.CommitAsync(cancellationToken).ConfigureAwait(false);
            deleted += chunk.Length;
        }
        return deleted;
    }

    /// <inheritdoc />
    public async Task ClearAll(CancellationToken cancellationToken = default)
    {
        this.Log("Firestore CLEAR ALL");
        var collections = this.db.ListRootCollectionsAsync();
        await foreach (var collection in collections.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            var snapshot = await ((Query)collection).GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
            var refs = snapshot.Documents.Select(d => d.Reference).ToList();
            await this.DeleteRefsAsync(refs, cancellationToken).ConfigureAwait(false);
        }
    }

    // ── Unit of work (compensating, matching the MongoDB/Cosmos single-node model) ─────

    Task IUnitOfWorkEngine.RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
        => this.Tracker.Track("transaction", "(transaction)", () => this.RunUnitAsyncImpl(work, cancellationToken));

    async Task RunUnitAsyncImpl(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        // Firestore transactions require all reads before any writes, which the mixed insert/update/remove
        // replay of a unit cannot satisfy. We use the compensating pattern (as MongoDB/Cosmos do): inserts are
        // tracked and deleted on failure; update/upsert/remove are applied against the live store.
        var tx = new FirestoreCompensatingStore(this);
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

    // ── Internal helpers used by FirestoreDocumentQuery ─────────────────

    internal async IAsyncEnumerable<T> LoadDocumentsAsync<T>(
        JsonTypeInfo<T>? typeInfo,
        IReadOnlyList<FirestoreClause>? pushdown,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class
    {
        var snapshot = await this.RunQueryAsync<T>(pushdown, cancellationToken).ConfigureAwait(false);
        foreach (var doc in snapshot.Documents)
        {
            var model = this.DeserializeSnapshot(doc, typeInfo);
            if (model != null)
                yield return model;
        }
    }

    // Applies the pushable clauses server-side; if the resulting query needs a composite index that isn't
    // provisioned (FAILED_PRECONDITION), retries as a full collection scan so results are still correct.
    async Task<QuerySnapshot> RunQueryAsync<T>(IReadOnlyList<FirestoreClause>? pushdown, CancellationToken cancellationToken) where T : class
    {
        Query query = this.GetCollection<T>();
        if (pushdown is { Count: > 0 })
        {
            foreach (var clause in pushdown)
                query = Apply(query, clause);
        }

        try
        {
            return await query.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.FailedPrecondition or StatusCode.InvalidArgument && pushdown is { Count: > 0 })
        {
            this.Log($"Firestore pushdown fell back to full scan ({ex.StatusCode}): {ex.Status.Detail}");
            return await ((Query)this.GetCollection<T>()).GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    static Query Apply(Query query, FirestoreClause clause) => clause.Op switch
    {
        FirestoreOp.Equal => query.WhereEqualTo(clause.Path, clause.Value),
        FirestoreOp.GreaterThan => query.WhereGreaterThan(clause.Path, clause.Value),
        FirestoreOp.GreaterThanOrEqual => query.WhereGreaterThanOrEqualTo(clause.Path, clause.Value),
        FirestoreOp.LessThan => query.WhereLessThan(clause.Path, clause.Value),
        FirestoreOp.LessThanOrEqual => query.WhereLessThanOrEqualTo(clause.Path, clause.Value),
        _ => query
    };

    internal IReadOnlyList<FirestoreClause> BuildPushdown<T>(IEnumerable<Expression<Func<T, bool>>> predicates, JsonTypeInfo<T>? typeInfo) where T : class
        => FirestoreExpressionVisitor.Extract(predicates, this.jsonOptions, typeInfo);

    internal async Task<int> DeleteWhereAsync<T>(Func<T, bool> predicate, IReadOnlyList<FirestoreClause>? pushdown, JsonTypeInfo<T>? typeInfo, CancellationToken cancellationToken) where T : class
    {
        var snapshot = await this.RunQueryAsync<T>(pushdown, cancellationToken).ConfigureAwait(false);
        var toDelete = new List<DocumentReference>();
        foreach (var doc in snapshot.Documents)
        {
            var model = this.DeserializeSnapshot(doc, typeInfo);
            if (model != null && predicate(model))
                toDelete.Add(doc.Reference);
        }
        return await this.DeleteRefsAsync(toDelete, cancellationToken).ConfigureAwait(false);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    internal async Task<int> UpdatePropertyWhereAsync<T>(Func<T, bool> predicate, string jsonPath, object? value, IReadOnlyList<FirestoreClause>? pushdown, JsonTypeInfo<T>? typeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var snapshot = await this.RunQueryAsync<T>(pushdown, cancellationToken).ConfigureAwait(false);
        var matched = new List<DocumentSnapshot>();
        foreach (var doc in snapshot.Documents)
        {
            var model = this.DeserializeSnapshot(doc, typeInfo);
            if (model != null && predicate(model))
                matched.Add(doc);
        }

        foreach (var doc in matched)
        {
            var node = JsonNode.Parse(FirestoreDocument.MapToJson(doc.ToDictionary()))!.AsObject();
            SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));
            var createdAt = FirestoreDocument.ReadTimestamps(doc.ToDictionary()).CreatedAt?.UtcDateTime ?? DateTime.UtcNow;
            var map = FirestoreDocument.BuildMap(node.ToJsonString(), typeName, createdAt, DateTime.UtcNow);
            await doc.Reference.SetAsync(map, SetOptions.Overwrite, cancellationToken).ConfigureAwait(false);
        }
        return matched.Count;
    }

    // ── Private helpers ─────────────────────────────────────────────────

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

    // ── Compensating transaction wrapper ────────────────────────────────

    sealed class FirestoreCompensatingStore(FirestoreDocumentStore inner) : CompensatingStore
    {
        protected override IDocumentStore Inner => inner;

        public override async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default)
        {
            await inner.Insert(document, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            var accessor = inner.idCache.GetOrCreate(inner.FindTypeInfo(jsonTypeInfo));
            this.TrackInsert(inner.ResolveCollectionNameFor<T>(), accessor.GetIdAsString(document));
        }

        protected override async Task DeleteTrackedAsync(string collectionName, string id, CancellationToken ct)
            => await inner.db.Collection(collectionName).Document(id).DeleteAsync(Precondition.None, ct).ConfigureAwait(false);
    }
}
