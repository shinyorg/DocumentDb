using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using NRedisStack;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Literals.Enums;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;
using StackExchange.Redis;

namespace Shiny.DocumentDb.Redis;

/// <summary>
/// Redis Stack implementation of <see cref="IDocumentStore"/> over RedisJSON + RediSearch. Each document is a
/// RedisJSON key <c>doc:{typeName}:{id}</c>; a per-type RediSearch index (built lazily from the declared
/// <c>MapIndexedProperty</c>/<c>MapFullTextProperty</c>/<c>MapVectorProperty</c> fields) drives server-side
/// full-text, vector, and <c>FT.AGGREGATE</c> grouping, while general LINQ predicates evaluate client-side
/// after a candidate load (index pushdown shrinks the candidate set). Requires the RedisJSON and RediSearch
/// modules — plain Redis is not supported.
/// </summary>
public partial class RedisDocumentStore : DocumentProviderBase, IDocumentStore, IDocumentMaintenance, IObservableDocumentStore, IUnitOfWorkEngine, IDisposable
{
    readonly RedisDocumentStoreOptions options;
    readonly IConnectionMultiplexer mux;
    readonly bool ownsMux;
    readonly IDatabase db;
    readonly JsonCommands json;
    readonly SearchCommands ft;
    readonly JsonSerializerOptions jsonOptions;
    readonly IdAccessorCache idCache;
    readonly string keyPrefix;
    Action<string>? logging;

    readonly ChangeBroadcaster broadcaster = new();
    readonly AsyncLocal<List<Action>?> pendingChanges = new();

    readonly SemaphoreSlim initGate = new(1, 1);
    bool modulesChecked;
    readonly ConcurrentDictionary<Type, IReadOnlyList<RedisIndexField>> indexFields = new();
    readonly ConcurrentDictionary<Type, byte> indexEnsured = new();

    /// <summary>Constructs the store and wires DI-registered interceptors from <paramref name="serviceProvider"/>.</summary>
    public RedisDocumentStore(RedisDocumentStoreOptions options, IServiceProvider serviceProvider) : this(options)
    {
        this.AttachServiceProvider(serviceProvider);
        this.logging = DocumentStoreLogging.Compose(this.logging, this.Logger);
    }

    public RedisDocumentStore(RedisDocumentStoreOptions options)
    {
        this.options = options;
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        this.logging = options.Logging;
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName, options.IdConverters);
        this.keyPrefix = string.IsNullOrEmpty(options.KeyNamespace) ? "" : options.KeyNamespace + ":";

        if (options.Multiplexer != null)
        {
            this.mux = options.Multiplexer;
            this.ownsMux = false;
        }
        else
        {
            if (string.IsNullOrWhiteSpace(options.ConnectionString))
                throw new InvalidOperationException("Either Multiplexer or ConnectionString must be set on RedisDocumentStoreOptions.");
            this.mux = ConnectionMultiplexer.Connect(options.ConnectionString);
            this.ownsMux = true;
        }
        this.db = options.Database >= 0 ? this.mux.GetDatabase(options.Database) : this.mux.GetDatabase();
        this.json = this.db.JSON();
        this.ft = this.db.FT();

        options.ResolveVersionJsonPaths(this.jsonOptions);
        options.ResolveVectorJsonPaths(this.jsonOptions);
        options.ResolveFullTextJsonPaths(this.jsonOptions);
        options.ResolveSpatialJsonPaths(this.jsonOptions);
        options.ResolveComputedJsonNames(this.jsonOptions);
    }

    protected override InterceptorPipeline Interceptors => this.options.Interceptors;
    internal RedisDocumentStoreOptions Options => this.options;
    internal JsonSerializerOptions JsonOptions => this.jsonOptions;
    internal IdAccessorCache IdCache => this.idCache;
    internal ChangeBroadcaster Broadcaster => this.broadcaster;

    public bool SupportsVector => this.options.vectorMappings.Count > 0;
    public bool SupportsFullText => this.options.fullTextMappings.Count > 0;
    public bool SupportsSpatial => this.options.spatialMappings.Count > 0;

    public void Dispose()
    {
        this.initGate.Dispose();
        if (this.ownsMux && this.mux is IDisposable d)
            d.Dispose();
    }

    void Log(string message) => this.logging?.Invoke(message);

    string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);
    internal string ResolveTypeNameFor<T>() => this.ResolveTypeName<T>();

    // ── Key builders (namespace-aware) ──────────────────────────────────
    string DocKey(string typeName, string id) => $"{this.keyPrefix}doc:{typeName}:{id}";
    string DocScanPattern(string typeName) => $"{this.keyPrefix}doc:{typeName}:*";
    string DocIndexPrefix(string typeName) => $"{this.keyPrefix}doc:{typeName}:";
    string SeqKey(string typeName) => $"{this.keyPrefix}seq:{typeName}";
    string IndexName(string typeName) => $"{this.keyPrefix}idx:{typeName}";

    // ── Module probe + index management ─────────────────────────────────

    async Task EnsureModulesAsync(CancellationToken ct)
    {
        if (this.modulesChecked)
            return;
        await this.initGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (this.modulesChecked)
                return;

            try
            {
                await this.ft._ListAsync().ConfigureAwait(false);
            }
            catch (RedisServerException ex)
            {
                throw new NotSupportedException(
                    "The RediSearch module is not loaded on this Redis server. Shiny.DocumentDb.Redis requires " +
                    "Redis Stack (RedisJSON + RediSearch), Redis 8+, or Redis Enterprise with the modules. " +
                    "Plain Redis is not supported.", ex);
            }

            try
            {
                await this.db.ExecuteAsync("JSON.TYPE", $"{this.keyPrefix}__docdb_probe__").ConfigureAwait(false);
            }
            catch (RedisServerException ex)
            {
                throw new NotSupportedException(
                    "The RedisJSON module is not loaded on this Redis server. Shiny.DocumentDb.Redis requires " +
                    "Redis Stack (RedisJSON + RediSearch), Redis 8+, or Redis Enterprise with the modules.", ex);
            }
            this.modulesChecked = true;
        }
        finally
        {
            this.initGate.Release();
        }
    }

    internal IReadOnlyList<RedisIndexField> IndexFieldsFor<T>()
        => this.indexFields.GetOrAdd(typeof(T), _ => this.BuildIndexFields<T>());

    IReadOnlyList<RedisIndexField> BuildIndexFields<T>()
    {
        var fields = new List<RedisIndexField>();

        foreach (var spec in this.options.ResolveIndexedSpecs(typeof(T)))
        {
            var jsonPath = string.Join('.', spec.ClrSegments.Select(s => this.jsonOptions.PropertyNamingPolicy?.ConvertName(s) ?? s));
            fields.Add(new RedisIndexField
            {
                ClrPath = string.Join('.', spec.ClrSegments),
                JsonPath = jsonPath,
                Kind = spec.IsNumeric ? RedisFieldKind.Numeric : RedisFieldKind.Tag
            });
        }

        var ft = this.options.ResolveFullTextMapping(typeof(T));
        if (ft != null)
        {
            foreach (var path in ft.JsonPaths)
                fields.Add(new RedisIndexField { ClrPath = path, JsonPath = path, Kind = RedisFieldKind.Text });
        }

        var vector = this.options.ResolveVectorMapping(typeof(T));
        if (vector != null)
            fields.Add(new RedisIndexField { ClrPath = vector.PropertyName, JsonPath = vector.JsonPath, Kind = RedisFieldKind.Vector });

        return fields;
    }

    /// <summary>Creates the per-type RediSearch index (lazy, cached). Returns true when an index exists for the type.</summary>
    async Task<bool> EnsureIndexAsync<T>(CancellationToken ct)
    {
        await this.EnsureModulesAsync(ct).ConfigureAwait(false);
        var fields = this.IndexFieldsFor<T>();
        if (fields.Count == 0)
            return false;

        if (this.indexEnsured.ContainsKey(typeof(T)))
            return true;

        var typeName = this.ResolveTypeName<T>();
        var indexName = this.IndexName(typeName);
        var schema = new Schema();
        foreach (var f in fields)
        {
            var name = new FieldName(f.SchemaPath, f.Alias);
            switch (f.Kind)
            {
                case RedisFieldKind.Numeric:
                    schema.AddNumericField(name, sortable: true);
                    break;
                case RedisFieldKind.Text:
                    schema.AddTextField(name, sortable: true);
                    break;
                case RedisFieldKind.Vector:
                    var vm = this.options.ResolveVectorMapping(typeof(T))!;
                    schema.AddHnswVectorField(name, Schema.VectorField.VectorType.FLOAT32, vm.Dimensions, MapMetric(vm.Metric));
                    break;
                default:
                    schema.AddTagField(name, sortable: true);
                    break;
            }
        }

        var createParams = new FTCreateParams()
            .On(IndexDataType.JSON)
            .Prefix(this.DocIndexPrefix(typeName));

        this.Log($"Redis FT.CREATE {indexName}");
        try
        {
            await this.ft.CreateAsync(indexName, createParams, schema).ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase))
        {
            // Idempotent — the index survives across store instances sharing the same Redis.
        }
        this.indexEnsured.TryAdd(typeof(T), 0);
        return true;
    }

    static Schema.VectorField.VectorDistanceMetric MapMetric(VectorDistance metric) => metric switch
    {
        VectorDistance.Cosine => Schema.VectorField.VectorDistanceMetric.CosineDistance,
        VectorDistance.Euclidean => Schema.VectorField.VectorDistanceMetric.EuclideanDistance,
        VectorDistance.DotProduct => Schema.VectorField.VectorDistanceMetric.InnerProduct,
        _ => throw new NotSupportedException($"Vector metric {metric} is not supported by the Redis provider.")
    };

    // ── JSON envelope IO (raw commands, deterministic) ──────────────────

    Task SetJsonAsync(string key, string envelopeJson)
        => this.db.ExecuteAsync("JSON.SET", key, "$", envelopeJson);

    /// <summary>Atomic insert: JSON.SET with NX only writes when the key does not already exist,
    /// so concurrent inserts of the same id race safely (one wins). Returns false if the key existed.</summary>
    async Task<bool> SetJsonIfNotExistsAsync(string key, string envelopeJson)
    {
        var result = await this.db.ExecuteAsync("JSON.SET", key, "$", envelopeJson, "NX").ConfigureAwait(false);
        return !result.IsNull;
    }

    async Task<JsonObject?> GetEnvelopeAsync(string key)
    {
        var result = await this.db.ExecuteAsync("JSON.GET", key).ConfigureAwait(false);
        if (result.IsNull)
            return null;
        return RedisDocument.ParseEnvelope((string?)result);
    }

    async Task<bool> DeleteKeyAsync(string key)
    {
        var result = await this.db.ExecuteAsync("JSON.DEL", key).ConfigureAwait(false);
        return (long)result > 0;
    }

    // ── Serialization ───────────────────────────────────────────────────

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
    internal static T? Deserialize<T>(string json, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? JsonSerializer.Deserialize(json, typeInfo) : JsonSerializer.Deserialize<T>(json, options);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static string ResolvePropertyPath<T>(Expression<Func<T, object>> property, JsonSerializerOptions options, JsonTypeInfo<T>? typeInfo)
        => typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, options, typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, options);

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

    // ── Ids ─────────────────────────────────────────────────────────────

    // Int/Long ids auto-generate via a per-type INCR counter — a genuine differentiator (every other NoSQL
    // provider throws here). The value is monotonic per store, not globally meaningful.
    async Task<string> GenerateIdAsync<T>(IdAccessor<T> accessor, string typeName) where T : class
    {
        switch (accessor.Kind)
        {
            case IdKind.Guid:
                return Guid.NewGuid().ToString("N");
            case IdKind.String:
                return Guid.NewGuid().ToString();
            case IdKind.Int:
            case IdKind.Long:
                var next = await this.db.StringIncrementAsync(this.SeqKey(typeName)).ConfigureAwait(false);
                return next.ToString(CultureInfo.InvariantCulture);
            case IdKind.Custom:
                return accessor.GenerateOrThrow();
            default:
                throw new InvalidOperationException($"Unsupported Id kind: {accessor.Kind}");
        }
    }

    // Keep the per-type counter ahead of an explicitly-assigned numeric id so a later auto-gen never collides.
    async Task BumpSeqIfNeededAsync<T>(IdAccessor<T> accessor, string typeName, string id) where T : class
    {
        if (accessor.Kind is IdKind.Int or IdKind.Long && long.TryParse(id, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n))
        {
            var seqKey = this.SeqKey(typeName);
            var cur = (long?)await this.db.StringGetAsync(seqKey).ConfigureAwait(false) ?? 0;
            if (n > cur)
                await this.db.StringSetAsync(seqKey, n).ConfigureAwait(false);
        }
    }

    // ── IDocumentStore: query entry ─────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        return new RedisDocumentQuery<T>(this, typeInfo);
    }

    // ── Insert ──────────────────────────────────────────────────────────

    public Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("insert", typeof(T).Name, () => this.InsertImpl(document, jsonTypeInfo, cancellationToken));

    async Task InsertImpl<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        await this.EnsureIndexAsync<T>(cancellationToken).ConfigureAwait(false);

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
            id = await this.GenerateIdAsync(accessor, typeName).ConfigureAwait(false);
            accessor.SetId(document, id);
        }
        else
        {
            id = accessor.GetIdAsString(document);
            await this.BumpSeqIfNeededAsync(accessor, typeName, id).ConfigureAwait(false);
        }

        versionMapping?.SetVersion(document, 1);
        var json = Serialize(document, typeInfo, this.jsonOptions);
        var key = this.DocKey(typeName, id);

        var envelope = RedisDocument.BuildEnvelope(id, typeName, json, DateTime.UtcNow, versionMapping != null ? 1 : null);
        this.Log($"Redis JSON.SET {key} NX");
        var inserted = await this.SetJsonIfNotExistsAsync(key, envelope).ConfigureAwait(false);
        if (!inserted)
            throw new InvalidOperationException($"A document of type '{typeName}' with Id '{id}' already exists.");

        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document) ?? 1, cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Inserted, id, document);
    }

    public Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("batch_insert", typeof(T).Name, () => this.BatchInsertImpl(documents, jsonTypeInfo, cancellationToken), r => r);

    async Task<int> BatchInsertImpl<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        await this.EnsureIndexAsync<T>(cancellationToken).ConfigureAwait(false);

        var srcList = documents as IReadOnlyList<T> ?? documents.ToList();

        DocumentWriteContext[]? ctxs = null;
        if (this.HasPerDocInterceptors)
        {
            var mutable = srcList.ToList();
            ctxs = await this.RunBeforeWriteBatchAsync(mutable, typeName, cancellationToken).ConfigureAwait(false);
            srcList = mutable;
        }

        var writes = new List<(string Key, string Envelope, string Id, T Doc)>(srcList.Count);
        var now = DateTime.UtcNow;
        foreach (var document in srcList)
        {
            string id;
            if (accessor.IsDefaultId(document))
            {
                if (accessor.Kind == IdKind.String)
                    throw new InvalidOperationException($"Insert requires a non-empty string Id on '{typeof(T).Name}'.");
                id = await this.GenerateIdAsync(accessor, typeName).ConfigureAwait(false);
                accessor.SetId(document, id);
            }
            else
            {
                id = accessor.GetIdAsString(document);
                await this.BumpSeqIfNeededAsync(accessor, typeName, id).ConfigureAwait(false);
            }

            versionMapping?.SetVersion(document, 1);
            var json = Serialize(document, typeInfo, this.jsonOptions);
            writes.Add((this.DocKey(typeName, id), RedisDocument.BuildEnvelope(id, typeName, json, now, versionMapping != null ? 1 : null), id, document));
        }

        if (writes.Count == 0)
            return 0;

        // Guard duplicates before committing so the batch is all-or-nothing.
        foreach (var w in writes)
        {
            if (await this.db.KeyExistsAsync(w.Key).ConfigureAwait(false))
                throw new InvalidOperationException($"A document of type '{typeName}' with Id '{w.Id}' already exists.");
        }

        this.Log($"Redis BATCH JSON.SET {writes.Count} into {typeName}");
        foreach (var w in writes)
            await this.SetJsonAsync(w.Key, w.Envelope).ConfigureAwait(false);

        for (var i = 0; i < srcList.Count; i++)
        {
            if (ctxs != null)
                await this.RunAfterWriteAsync(ctxs[i], writes[i].Id, versionMapping?.GetVersion(srcList[i]) ?? 1, cancellationToken).ConfigureAwait(false);
        }
        foreach (var w in writes)
            this.PublishChange(DocumentChangeType.Inserted, w.Id, w.Doc);
        return writes.Count;
    }

    // ── Update ──────────────────────────────────────────────────────────

    public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("update", typeof(T).Name, () => this.UpdateImpl(document, jsonTypeInfo, cancellationToken));

    async Task UpdateImpl<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var typeName = this.ResolveTypeName<T>();
        await this.EnsureIndexAsync<T>(cancellationToken).ConfigureAwait(false);

        var ctx = this.NewWriteContext(DocumentOperation.Update, typeName, null, document);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);
        if (ctx?.Document is T mutated)
            document = mutated;

        if (accessor.IsDefaultId(document))
            throw new InvalidOperationException(
                $"Update requires a non-default Id on the document. Set the Id property on '{typeof(T).Name}' before calling Update.");

        var id = accessor.GetIdAsString(document);
        var key = this.DocKey(typeName, id);
        var existing = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
        if (existing == null || !this.StoredPassesFilters<T>(existing, typeInfo))
            throw new InvalidOperationException($"No document of type '{typeName}' with Id '{id}' was found to update.");

        int? expectedVersion = null;
        if (versionMapping != null)
        {
            var ev = versionMapping.GetVersion(document);
            var storedVersion = RedisDocument.GetVersion(existing);
            if (storedVersion != ev)
                throw new ConcurrencyException(typeName, id, ev, storedVersion);
            versionMapping.SetVersion(document, ev + 1);
            expectedVersion = ev;
        }

        var json = Serialize(document, typeInfo, this.jsonOptions);
        var envelope = RedisDocument.BuildEnvelope(id, typeName, json, DateTime.UtcNow,
            versionMapping != null ? expectedVersion + 1 : null, RedisDocument.GetCreatedAt(existing));

        this.Log($"Redis UPDATE {key}");
        await this.WriteWithVersionGuardAsync(key, envelope, typeName, id, expectedVersion, cancellationToken).ConfigureAwait(false);

        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document), cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Updated, id, document);
    }

    public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("upsert", typeof(T).Name, () => this.UpsertImpl(patch, jsonTypeInfo, cancellationToken));

    async Task UpsertImpl<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var typeName = this.ResolveTypeName<T>();
        await this.EnsureIndexAsync<T>(cancellationToken).ConfigureAwait(false);

        var ctx = this.NewWriteContext(DocumentOperation.Upsert, typeName, null, patch);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);
        if (ctx?.Document is T mutated)
            patch = mutated;

        if (accessor.IsDefaultId(patch))
            throw new InvalidOperationException(
                $"Upsert requires a non-default Id on the document. Set the Id property on '{typeof(T).Name}' before calling Upsert.");

        var id = accessor.GetIdAsString(patch);
        await this.BumpSeqIfNeededAsync(accessor, typeName, id).ConfigureAwait(false);
        var key = this.DocKey(typeName, id);
        var existing = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
        var now = DateTime.UtcNow;

        if (existing == null)
        {
            versionMapping?.SetVersion(patch, 1);
            var patchJson = StripNullProperties(Serialize(patch, typeInfo, this.jsonOptions));
            var envelope = RedisDocument.BuildEnvelope(id, typeName, patchJson, now, versionMapping != null ? 1 : null);
            this.Log($"Redis UPSERT (insert) {key}");
            await this.SetJsonAsync(key, envelope).ConfigureAwait(false);
        }
        else
        {
            int? guardVersion = null;
            if (versionMapping != null)
            {
                var expectedVersion = versionMapping.GetVersion(patch);
                var storedVersion = RedisDocument.GetVersion(existing);
                if (expectedVersion > 0 && storedVersion != expectedVersion)
                    throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
                versionMapping.SetVersion(patch, storedVersion + 1);
                if (expectedVersion > 0)
                    guardVersion = expectedVersion;
            }

            var patchJson = StripNullProperties(Serialize(patch, typeInfo, this.jsonOptions));
            var originalData = RedisDocument.GetDataJson(existing) ?? "{}";
            var merged = MergeJson(originalData, patchJson);
            var newVersion = versionMapping != null ? versionMapping.GetVersion(patch) : (int?)null;
            var envelope = RedisDocument.BuildEnvelope(id, typeName, merged, now, newVersion, RedisDocument.GetCreatedAt(existing));

            this.Log($"Redis UPSERT (merge) {key}");
            await this.WriteWithVersionGuardAsync(key, envelope, typeName, id, guardVersion, cancellationToken).ConfigureAwait(false);
        }

        await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Updated, id, patch);
    }

    // Atomic compare-and-swap on the envelope version field via a Lua script. The version is read with the
    // JSONPath form ($.version → "[N]") so it does not depend on RedisJSON's legacy-path interpretation.
    // Returns: -1 not found, 0 version conflict, 1 written.
    const string CasScript = @"
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
local v = redis.call('JSON.GET', KEYS[1], '$.version')
if v ~= ARGV[1] then return 0 end
redis.call('JSON.SET', KEYS[1], '$', ARGV[2])
return 1";

    async Task WriteWithVersionGuardAsync(string key, string envelope, string typeName, string id, int? expectedVersion, CancellationToken ct)
    {
        if (expectedVersion == null)
        {
            await this.SetJsonAsync(key, envelope).ConfigureAwait(false);
            return;
        }

        var result = await this.db.ScriptEvaluateAsync(
            CasScript,
            new RedisKey[] { key },
            new RedisValue[] { $"[{expectedVersion.Value.ToString(CultureInfo.InvariantCulture)}]", envelope }).ConfigureAwait(false);

        var code = (long)result;
        if (code == -1)
            throw new InvalidOperationException($"No document of type '{typeName}' with Id '{id}' was found to update.");
        if (code == 0)
            throw new ConcurrencyException(typeName, id, expectedVersion.Value);
    }

    // ── SetProperty / RemoveProperty ────────────────────────────────────

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
        await this.EnsureIndexAsync<T>(cancellationToken).ConfigureAwait(false);

        var key = this.DocKey(typeName, resolvedId);
        var existing = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
        if (existing == null || !this.StoredPassesFilters<T>(existing, typeInfo))
            return false;

        var node = JsonNode.Parse(RedisDocument.GetDataJson(existing) ?? "{}")!.AsObject();
        SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));
        var envelope = RedisDocument.BuildEnvelope(resolvedId, typeName, node.ToJsonString(), DateTime.UtcNow,
            existing[RedisDocument.Version] != null ? RedisDocument.GetVersion(existing) : null, RedisDocument.GetCreatedAt(existing));

        this.Log($"Redis SET PROPERTY {key} Path={jsonPath}");
        await this.SetJsonAsync(key, envelope).ConfigureAwait(false);
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
        await this.EnsureIndexAsync<T>(cancellationToken).ConfigureAwait(false);

        var key = this.DocKey(typeName, resolvedId);
        var existing = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
        if (existing == null || !this.StoredPassesFilters<T>(existing, typeInfo))
            return false;

        var node = JsonNode.Parse(RedisDocument.GetDataJson(existing) ?? "{}")!.AsObject();
        RemoveNestedProperty(node, jsonPath);
        var envelope = RedisDocument.BuildEnvelope(resolvedId, typeName, node.ToJsonString(), DateTime.UtcNow,
            existing[RedisDocument.Version] != null ? RedisDocument.GetVersion(existing) : null, RedisDocument.GetCreatedAt(existing));

        this.Log($"Redis REMOVE PROPERTY {key} Path={jsonPath}");
        await this.SetJsonAsync(key, envelope).ConfigureAwait(false);
        return true;
    }

    // ── Get / GetDiff ───────────────────────────────────────────────────

    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("get", typeof(T).Name, () => this.GetImpl(id, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    async Task<T?> GetImpl<T>(object id, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        await this.EnsureModulesAsync(cancellationToken).ConfigureAwait(false);

        var key = this.DocKey(typeName, resolvedId);
        this.Log($"Redis GET {key}");
        var existing = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
        if (existing == null)
            return null;

        var dataJson = RedisDocument.GetDataJson(existing);
        if (dataJson == null)
            return null;
        var doc = Deserialize(dataJson, typeInfo, this.jsonOptions);
        if (doc != null && !this.PassesGlobalFilters(doc))
            return null;
        if (doc != null)
            ComputedReadBack.Apply(new[] { doc }, this.options.ResolveComputedMappings(typeof(T)));
        return doc;
    }

    public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("get_diff", typeof(T).Name, () => this.GetDiffImpl(id, modified, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    async Task<JsonPatchDocument<T>?> GetDiffImpl<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        await this.EnsureModulesAsync(cancellationToken).ConfigureAwait(false);

        var key = this.DocKey(typeName, resolvedId);
        var existing = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
        if (existing == null || !this.StoredPassesFilters<T>(existing, typeInfo))
            return null;

        var originalJson = RedisDocument.GetDataJson(existing) ?? "{}";
        var modifiedJson = Serialize(modified, typeInfo, this.jsonOptions);
        return JsonDiff.CreatePatch<T>(originalJson, modifiedJson, this.jsonOptions);
    }

    // ── String query surface (unsupported — use LINQ) ───────────────────

    public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Redis does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload, or FullTextSearch/NearestVectors for native search.");

    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Redis does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload, or FullTextSearch/NearestVectors for native search.");

    // ── Count ───────────────────────────────────────────────────────────

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("count", typeof(T).Name, () => this.CountImpl<T>(whereClause, parameters, cancellationToken), r => r);

    async Task<int> CountImpl<T>(string? whereClause, object? parameters, CancellationToken cancellationToken) where T : class
    {
        if (!string.IsNullOrWhiteSpace(whereClause))
            throw new NotSupportedException("Redis does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload instead.");

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;
        var keys = await this.ScanKeysAsync(typeName, cancellationToken).ConfigureAwait(false);

        if (!hasFilters)
            return keys.Count;

        var matched = 0;
        foreach (var key in keys)
        {
            var env = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
            var dataJson = env == null ? null : RedisDocument.GetDataJson(env);
            var doc = dataJson == null ? null : Deserialize(dataJson, typeInfo, this.jsonOptions);
            if (doc != null && this.PassesGlobalFilters(doc))
                matched++;
        }
        return matched;
    }

    // ── Remove / Clear ──────────────────────────────────────────────────

    public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("remove", typeof(T).Name, () => this.RemoveImpl<T>(id, cancellationToken), r => r ? 1 : 0);

    async Task<bool> RemoveImpl<T>(object id, CancellationToken cancellationToken) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        await this.EnsureModulesAsync(cancellationToken).ConfigureAwait(false);

        var ctx = this.NewWriteContext<T>(DocumentOperation.Delete, typeName, id, null);
        await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false);

        var key = this.DocKey(typeName, resolvedId);
        if (this.options.ResolveQueryFilters(typeof(T)).Count > 0)
        {
            var existing = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
            if (existing == null || !this.StoredPassesFilters<T>(existing, null))
                return false;
        }

        this.Log($"Redis DELETE {key}");
        var deleted = await this.DeleteKeyAsync(key).ConfigureAwait(false);
        if (deleted)
        {
            await this.RunAfterWriteAsync(ctx, id, null, cancellationToken).ConfigureAwait(false);
            this.PublishChange<T>(DocumentChangeType.Removed, resolvedId, null);
        }
        return deleted;
    }

    public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("clear", typeof(T).Name, () => this.ClearImpl<T>(cancellationToken), r => r);

    async Task<int> ClearImpl<T>(CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        await this.EnsureModulesAsync(cancellationToken).ConfigureAwait(false);
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Clear, typeName);
        await this.RunBeforeBulkAsync(bulkCtx, cancellationToken).ConfigureAwait(false);

        this.Log($"Redis CLEAR {typeName}");
        var keys = await this.ScanKeysAsync(typeName, cancellationToken).ConfigureAwait(false);
        var deleted = 0;
        foreach (var key in keys)
        {
            var eligible = true;
            if (hasFilters)
            {
                var env = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
                var dataJson = env == null ? null : RedisDocument.GetDataJson(env);
                var doc = dataJson == null ? null : Deserialize(dataJson, typeInfo, this.jsonOptions);
                eligible = doc != null && this.PassesGlobalFilters(doc);
            }
            if (eligible && await this.DeleteKeyAsync(key).ConfigureAwait(false))
                deleted++;
        }

        await this.RunAfterBulkAsync(bulkCtx, deleted, cancellationToken).ConfigureAwait(false);
        if (deleted > 0)
            this.PublishChange<T>(DocumentChangeType.Cleared, "", null);
        return deleted;
    }

    /// <inheritdoc />
    public async Task ClearAll(CancellationToken cancellationToken = default)
    {
        await this.EnsureModulesAsync(cancellationToken).ConfigureAwait(false);
        this.Log("Redis CLEAR ALL");
        var server = this.GetServer();
        var pattern = $"{this.keyPrefix}doc:*";
        var keys = new List<RedisKey>();
        await foreach (var key in server.KeysAsync(database: this.db.Database, pattern: pattern).WithCancellation(cancellationToken).ConfigureAwait(false))
            keys.Add(key);
        if (keys.Count > 0)
            await this.db.KeyDeleteAsync(keys.ToArray()).ConfigureAwait(false);
    }

    // ── Batch remove (native loop) ──────────────────────────────────────

    public Task<int> BatchRemove<T>(IEnumerable<object> ids, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("batch_remove", typeof(T).Name, () => this.BatchRemoveImpl<T>(ids, cancellationToken), r => r);

    async Task<int> BatchRemoveImpl<T>(IEnumerable<object> ids, CancellationToken cancellationToken) where T : class
    {
        var accessor = this.idCache.GetOrCreate<T>(null);
        var typeName = this.ResolveTypeName<T>();
        await this.EnsureModulesAsync(cancellationToken).ConfigureAwait(false);

        var removed = 0;
        foreach (var id in ids)
        {
            var key = this.DocKey(typeName, accessor.ResolveId(id));
            if (await this.DeleteKeyAsync(key).ConfigureAwait(false))
                removed++;
        }
        return removed;
    }

    // ── Unit of work (compensating apply) ───────────────────────────────

    Task IUnitOfWorkEngine.RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
        => this.Tracker.Track("transaction", "(transaction)", () => this.RunUnitImpl(work, cancellationToken));

    async Task RunUnitImpl(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        var tx = new RedisCompensatingStore(this);
        var buffer = new List<Action>();
        this.pendingChanges.Value = buffer;
        try
        {
            await work(tx, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
        finally
        {
            this.pendingChanges.Value = null;
        }
        foreach (var emit in buffer)
            emit();
    }

    // ── Change notifications (in-process) ───────────────────────────────

    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange<T>(CancellationToken cancellationToken = default) where T : class
        => this.broadcaster.Observe<T>(cancellationToken);

    void PublishChange<T>(DocumentChangeType changeType, string id, T? document) where T : class
    {
        var change = new DocumentChange<T> { ChangeType = changeType, Id = id, Document = document };
        var buffer = this.pendingChanges.Value;
        if (buffer != null)
            buffer.Add(() => this.broadcaster.Publish(change));
        else if (this.broadcaster.HasSubscribers<T>())
            this.broadcaster.Publish(change);
    }

    // ── Full-text search (RediSearch TEXT + BM25) ───────────────────────

    public Task<IReadOnlyList<FullTextResult<T>>> FullTextSearch<T>(
        string searchText,
        int maxResults = 50,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("full_text_search", typeof(T).Name, () => this.FullTextSearchImpl(searchText, maxResults, filter, cancellationToken), r => r.Count);

    async Task<IReadOnlyList<FullTextResult<T>>> FullTextSearchImpl<T>(string searchText, int maxResults, Expression<Func<T, bool>>? filter, CancellationToken ct) where T : class
    {
        _ = this.options.ResolveFullTextMapping(typeof(T))
            ?? throw new NotSupportedException($"No full-text property mapped for type '{typeof(T).Name}'. Call MapFullTextProperty<{typeof(T).Name}>() in options.");
        ArgumentException.ThrowIfNullOrEmpty(searchText);
        if (maxResults <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxResults));

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        await this.EnsureIndexAsync<T>(ct).ConfigureAwait(false);

        // Terms are OR-combined (contract). Escape then join with the RediSearch union operator.
        var terms = searchText.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var queryString = terms.Length == 0 ? "*" : string.Join(" | ", terms.Select(RedisSearchQueryBuilder.EscapeText));
        var fetch = filter == null ? maxResults : maxResults * 4;

        var q = new Query(queryString).SetWithScores().SetNoContent().Limit(0, fetch).Dialect(2);
        var result = await this.ft.SearchAsync(this.IndexName(typeName), q).ConfigureAwait(false);

        var postFilter = filter == null ? null : ExpressionInterpreter.Interpret(filter);
        var results = new List<FullTextResult<T>>();
        foreach (var doc in result.Documents)
        {
            if (results.Count >= maxResults)
                break;
            var env = await this.GetEnvelopeAsync(doc.Id).ConfigureAwait(false);
            var dataJson = env == null ? null : RedisDocument.GetDataJson(env);
            var model = dataJson == null ? null : Deserialize(dataJson, typeInfo, this.jsonOptions);
            if (model != null && (postFilter == null || postFilter(model)))
                results.Add(new FullTextResult<T> { Document = model, Score = doc.Score });
        }
        return results;
    }

    // ── Vector KNN (RediSearch HNSW/FLAT) ───────────────────────────────

    public Task<IReadOnlyList<VectorResult<T>>> NearestVectors<T>(
        ReadOnlyMemory<float> query,
        int k,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("nearest_vectors", typeof(T).Name, () => this.NearestVectorsImpl(query, k, filter, cancellationToken), r => r.Count);

    async Task<IReadOnlyList<VectorResult<T>>> NearestVectorsImpl<T>(ReadOnlyMemory<float> query, int k, Expression<Func<T, bool>>? filter, CancellationToken ct) where T : class
    {
        var mapping = this.options.ResolveVectorMapping(typeof(T))
            ?? throw new NotSupportedException($"No vector property mapped for type '{typeof(T).Name}'. Call MapVectorProperty<{typeof(T).Name}>() in options.");
        if (query.Length != mapping.Dimensions)
            throw new ArgumentException($"Query vector has {query.Length} dimensions; mapping expects {mapping.Dimensions}.", nameof(query));
        if (k <= 0)
            throw new ArgumentOutOfRangeException(nameof(k));

        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        await this.EnsureIndexAsync<T>(ct).ConfigureAwait(false);

        var field = this.IndexFieldsFor<T>().First(f => f.Kind == RedisFieldKind.Vector);
        var blob = MemoryMarshalToBytes(query.Span);
        var fetch = filter == null ? k : k * 4;

        var q = new Query($"*=>[KNN {fetch} @{field.Alias} $vec AS __vscore]")
            .AddParam("vec", blob)
            .SetSortBy("__vscore")
            .ReturnFields("__vscore")
            .Limit(0, fetch)
            .Dialect(2);

        var result = await this.ft.SearchAsync(this.IndexName(typeName), q).ConfigureAwait(false);
        var postFilter = filter == null ? null : ExpressionInterpreter.Interpret(filter);
        var results = new List<VectorResult<T>>();
        foreach (var doc in result.Documents)
        {
            if (results.Count >= k)
                break;
            var env = await this.GetEnvelopeAsync(doc.Id).ConfigureAwait(false);
            var dataJson = env == null ? null : RedisDocument.GetDataJson(env);
            var model = dataJson == null ? null : Deserialize(dataJson, typeInfo, this.jsonOptions);
            if (model != null && (postFilter == null || postFilter(model)))
            {
                var score = doc.GetProperties().FirstOrDefault(p => p.Key == "__vscore").Value;
                results.Add(new VectorResult<T> { Document = model, Score = score.IsNull ? float.NaN : (float)(double)score });
            }
        }
        return results;
    }

    static byte[] MemoryMarshalToBytes(ReadOnlySpan<float> span)
    {
        var bytes = new byte[span.Length * sizeof(float)];
        System.Runtime.InteropServices.MemoryMarshal.AsBytes(span).CopyTo(bytes);
        return bytes;
    }

    // ── Spatial (client-side haversine over mapped GeoPoint) ────────────

    public Task<IReadOnlyList<SpatialResult<T>>> WithinRadius<T>(GeoPoint center, double radiusMeters, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("within_radius", typeof(T).Name, () => this.SpatialImpl(center, filter, r => r.Where(x => x.Distance <= radiusMeters), null, cancellationToken), r => r.Count);

    public Task<IReadOnlyList<SpatialResult<T>>> NearestNeighbors<T>(GeoPoint center, int count, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("nearest_neighbors", typeof(T).Name, () => this.SpatialImpl(center, filter, r => r, count, cancellationToken), r => r.Count);

    public Task<IReadOnlyList<T>> WithinBoundingBox<T>(GeoBoundingBox box, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("within_box", typeof(T).Name, () => this.WithinBoundingBoxImpl(box, filter, cancellationToken), r => r.Count);

    async Task<IReadOnlyList<SpatialResult<T>>> SpatialImpl<T>(GeoPoint center, Expression<Func<T, bool>>? filter, Func<IEnumerable<(T Doc, double Distance)>, IEnumerable<(T Doc, double Distance)>> shape, int? take, CancellationToken ct) where T : class
    {
        var mapping = this.options.ResolveSpatialMapping(typeof(T))
            ?? throw new NotSupportedException($"No spatial property mapped for type '{typeof(T).Name}'. Call MapSpatialProperty<{typeof(T).Name}>() in options.");
        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var postFilter = filter == null ? null : ExpressionInterpreter.Interpret(filter);

        var docs = await this.LoadDocumentsAsync(await this.ScanKeysAsync(typeName, ct).ConfigureAwait(false), typeInfo, ct).ConfigureAwait(false);
        var scored = new List<(T Doc, double Distance)>();
        foreach (var doc in docs)
        {
            var match = this.PassesGlobalFilters(doc) && (postFilter == null || postFilter(doc));
            var point = match ? mapping.GetGeoPoint(doc) : null;
            if (point is not null)
                scored.Add((doc, Haversine(center, point.Value)));
        }
        IEnumerable<(T Doc, double Distance)> ordered = scored.OrderBy(x => x.Distance);
        ordered = shape(ordered);
        if (take.HasValue)
            ordered = ordered.Take(take.Value);
        return ordered.Select(x => new SpatialResult<T> { Document = x.Doc, DistanceMeters = x.Distance }).ToList();
    }

    async Task<IReadOnlyList<T>> WithinBoundingBoxImpl<T>(GeoBoundingBox box, Expression<Func<T, bool>>? filter, CancellationToken ct) where T : class
    {
        var mapping = this.options.ResolveSpatialMapping(typeof(T))
            ?? throw new NotSupportedException($"No spatial property mapped for type '{typeof(T).Name}'. Call MapSpatialProperty<{typeof(T).Name}>() in options.");
        var typeInfo = this.FindTypeInfo<T>(null);
        var typeName = this.ResolveTypeName<T>();
        var postFilter = filter == null ? null : ExpressionInterpreter.Interpret(filter);

        var docs = await this.LoadDocumentsAsync(await this.ScanKeysAsync(typeName, ct).ConfigureAwait(false), typeInfo, ct).ConfigureAwait(false);
        var results = new List<T>();
        foreach (var doc in docs)
        {
            var match = this.PassesGlobalFilters(doc) && (postFilter == null || postFilter(doc));
            var point = match ? mapping.GetGeoPoint(doc) : null;
            if (point is { } p
                && p.Latitude >= box.MinLatitude && p.Latitude <= box.MaxLatitude
                && p.Longitude >= box.MinLongitude && p.Longitude <= box.MaxLongitude)
                results.Add(doc);
        }
        return results;
    }

    static double Haversine(GeoPoint a, GeoPoint b)
    {
        const double r = 6_371_000d;
        var dLat = (b.Latitude - a.Latitude) * Math.PI / 180;
        var dLon = (b.Longitude - a.Longitude) * Math.PI / 180;
        var lat1 = a.Latitude * Math.PI / 180;
        var lat2 = b.Latitude * Math.PI / 180;
        var h = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) + Math.Sin(dLon / 2) * Math.Sin(dLon / 2) * Math.Cos(lat1) * Math.Cos(lat2);
        return 2 * r * Math.Asin(Math.Min(1, Math.Sqrt(h)));
    }

    // ── Server-side GroupBy via FT.AGGREGATE (the differentiator) ───────

    /// <summary>
    /// Runs a genuine server-side <c>FT.AGGREGATE … GROUPBY … REDUCE COUNT/SUM</c> over the type's RediSearch
    /// index and returns one row per group. The group field (and any summed field) must be declared with
    /// <see cref="RedisDocumentStoreOptions.MapIndexedProperty"/>. This is where Redis differs from the other
    /// document providers, which group client-side.
    /// </summary>
    internal async Task<IReadOnlyList<(string Key, long Count, double Sum)>> ServerSideGroupByAsync<T>(
        Expression<Func<T, object>> groupField,
        Expression<Func<T, object>>? sumField,
        CancellationToken ct) where T : class
    {
        var hasIndex = await this.EnsureIndexAsync<T>(ct).ConfigureAwait(false);
        var fields = this.IndexFieldsFor<T>();
        var groupPath = string.Join('.', RedisSearchQueryBuilder.MemberPath(groupField));
        var groupFieldSpec = fields.FirstOrDefault(f => f.ClrPath == groupPath)
            ?? throw new NotSupportedException(
                $"Server-side GroupBy requires the group field '{groupPath}' to be declared with MapIndexedProperty<{typeof(T).Name}>().");
        if (!hasIndex)
            throw new NotSupportedException($"No RediSearch index exists for '{typeof(T).Name}'.");

        var reducers = new List<NRedisStack.Search.Aggregation.Reducer> { NRedisStack.Search.Aggregation.Reducers.Count().As("count") };
        if (sumField != null)
        {
            var sumPath = string.Join('.', RedisSearchQueryBuilder.MemberPath(sumField));
            var sumSpec = fields.FirstOrDefault(f => f.ClrPath == sumPath)
                ?? throw new NotSupportedException($"Server-side GroupBy sum requires '{sumPath}' to be declared with MapIndexedProperty<{typeof(T).Name}>().");
            reducers.Add(NRedisStack.Search.Aggregation.Reducers.Sum($"@{sumSpec.Alias}").As("sum"));
        }

        var request = new NRedisStack.Search.AggregationRequest("*", 2)
            .GroupBy($"@{groupFieldSpec.Alias}", reducers.ToArray());

        this.Log($"Redis FT.AGGREGATE {this.IndexName(this.ResolveTypeName<T>())} GROUPBY {groupFieldSpec.Alias}");
        var result = await this.ft.AggregateAsync(this.IndexName(this.ResolveTypeName<T>()), request).ConfigureAwait(false);

        var rows = new List<(string, long, double)>();
        var rowCount = (int)result.TotalResults;
        for (var i = 0; i < rowCount; i++)
        {
            var row = result.GetRow(i);
            if (row.FieldCount() > 0)
            {
                var key = (row.ContainsKey(groupFieldSpec.Alias) ? row.GetString(groupFieldSpec.Alias) : "") ?? "";
                var count = row.ContainsKey("count") ? row.GetLong("count") : 0;
                var sum = row.ContainsKey("sum") ? row.GetDouble("sum") : 0d;
                rows.Add((key, count, sum));
            }
        }
        return rows;
    }

    // ── Query-support helpers (used by RedisDocumentQuery) ──────────────

    IServer GetServer()
    {
        var endpoints = this.mux.GetEndPoints();
        return this.mux.GetServer(endpoints[0]);
    }

    async Task<List<string>> ScanKeysAsync(string typeName, CancellationToken ct)
    {
        var server = this.GetServer();
        var keys = new List<string>();
        await foreach (var key in server.KeysAsync(database: this.db.Database, pattern: this.DocScanPattern(typeName)).WithCancellation(ct).ConfigureAwait(false))
            keys.Add(key!);
        return keys;
    }

    // Resolves candidate keys: an FT.SEARCH pushdown over indexed conjuncts when possible, else a full scan.
    // The caller re-applies the full predicate client-side, so the candidate set need only be a superset.
    internal async Task<List<string>> ResolveCandidateKeysAsync<T>(IReadOnlyList<Expression<Func<T, bool>>> predicates, CancellationToken ct) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var hasIndex = await this.EnsureIndexAsync<T>(ct).ConfigureAwait(false);
        if (!hasIndex)
            return await this.ScanKeysAsync(typeName, ct).ConfigureAwait(false);

        var query = RedisSearchQueryBuilder.BuildPushdown(predicates, this.IndexFieldsFor<T>());
        if (query == null)
            return await this.ScanKeysAsync(typeName, ct).ConfigureAwait(false);

        return await this.SearchKeysAsync(typeName, query, ct).ConfigureAwait(false);
    }

    async Task<List<string>> SearchKeysAsync(string typeName, string queryString, CancellationToken ct)
    {
        var indexName = this.IndexName(typeName);
        var keys = new List<string>();
        const int batch = 500;
        var offset = 0;
        this.Log($"Redis FT.SEARCH {indexName} '{queryString}'");
        while (!ct.IsCancellationRequested)
        {
            var q = new Query(queryString).SetNoContent().Limit(offset, batch).Dialect(2);
            var result = await this.ft.SearchAsync(indexName, q).ConfigureAwait(false);
            foreach (var doc in result.Documents)
                keys.Add(doc.Id);
            offset += batch;
            if (offset >= result.TotalResults || result.Documents.Count == 0)
                break;
        }
        return keys;
    }

    // Loads and deserializes the documents behind a candidate key list, honouring computed read-back.
    internal async Task<List<T>> LoadDocumentsAsync<T>(IReadOnlyList<string> keys, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var results = new List<T>(keys.Count);
        foreach (var key in keys)
        {
            ct.ThrowIfCancellationRequested();
            var env = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
            var dataJson = env == null ? null : RedisDocument.GetDataJson(env);
            var doc = dataJson == null ? null : Deserialize(dataJson, typeInfo, this.jsonOptions);
            if (doc != null)
                results.Add(doc);
        }
        ComputedReadBack.Apply(results, this.options.ResolveComputedMappings(typeof(T)));
        return results;
    }

    internal async Task<int> DeleteWhereAsync<T>(IReadOnlyList<Expression<Func<T, bool>>> predicates, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var compiled = predicates.Select(ExpressionInterpreter.Interpret).ToList();
        var keys = await this.ResolveCandidateKeysAsync(predicates, ct).ConfigureAwait(false);
        var deleted = 0;
        foreach (var key in keys)
        {
            var env = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
            var dataJson = env == null ? null : RedisDocument.GetDataJson(env);
            var doc = dataJson == null ? null : Deserialize(dataJson, typeInfo, this.jsonOptions);
            if (doc != null && compiled.All(p => p(doc)) && await this.DeleteKeyAsync(key).ConfigureAwait(false))
                deleted++;
        }
        return deleted;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    internal async Task<int> UpdatePropertyWhereAsync<T>(IReadOnlyList<Expression<Func<T, bool>>> predicates, string jsonPath, object? value, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var compiled = predicates.Select(ExpressionInterpreter.Interpret).ToList();
        var keys = await this.ResolveCandidateKeysAsync(predicates, ct).ConfigureAwait(false);
        var now = DateTime.UtcNow;
        var updated = 0;
        foreach (var key in keys)
        {
            var env = await this.GetEnvelopeAsync(key).ConfigureAwait(false);
            var dataJson = env == null ? null : RedisDocument.GetDataJson(env);
            var doc = dataJson == null ? null : Deserialize(dataJson, typeInfo, this.jsonOptions);
            if (doc != null && compiled.All(p => p(doc)))
            {
                var node = JsonNode.Parse(dataJson!)!.AsObject();
                SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));
                var envelope = RedisDocument.BuildEnvelope(env![RedisDocument.Id]!.GetValue<string>(), typeName, node.ToJsonString(), now,
                    env[RedisDocument.Version] != null ? RedisDocument.GetVersion(env) : null, RedisDocument.GetCreatedAt(env));
                await this.SetJsonAsync(key, envelope).ConfigureAwait(false);
                updated++;
            }
        }
        return updated;
    }

    bool StoredPassesFilters<T>(JsonObject envelope, JsonTypeInfo<T>? typeInfo) where T : class
    {
        if (this.options.ResolveQueryFilters(typeof(T)).Count == 0)
            return true;
        var dataJson = RedisDocument.GetDataJson(envelope);
        var doc = dataJson == null ? null : Deserialize(dataJson, typeInfo, this.jsonOptions);
        return doc != null && this.PassesGlobalFilters(doc);
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

    sealed class RedisCompensatingStore(RedisDocumentStore inner) : CompensatingStore
    {
        protected override IDocumentStore Inner => inner;

        public override async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default)
        {
            await inner.Insert(document, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            var accessor = inner.IdCache.GetOrCreate(inner.FindTypeInfo(jsonTypeInfo));
            this.TrackInsert(inner.ResolveTypeNameFor<T>(), accessor.GetIdAsString(document));
        }

        protected override async Task DeleteTrackedAsync(string typeName, string id, CancellationToken ct)
            => await inner.DeleteKeyAsync(inner.DocKey(typeName, id)).ConfigureAwait(false);
    }
}
