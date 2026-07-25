using Shiny.DocumentDb.Internal.Query;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Microsoft.JSInterop;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.IndexedDb;

public partial class IndexedDbDocumentStore : DocumentProviderBase, IDocumentStore, ITemporalDocumentStore, IUnitOfWorkEngine, IAsyncDisposable
{
    readonly IndexedDbDocumentStoreOptions options;
    readonly JsonSerializerOptions jsonOptions;
    readonly IdAccessorCache idCache;
    Action<string>? logging;
    readonly SemaphoreSlim moduleLock = new(1, 1);
    bool moduleImported;

    // IJSRuntime is kept on the constructor signature for backwards compatibility
    // with existing DI registrations, but is no longer used — all JS interop now
    // flows through [JSImport] in IndexedDbJsInterop (AOT/reflection-free).
    public IndexedDbDocumentStore(IJSRuntime jsRuntime, IndexedDbDocumentStoreOptions options)
        : this(options)
    {
    }

    /// <summary>Constructs the store and wires DI-registered interceptors from <paramref name="serviceProvider"/>
    /// (so container-registered <see cref="IDocumentInterceptor"/>s fire alongside options-registered ones, and a
    /// scoped interceptor can resolve <see cref="DocumentWriteContext.Services"/>).</summary>
    public IndexedDbDocumentStore(IndexedDbDocumentStoreOptions options, IServiceProvider serviceProvider) : this(options)
    {
        this.AttachServiceProvider(serviceProvider);
        this.logging = DocumentStoreLogging.Compose(this.logging, this.Logger);
    }

    public IndexedDbDocumentStore(IndexedDbDocumentStoreOptions options)
    {
        this.options = options;
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        this.logging = options.Logging;
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName, options.IdConverters);
        options.ResolveVersionJsonPaths(this.jsonOptions);
        options.ResolveComputedJsonNames(this.jsonOptions);
    }

    void Log(string message) => this.logging?.Invoke(message);

    string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);

    string ResolveStoreName<T>() => this.options.ResolveStoreName(this.ResolveTypeName<T>());

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

    async ValueTask EnsureModuleAsync()
    {
        if (this.moduleImported)
            return;

        await this.moduleLock.WaitAsync();
        try
        {
            if (this.moduleImported)
                return;

            await IndexedDbJsInterop.ImportAsync();
            var storeNames = this.options.GetAllStoreNames().Distinct().ToArray();
            await IndexedDbJsInterop.Initialize(this.options.DatabaseName, this.options.Version, storeNames);
            this.moduleImported = true;
        }
        finally
        {
            this.moduleLock.Release();
        }
    }

    static string SerializeRecord(DocumentRecord record)
        => JsonSerializer.Serialize(record, IndexedDbInteropJsonContext.Default.DocumentRecord);

    static string SerializeRecords(DocumentRecord[] records)
        => JsonSerializer.Serialize(records, IndexedDbInteropJsonContext.Default.DocumentRecordArray);

    static DocumentRecord[] DeserializeRecords(string json)
        => JsonSerializer.Deserialize(json, IndexedDbInteropJsonContext.Default.DocumentRecordArray) ?? Array.Empty<DocumentRecord>();

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static string Serialize<T>(T value, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? JsonSerializer.Serialize(value, typeInfo) : JsonSerializer.Serialize(value, options);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static T? Deserialize<T>(string json, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? JsonSerializer.Deserialize(json, typeInfo) : JsonSerializer.Deserialize<T>(json, options);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    static string ResolvePropertyPath<T>(Expression<Func<T, object>> property, JsonSerializerOptions options, JsonTypeInfo<T>? typeInfo)
        => typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, options, typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, options);

    string GenerateId<T>(IdAccessor<T> accessor, string typeName, IReadOnlyList<DocumentRecord>? existingDocs = null) where T : class
    {
        switch (accessor.Kind)
        {
            case IdKind.Guid:
                return Guid.NewGuid().ToString("N");

            case IdKind.String:
                return Guid.NewGuid().ToString();

            case IdKind.Int:
            case IdKind.Long:
                long max = 0;
                if (existingDocs != null)
                {
                    foreach (var doc in existingDocs)
                    {
                        if (doc.TypeName == typeName && long.TryParse(doc.Id, CultureInfo.InvariantCulture, out var v) && v > max)
                            max = v;
                    }
                }
                return (max + 1).ToString(CultureInfo.InvariantCulture);

            case IdKind.Custom:
                return accessor.GenerateOrThrow();

            default:
                throw new InvalidOperationException($"Unsupported Id kind: {accessor.Kind}");
        }
    }

    // ── IDocumentStore ──────────────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        return new IndexedDbDocumentQuery<T>(this, typeInfo);
    }

    protected override InterceptorPipeline Interceptors => this.options.Interceptors;
    protected override DocumentMappingRegistry Mappings => this.options.Mappings;
    protected override IdAccessorCache IdCache => this.idCache;
    protected override JsonTypeInfo<T>? ResolveTypeInfo<T>(JsonTypeInfo<T>? provided) where T : class => this.FindTypeInfo(provided);
    protected override string ResolveDocumentTypeName<T>() where T : class => this.ResolveTypeName<T>();

    public Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("insert", typeof(T).Name, () => this.InsertImpl(document, jsonTypeInfo, cancellationToken));

    async Task InsertImpl<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var write = await this.BeginWriteAsync(DocumentOperation.Insert, document, null, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
        if (!write.Proceed)
            return;
        var (typeInfo, typeName, versionMapping) = (write.TypeInfo, write.TypeName, write.VersionMapping);
        var accessor = write.Accessor;
        document = write.Doc;
        var storeName = this.ResolveStoreName<T>();

        var id = this.ResolveInsertId(write, accessor => accessor.GetIdAsString(document));

        versionMapping?.SetVersion(document, 1);
        var json = Serialize(document, typeInfo, this.jsonOptions);
        var now = DateTimeOffset.UtcNow.ToString("o");
        var compositeKey = $"{typeName}:{id}";

        await this.EnsureModuleAsync();

        var record = new DocumentRecord
        {
            Key = compositeKey,
            Id = id,
            TypeName = typeName,
            Data = json,
            CreatedAt = now,
            UpdatedAt = now
        };

        this.Log($"IndexedDB INSERT into {storeName} Id={id}");
        // Atomic get-check-put in one transaction — a concurrent insert on the same key can't also slip through.
        var outcome = await IndexedDbJsInterop.InsertIfAbsent(storeName, SerializeRecord(record));
        if (outcome == "exists")
            throw new InvalidOperationException(
                $"A document of type '{typeName}' with Id '{id}' already exists.");
        await this.AppendHistoryAsync<T>(id, typeName, TemporalOperation.Inserted, json);
        await this.RunAfterWriteAsync(write.Context, id, versionMapping?.GetVersion(document) ?? 1, cancellationToken).ConfigureAwait(false);
    }

    public Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("batch_insert", typeof(T).Name, () => this.BatchInsertImpl(documents, jsonTypeInfo, cancellationToken), r => r);

    async Task<int> BatchInsertImpl<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var storeName = this.ResolveStoreName<T>();
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));

        await this.EnsureModuleAsync();
        DocumentRecord[]? existingDocs = null;

        var docList = documents as IReadOnlyList<T> ?? documents.ToList();

        // Per-doc BeforeWrite before serialization.
        DocumentWriteContext[]? ctxs = null;
        if (this.HasPerDocInterceptors)
        {
            var mutableDocs = docList.ToList();
            ctxs = await this.RunBeforeWriteBatchAsync(mutableDocs, typeName, cancellationToken).ConfigureAwait(false);
            docList = mutableDocs;
        }

        var records = new List<DocumentRecord>();
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
                        existingDocs ??= DeserializeRecords(await IndexedDbJsInterop.GetAllByTypeName(storeName, typeName));
                        var seed = this.GenerateId(accessor, typeName, existingDocs);
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
            var now = DateTimeOffset.UtcNow.ToString("o");
            records.Add(new DocumentRecord
            {
                Key = $"{typeName}:{id}",
                Id = id,
                TypeName = typeName,
                Data = json,
                CreatedAt = now,
                UpdatedAt = now
            });
        }

        if (records.Count == 0)
            return 0;

        this.Log($"IndexedDB BATCH INSERT {records.Count} docs into {storeName}");
        await IndexedDbJsInterop.BatchPut(storeName, SerializeRecords(records.ToArray()));
        for (var i = 0; i < records.Count; i++)
        {
            await this.AppendHistoryAsync<T>(records[i].Id, typeName, TemporalOperation.Inserted, records[i].Data);
            if (ctxs != null)
                await this.RunAfterWriteAsync(ctxs[i], records[i].Id, versionMapping?.GetVersion(docList[i]) ?? 1, cancellationToken).ConfigureAwait(false);
        }
        return records.Count;
    }

    public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("update", typeof(T).Name, () => this.UpdateImpl(document, jsonTypeInfo, cancellationToken));

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "DocumentRecord is a simple internal DTO with string properties.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "DocumentRecord is a simple internal DTO with string properties.")]
    async Task UpdateImpl<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var write = await this.BeginWriteAsync(DocumentOperation.Update, document, null, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
        if (!write.Proceed)
            return;
        var (typeInfo, typeName, versionMapping) = (write.TypeInfo, write.TypeName, write.VersionMapping);
        var accessor = write.Accessor;
        document = write.Doc;

        var id = this.RequireDocumentId(write);
        var storeName = this.ResolveStoreName<T>();
        var compositeKey = $"{typeName}:{id}";

        await this.EnsureModuleAsync();

        // Global query filters need to inspect the existing body — a best-effort pre-check (advisory, not a
        // concurrency token). The authoritative existence + version check happens atomically below.
        if (this.options.ResolveQueryFilters(typeof(T)).Count > 0)
        {
            var existingJson = await IndexedDbJsInterop.Get(storeName, compositeKey);
            var existingDoc = existingJson == null
                ? null
                : Deserialize(JsonSerializer.Deserialize(existingJson, IndexedDbInteropJsonContext.Default.DocumentRecord)!.Data, typeInfo, this.jsonOptions);
            if (existingDoc == null || !this.PassesGlobalFilters(existingDoc))
                throw new InvalidOperationException(
                    $"No document of type '{typeName}' with Id '{id}' was found to update.");
        }

        // Compute the expected version and bump the in-memory copy before serializing. The atomic JS call
        // re-checks the stored version inside one transaction; on conflict we restore the caller's version.
        var checkVersion = versionMapping != null;
        var expectedVersion = versionMapping?.GetVersion(document) ?? 0;
        versionMapping?.SetVersion(document, expectedVersion + 1);

        var json = Serialize(document, typeInfo, this.jsonOptions);
        var now = DateTimeOffset.UtcNow.ToString("o");

        var record = new DocumentRecord
        {
            Key = compositeKey,
            Id = id,
            TypeName = typeName,
            Data = json,
            CreatedAt = now, // preserved from the existing row by the atomic JS call
            UpdatedAt = now
        };

        this.Log($"IndexedDB UPDATE {storeName} Id={id}");
        var outcome = await IndexedDbJsInterop.UpdateIfVersionMatches(
            storeName, SerializeRecord(record), checkVersion, expectedVersion, versionMapping?.JsonPath ?? "");

        if (outcome == "missing")
        {
            versionMapping?.SetVersion(document, expectedVersion);
            throw new InvalidOperationException(
                $"No document of type '{typeName}' with Id '{id}' was found to update.");
        }
        if (outcome.StartsWith("conflict:", StringComparison.Ordinal))
        {
            versionMapping?.SetVersion(document, expectedVersion);
            var storedVersion = int.TryParse(outcome.AsSpan("conflict:".Length), out var sv) ? sv : 0;
            throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
        }

        await this.AppendHistoryAsync<T>(id, typeName, TemporalOperation.Updated, json);
        await this.RunAfterWriteAsync(write.Context, id, versionMapping?.GetVersion(document), cancellationToken).ConfigureAwait(false);
    }

    public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("upsert", typeof(T).Name, () => this.UpsertImpl(patch, jsonTypeInfo, cancellationToken));

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "DocumentRecord is a simple internal DTO with string properties.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "DocumentRecord is a simple internal DTO with string properties.")]
    async Task UpsertImpl<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var write = await this.BeginWriteAsync(DocumentOperation.Upsert, patch, null, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
        if (!write.Proceed)
            return;
        var (typeInfo, typeName, versionMapping) = (write.TypeInfo, write.TypeName, write.VersionMapping);
        var accessor = write.Accessor;
        patch = write.Doc;

        if (accessor.IsDefaultId(patch))
            throw new InvalidOperationException(
                $"Upsert requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

        var id = accessor.GetIdAsString(patch);
        var storeName = this.ResolveStoreName<T>();
        var compositeKey = $"{typeName}:{id}";

        await this.EnsureModuleAsync();
        var existingJson = await IndexedDbJsInterop.Get(storeName, compositeKey);
        var now = DateTimeOffset.UtcNow.ToString("o");

        DocumentRecord record;
        if (existingJson == null)
        {
            versionMapping?.SetVersion(patch, 1);
            var patchJson = Serialize(patch, typeInfo, this.jsonOptions);
            patchJson = StripNullProperties(patchJson);

            record = new DocumentRecord
            {
                Key = compositeKey,
                Id = id,
                TypeName = typeName,
                Data = patchJson,
                CreatedAt = now,
                UpdatedAt = now
            };
            this.Log($"IndexedDB UPSERT (insert) {storeName} Id={id}");
        }
        else
        {
            var existing = JsonSerializer.Deserialize(existingJson, IndexedDbInteropJsonContext.Default.DocumentRecord)!;

            if (versionMapping != null)
            {
                var expectedVersion = versionMapping.GetVersion(patch);
                var storedNode = JsonNode.Parse(existing.Data)!.AsObject();
                var storedVersion = storedNode[versionMapping.JsonPath]?.GetValue<int>() ?? 0;
                if (expectedVersion > 0 && storedVersion != expectedVersion)
                    throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
                versionMapping.SetVersion(patch, storedVersion + 1);
            }

            var patchJson = Serialize(patch, typeInfo, this.jsonOptions);
            patchJson = StripNullProperties(patchJson);

            var merged = MergeJson(existing.Data, patchJson);
            record = new DocumentRecord
            {
                Key = compositeKey,
                Id = id,
                TypeName = typeName,
                Data = merged,
                CreatedAt = existing.CreatedAt,
                UpdatedAt = now
            };
            this.Log($"IndexedDB UPSERT (merge) {storeName} Id={id}");
        }

        await IndexedDbJsInterop.Put(storeName, SerializeRecord(record));
        await this.AppendHistoryAsync<T>(id, typeName, TemporalOperation.Updated, record.Data);
        await this.RunAfterWriteAsync(write.Context, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
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
        var storeName = this.ResolveStoreName<T>();
        var compositeKey = $"{typeName}:{resolvedId}";

        await this.EnsureModuleAsync();
        var existingJson = await IndexedDbJsInterop.Get(storeName, compositeKey);
        if (existingJson == null)
            return false;

        var existing = JsonSerializer.Deserialize(existingJson, IndexedDbInteropJsonContext.Default.DocumentRecord)!;
        var node = JsonNode.Parse(existing.Data)!.AsObject();
        SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));

        existing.Data = node.ToJsonString();
        existing.UpdatedAt = DateTimeOffset.UtcNow.ToString("o");

        this.Log($"IndexedDB SET PROPERTY {storeName} Id={resolvedId} Path={jsonPath}");
        await IndexedDbJsInterop.Put(storeName, SerializeRecord(existing));
        await this.AppendHistoryAsync<T>(resolvedId, typeName, TemporalOperation.Updated, existing.Data);
        return true;
    }

    public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("remove_property", typeof(T).Name, () => this.RemovePropertyImpl(id, property, jsonTypeInfo, cancellationToken), r => r ? 1 : 0);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "DocumentRecord is a simple internal DTO with string properties.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "DocumentRecord is a simple internal DTO with string properties.")]
    async Task<bool> RemovePropertyImpl<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var storeName = this.ResolveStoreName<T>();
        var compositeKey = $"{typeName}:{resolvedId}";

        await this.EnsureModuleAsync();
        var existingJson = await IndexedDbJsInterop.Get(storeName, compositeKey);
        if (existingJson == null)
            return false;

        var existing = JsonSerializer.Deserialize(existingJson, IndexedDbInteropJsonContext.Default.DocumentRecord)!;
        var node = JsonNode.Parse(existing.Data)!.AsObject();
        RemoveNestedProperty(node, jsonPath);

        existing.Data = node.ToJsonString();
        existing.UpdatedAt = DateTimeOffset.UtcNow.ToString("o");

        this.Log($"IndexedDB REMOVE PROPERTY {storeName} Id={resolvedId} Path={jsonPath}");
        await IndexedDbJsInterop.Put(storeName, SerializeRecord(existing));
        await this.AppendHistoryAsync<T>(resolvedId, typeName, TemporalOperation.Updated, existing.Data);
        return true;
    }

    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("get", typeof(T).Name, () => this.GetImpl(id, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "DocumentRecord is a simple internal DTO with string properties.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "DocumentRecord is a simple internal DTO with string properties.")]
    async Task<T?> GetImpl<T>(object id, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var storeName = this.ResolveStoreName<T>();
        var compositeKey = $"{typeName}:{resolvedId}";

        await this.EnsureModuleAsync();
        this.Log($"IndexedDB GET {storeName} Id={resolvedId}");
        var existingJson = await IndexedDbJsInterop.Get(storeName, compositeKey);
        if (existingJson == null)
            return null;

        var record = JsonSerializer.Deserialize(existingJson, IndexedDbInteropJsonContext.Default.DocumentRecord)!;
        var doc = Deserialize(record.Data, typeInfo, this.jsonOptions);
        if (doc != null && !this.PassesGlobalFilters(doc))
            return null;
        return doc;
    }

    public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("get_diff", typeof(T).Name, () => this.GetDiffImpl(id, modified, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "DocumentRecord is a simple internal DTO with string properties.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "DocumentRecord is a simple internal DTO with string properties.")]
    async Task<JsonPatchDocument<T>?> GetDiffImpl<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var storeName = this.ResolveStoreName<T>();
        var compositeKey = $"{typeName}:{resolvedId}";

        await this.EnsureModuleAsync();
        var existingJson = await IndexedDbJsInterop.Get(storeName, compositeKey);
        if (existingJson == null)
            return null;

        var record = JsonSerializer.Deserialize(existingJson, IndexedDbInteropJsonContext.Default.DocumentRecord)!;
        var doc = Deserialize(record.Data, typeInfo, this.jsonOptions);
        if (doc != null && !this.PassesGlobalFilters(doc))
            return null;
        var modifiedJson = Serialize(modified, typeInfo, this.jsonOptions);
        return JsonDiff.CreatePatch<T>(record.Data, modifiedJson, this.jsonOptions);
    }

    public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("IndexedDB does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload instead.");

    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("IndexedDB does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload instead.");

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("count", typeof(T).Name, () => this.CountImpl<T>(whereClause, parameters, cancellationToken), r => r);

    async Task<int> CountImpl<T>(string? whereClause, object? parameters, CancellationToken cancellationToken) where T : class
    {
        if (!string.IsNullOrWhiteSpace(whereClause))
            throw new NotSupportedException("IndexedDB does not support SQL WHERE clauses. Use the LINQ-based Query<T>() overload instead.");

        var typeName = this.ResolveTypeName<T>();
        var storeName = this.ResolveStoreName<T>();

        await this.EnsureModuleAsync();
        this.Log($"IndexedDB COUNT {storeName}");
        return await IndexedDbJsInterop.CountByTypeName(storeName, typeName);
    }

    public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("remove", typeof(T).Name, () => this.RemoveImpl<T>(id, cancellationToken), r => r ? 1 : 0);

    async Task<bool> RemoveImpl<T>(object id, CancellationToken cancellationToken) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var storeName = this.ResolveStoreName<T>();
        var compositeKey = $"{typeName}:{resolvedId}";

        var write = await this.BeginWriteAsync<T>(DocumentOperation.Delete, null, id, null, cancellationToken).ConfigureAwait(false);
        if (!write.Proceed)
            return write.CancelResult;

        await this.EnsureModuleAsync();

        if (this.options.ResolveQueryFilters(typeof(T)).Count > 0)
        {
            var existingJson = await IndexedDbJsInterop.Get(storeName, compositeKey);
            if (existingJson == null)
                return false;
            var record = JsonSerializer.Deserialize(existingJson, IndexedDbInteropJsonContext.Default.DocumentRecord)!;
            var doc = Deserialize<T>(record.Data, null, this.jsonOptions);
            if (doc == null || !this.PassesGlobalFilters(doc))
                return false;
        }

        this.Log($"IndexedDB DELETE {storeName} Id={resolvedId}");
        var removed = await IndexedDbJsInterop.Remove(storeName, compositeKey);
        if (removed)
        {
            await this.AppendHistoryAsync<T>(resolvedId, typeName, TemporalOperation.Removed, null);
            await this.RunAfterWriteAsync(write.Context, id, null, cancellationToken).ConfigureAwait(false);
        }
        return removed;
    }

    public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("clear", typeof(T).Name, () => this.ClearImpl<T>(cancellationToken), r => r);

    async Task<int> ClearImpl<T>(CancellationToken cancellationToken) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var storeName = this.ResolveStoreName<T>();

        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Clear, typeName);
        if (!await this.RunBeforeBulkAsync(bulkCtx, cancellationToken).ConfigureAwait(false))
            return bulkCtx!.CancelAffected;

        await this.EnsureModuleAsync();
        this.Log($"IndexedDB CLEAR {storeName}");

        int deleted;
        if (this.options.ResolveQueryFilters(typeof(T)).Count == 0)
        {
            deleted = await IndexedDbJsInterop.ClearByTypeName(storeName, typeName);
        }
        else
        {
            // Filters present — load each doc, evaluate, delete only matching ones.
            var docs = await this.LoadDocumentsAsync<T>(typeName, null);
            deleted = 0;
            foreach (var d in docs)
            {
                if (this.PassesGlobalFilters(d))
                {
                    var docId = this.idCache.GetOrCreate<T>(null).GetIdAsString(d);
                    if (await IndexedDbJsInterop.Remove(storeName, $"{typeName}:{docId}"))
                        deleted++;
                }
            }
        }
        await this.RunAfterBulkAsync(bulkCtx, deleted, cancellationToken).ConfigureAwait(false);
        return deleted;
    }

    /// <inheritdoc />

    Task IUnitOfWorkEngine.RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
        => this.Tracker.Track("transaction", "(transaction)", () => this.RunUnitAsyncImpl(work, cancellationToken));

    async Task RunUnitAsyncImpl(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        // IndexedDB transactions are auto-committed when all requests complete.
        // For simplicity, we run against self — operations are atomic at the individual put/delete level.
        // True multi-operation atomicity would require batching all ops in a single JS call.
        await work(this, cancellationToken);
    }

    // ── Internal helpers used by IndexedDbDocumentQuery ────────────────────

    internal async Task<IEnumerable<T>> LoadDocumentsAsync<T>(string typeName, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var storeName = this.options.ResolveStoreName(typeName);
        await this.EnsureModuleAsync();
        var records = DeserializeRecords(await IndexedDbJsInterop.GetAllByTypeName(storeName, typeName));

        var results = new List<T>();
        foreach (var record in records)
        {
            var obj = Deserialize(record.Data, typeInfo, this.jsonOptions);
            if (obj != null)
                results.Add(obj);
        }
        return results;
    }

    // ── Full-text search (in-memory TF-IDF fallback — IndexedDB has no native FTS) ──

    public bool SupportsFullText => true;

    public Task<IReadOnlyList<FullTextResult<T>>> FullTextSearch<T>(
        string searchText,
        int maxResults = 50,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("full_text_search", typeof(T).Name, () => this.FullTextSearchImpl(searchText, maxResults, filter, cancellationToken), r => r.Count);

    async Task<IReadOnlyList<FullTextResult<T>>> FullTextSearchImpl<T>(
        string searchText,
        int maxResults,
        Expression<Func<T, bool>>? filter,
        CancellationToken cancellationToken) where T : class
    {
        var mapping = this.options.ResolveFullTextMapping(typeof(T))
            ?? throw new InvalidOperationException(
                $"No full-text mapping is registered for '{typeof(T).Name}'. " +
                $"Call MapFullTextProperty<{typeof(T).Name}>(...) at startup.");
        ArgumentException.ThrowIfNullOrEmpty(searchText);
        if (maxResults <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxResults), "maxResults must be > 0.");

        var typeInfo = this.FindTypeInfo<T>(null);
        IEnumerable<T> items = await this.LoadDocumentsAsync(this.ResolveTypeName<T>(), typeInfo);
        if (filter != null)
        {
            var compiled = ExpressionInterpreter.Interpret(filter);
            items = items.Where(compiled);
        }

        var docs = items
            .Select(d => (Document: d, Text: FullTextMappingFactory.ExtractText(mapping, d)))
            .ToList();
        return InMemoryFullTextSearch.Rank(docs, searchText, maxResults);
    }

    internal async Task<int> DeleteDocumentsAsync<T>(string typeName, Func<T, bool> predicate, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var storeName = this.options.ResolveStoreName(typeName);
        await this.EnsureModuleAsync();
        var records = DeserializeRecords(await IndexedDbJsInterop.GetAllByTypeName(storeName, typeName));

        var keysToDelete = new List<string>();
        foreach (var record in records)
        {
            var obj = Deserialize(record.Data, typeInfo, this.jsonOptions);
            if (obj != null && predicate(obj))
                keysToDelete.Add(record.Key);
        }

        if (keysToDelete.Count > 0)
            await IndexedDbJsInterop.BatchDelete(storeName, keysToDelete.ToArray());

        return keysToDelete.Count;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    internal async Task<int> UpdateDocumentPropertyAsync<T>(
        string typeName,
        Func<T, bool> predicate,
        string jsonPath,
        object? value,
        JsonTypeInfo<T>? typeInfo) where T : class
    {
        var storeName = this.options.ResolveStoreName(typeName);
        await this.EnsureModuleAsync();
        var records = DeserializeRecords(await IndexedDbJsInterop.GetAllByTypeName(storeName, typeName));

        var updatedRecords = new List<DocumentRecord>();
        foreach (var record in records)
        {
            var obj = Deserialize(record.Data, typeInfo, this.jsonOptions);
            if (obj == null || !predicate(obj))
                continue;

            var node = JsonNode.Parse(record.Data)!.AsObject();
            SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));
            record.Data = node.ToJsonString();
            record.UpdatedAt = DateTimeOffset.UtcNow.ToString("o");
            updatedRecords.Add(record);
        }

        if (updatedRecords.Count > 0)
            await IndexedDbJsInterop.BatchPut(storeName, SerializeRecords(updatedRecords.ToArray()));

        return updatedRecords.Count;
    }

    /// <summary>Everything the shared query base needs from this store, built once per root query.</summary>
    internal DocumentQueryContext<T> BuildQueryContext<T>(JsonTypeInfo<T>? typeInfo) where T : class
        => new()
        {
            TypeName = this.ResolveTypeName<T>(),
            Tracker = this.Tracker,
            Interceptors = this.options.Interceptors,
            JsonOptions = this.jsonOptions,
            TypeInfo = typeInfo,
            Filters = QueryContextFilters.Resolve<T>(this.options.ResolveQueryFilters(typeof(T))),
            ApplyComputed = QueryContextFilters.ApplyComputed<T>(this.options.ResolveComputedMappings(typeof(T))),
            GetId = this.idCache.GetOrCreate(typeInfo).GetIdAsString,
            ComputedLookup = this.options.ResolveComputedLookup(typeof(T))
        };

    internal string ResolveTypeNameFor<T>() => this.ResolveTypeName<T>();

    internal JsonSerializerOptions JsonOptions => this.jsonOptions;
    internal InterceptorPipeline InterceptorPipeline => this.options.Interceptors;

    internal IndexedDbDocumentStoreOptions Options => this.options;

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

    public ValueTask DisposeAsync()
    {
        this.moduleLock.Dispose();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Synchronous disposal, required by <see cref="ITemporalDocumentStore"/>. Prefer
    /// <see cref="DisposeAsync"/>; this releases the same resources.
    /// </summary>
    public void Dispose()
    {
        this.moduleLock.Dispose();
        GC.SuppressFinalize(this);
    }
}
