using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;
using IRavenStore = Raven.Client.Documents.IDocumentStore;
using RavenClientStore = Raven.Client.Documents.DocumentStore;
using RavenConcurrencyException = Raven.Client.Exceptions.ConcurrencyException;

namespace Shiny.DocumentDb.RavenDb;

/// <summary>
/// RavenDB implementation of <see cref="IDocumentStore"/>. Every document — regardless of CLR type — is
/// stored as a <see cref="RavenDbDocument"/> envelope in the single <c>RavenDbDocuments</c> collection, with
/// the id <c>{typeName}/{logicalId}</c>. The caller's POCO is serialized with <b>our</b> System.Text.Json
/// pipeline into the opaque <see cref="RavenDbDocument.DataJson"/> string so RavenDB's own serializer never
/// reinterprets the body (round-trips stay deterministic and consistent with every other provider).
/// <para>
/// Because the body is opaque to RavenDB, LINQ queries evaluate <b>client-side</b>: a native id-prefix stream
/// (immediately consistent — no index staleness) pulls every document of a type, then predicates, ordering,
/// paging, projection, and aggregates run in memory via <see cref="ExpressionInterpreter"/> (the LiteDB/Dynamo
/// model). Reads and writes by id are ACID; optimistic concurrency is enforced with the mapped version field
/// backed by RavenDB change vectors.
/// </para>
/// </summary>
public partial class RavenDbDocumentStore : DocumentProviderBase, IDocumentStore, IDocumentMaintenance, IObservableDocumentStore, IUnitOfWorkEngine, IDisposable
{
    readonly RavenDbDocumentStoreOptions options;
    readonly IRavenStore ravenStore;
    readonly bool ownsStore;
    readonly JsonSerializerOptions jsonOptions;
    readonly IdAccessorCache idCache;
    // AsyncLocal so concurrent units of work and direct writes each buffer notifications independently.
    Action<string>? logging;

    /// <summary>Constructs the store and wires DI-registered interceptors from <paramref name="serviceProvider"/>
    /// (so container-registered <see cref="IDocumentInterceptor"/>s fire alongside options-registered ones).</summary>
    public RavenDbDocumentStore(RavenDbDocumentStoreOptions options, IServiceProvider serviceProvider) : this(options)
    {
        this.AttachServiceProvider(serviceProvider);
        this.logging = DocumentStoreLogging.Compose(this.logging, this.Logger);
    }

    public RavenDbDocumentStore(RavenDbDocumentStoreOptions options)
    {
        this.options = options;
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        this.logging = options.Logging;
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName, options.IdConverters);

        if (options.DocumentStore != null)
        {
            this.ravenStore = options.DocumentStore;
            this.ownsStore = false;
        }
        else
        {
            if (options.Urls == null || options.Urls.Length == 0)
                throw new InvalidOperationException("RavenDbDocumentStoreOptions requires either DocumentStore or Urls + Database.");
            if (string.IsNullOrWhiteSpace(options.Database))
                throw new InvalidOperationException("RavenDbDocumentStoreOptions.Database is required when DocumentStore is not supplied.");

            var store = new RavenClientStore
            {
                Urls = options.Urls,
                Database = options.Database,
                Certificate = options.Certificate
            };
            store.Initialize();
            this.ravenStore = store;
            this.ownsStore = true;
        }

        options.ResolveVersionJsonPaths(this.jsonOptions);
    }

    protected override InterceptorPipeline Interceptors => this.options.Interceptors;
    protected override DocumentMappingRegistry Mappings => this.options.Mappings;
    protected override IdAccessorCache IdCache => this.idCache;
    protected override JsonTypeInfo<T>? ResolveTypeInfo<T>(JsonTypeInfo<T>? provided) where T : class => this.FindTypeInfo(provided);
    protected override string ResolveDocumentTypeName<T>() where T : class => this.ResolveTypeName<T>();
    internal InterceptorPipeline InterceptorPipeline => this.options.Interceptors;
    internal JsonSerializerOptions JsonOptions => this.jsonOptions;
    internal RavenDbDocumentStoreOptions Options => this.options;
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
            GetId = this.idCache.GetOrCreate(typeInfo).GetIdAsString
        };

    internal string ResolveTypeNameFor<T>() => this.ResolveTypeName<T>();

    string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);

    IAsyncDocumentSession NewRavenSession() => this.ravenStore.OpenAsyncSession();

    // A write session compares change vectors on SaveChanges so a concurrent modification between our
    // load and save fails rather than silently last-write-wins (closes the version-check TOCTOU window).
    static void EnableOptimisticConcurrency(IAsyncDocumentSession session)
    {
#pragma warning disable CS0618 // UseOptimisticConcurrency is the stable 7.x knob; the replacement mode enum is newer.
        session.Advanced.UseOptimisticConcurrency = true;
#pragma warning restore CS0618
    }

    void Log(string message) => this.logging?.Invoke(message);

    public void Dispose()
    {
        if (this.ownsStore)
            this.ravenStore.Dispose();
    }

    // ── Serialization helpers ───────────────────────────────────────────

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

    static RavenDbDocument BuildWrapper(string id, string typeName, string dataJson, DateTime now, int? version, DateTime? createdAt = null)
        => new()
        {
            DocId = id,
            TypeName = typeName,
            DataJson = dataJson,
            CreatedAt = createdAt ?? now,
            UpdatedAt = now,
            Version = version
        };

    // ── Query filters ───────────────────────────────────────────────────

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

    bool StoredPassesFilters<T>(RavenDbDocument wrapper, JsonTypeInfo<T>? typeInfo) where T : class
    {
        if (this.options.ResolveQueryFilters(typeof(T)).Count == 0)
            return true;
        var doc = this.Materialize(wrapper.DataJson, typeInfo);
        return doc != null && this.PassesGlobalFilters(doc);
    }

    // ── Id generation ───────────────────────────────────────────────────

    async Task<string> GenerateIdAsync<T>(IdAccessor<T> accessor, string typeName, CancellationToken ct) where T : class
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
                await foreach (var wrapper in this.StreamWrappersAsync(typeName, ct).ConfigureAwait(false))
                {
                    if (long.TryParse(wrapper.DocId, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) && v > max)
                        max = v;
                }
                return (max + 1).ToString(CultureInfo.InvariantCulture);

            case IdKind.Custom:
                return accessor.GenerateOrThrow();

            default:
                throw new InvalidOperationException($"Unsupported Id kind: {accessor.Kind}");
        }
    }

    // ── Change notifications ────────────────────────────────────────────

    /// <inheritdoc />
    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange<T>(CancellationToken cancellationToken = default) where T : class
        => this.Broadcaster.Observe<T>(cancellationToken);

    // ── Streaming (immediately consistent id-prefix scan) ───────────────

    async IAsyncEnumerable<RavenDbDocument> StreamWrappersAsync(string typeName, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        using var session = this.NewRavenSession();
        await using var enumerator = await session.Advanced
            .StreamAsync<RavenDbDocument>(RavenDbDocument.Prefix(typeName), token: ct)
            .ConfigureAwait(false);
        while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            yield return enumerator.Current.Document;
    }

    internal async IAsyncEnumerable<T> LoadDocumentsAsync<T>(JsonTypeInfo<T>? typeInfo, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        await foreach (var wrapper in this.StreamWrappersAsync(typeName, ct).ConfigureAwait(false))
        {
            var doc = this.Materialize(wrapper.DataJson, typeInfo);
            if (doc != null)
                yield return doc;
        }
    }

    // ── IDocumentStore ──────────────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        return new RavenDbDocumentQuery<T>(this, typeInfo);
    }

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

        var id = await this.ResolveInsertIdAsync(write, accessor => this.GenerateIdAsync(accessor, typeName, cancellationToken));

        versionMapping?.SetVersion(document, 1);
        var preparedBlobs = this.PrepareBlobs(document);
        var json = Serialize(document, typeInfo, this.jsonOptions);
        var wrapper = BuildWrapper(id, typeName, json, DateTime.UtcNow, versionMapping?.GetVersion(document));

        this.Log($"RavenDB INSERT {typeName}/{id}");
        using var session = this.NewRavenSession();
        EnableOptimisticConcurrency(session);
        // An empty change vector enforces insert-only: a concurrent/duplicate write fails on SaveChanges.
        await session.StoreAsync(wrapper, changeVector: "", id: RavenDbDocument.RavenId(typeName, id), token: cancellationToken).ConfigureAwait(false);
        this.AttachInSession<T>(session, RavenDbDocument.RavenId(typeName, id), null, preparedBlobs, prune: false);
        try
        {
            await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RavenConcurrencyException ex)
        {
            throw new InvalidOperationException($"A document of type '{typeName}' with Id '{id}' already exists.", ex);
        }

        await this.CompleteWriteAsync(write, id, versionMapping?.GetVersion(document) ?? 1, DocumentChangeType.Inserted, document, cancellationToken).ConfigureAwait(false);
    }

    public Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("batch_insert", typeof(T).Name, () => this.BatchInsertImpl(documents, jsonTypeInfo, cancellationToken), r => r);

    async Task<int> BatchInsertImpl<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var typeName = this.ResolveTypeName<T>();
        var versionMapping = this.options.ResolveVersionMapping(typeof(T));

        var docList = documents as IReadOnlyList<T> ?? documents.ToList();

        DocumentWriteContext[]? ctxs = null;
        if (this.HasPerDocInterceptors)
        {
            var mutable = docList.ToList();
            ctxs = await this.RunBeforeWriteBatchAsync(mutable, typeName, cancellationToken).ConfigureAwait(false);
            docList = mutable;
        }

        if (docList.Count == 0)
            return 0;

        var now = DateTime.UtcNow;
        var ids = new List<string>(docList.Count);
        long nextInt = -1;

        using var session = this.NewRavenSession();
        EnableOptimisticConcurrency(session);

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
                        var seed = await this.GenerateIdAsync(accessor, typeName, cancellationToken).ConfigureAwait(false);
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
                    id = await this.GenerateIdAsync(accessor, typeName, cancellationToken).ConfigureAwait(false);
                }
                accessor.SetId(document, id);
            }
            else
            {
                id = accessor.GetIdAsString(document);
            }

            versionMapping?.SetVersion(document, 1);
            var json = Serialize(document, typeInfo, this.jsonOptions);
            var wrapper = BuildWrapper(id, typeName, json, now, versionMapping?.GetVersion(document));
            await session.StoreAsync(wrapper, changeVector: "", id: RavenDbDocument.RavenId(typeName, id), token: cancellationToken).ConfigureAwait(false);
            ids.Add(id);
        }

        this.Log($"RavenDB BATCH INSERT {typeName} ({ids.Count})");
        try
        {
            await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RavenConcurrencyException ex)
        {
            throw new InvalidOperationException($"A document of type '{typeName}' has a duplicate Id in the batch.", ex);
        }

        for (var i = 0; i < ids.Count; i++)
        {
            this.PublishChange(DocumentChangeType.Inserted, ids[i], docList[i]);
            if (ctxs != null)
                await this.RunAfterWriteAsync(ctxs[i], ids[i], versionMapping?.GetVersion(docList[i]) ?? 1, cancellationToken).ConfigureAwait(false);
        }

        return ids.Count;
    }

    public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("update", typeof(T).Name, () => this.UpdateImpl(document, jsonTypeInfo, cancellationToken));

    async Task UpdateImpl<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var write = await this.BeginWriteAsync(DocumentOperation.Update, document, null, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
        if (!write.Proceed)
            return;
        var (typeInfo, typeName, versionMapping) = (write.TypeInfo, write.TypeName, write.VersionMapping);
        var accessor = write.Accessor;
        document = write.Doc;

        var id = this.RequireDocumentId(write);
        var ravenId = RavenDbDocument.RavenId(typeName, id);

        using var session = this.NewRavenSession();
        EnableOptimisticConcurrency(session);

        var wrapper = await session.LoadAsync<RavenDbDocument>(ravenId, cancellationToken).ConfigureAwait(false);
        if (wrapper == null || !this.StoredPassesFilters<T>(wrapper, typeInfo))
            throw new InvalidOperationException($"No document of type '{typeName}' with Id '{id}' was found to update.");

        var expectedVersion = 0;
        if (versionMapping != null)
        {
            expectedVersion = versionMapping.GetVersion(document);
            var storedVersion = wrapper.Version ?? 0;
            if (storedVersion != expectedVersion)
                throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
            versionMapping.SetVersion(document, expectedVersion + 1);
        }

        var preparedBlobs = this.PrepareBlobs(document);
        var json = Serialize(document, typeInfo, this.jsonOptions);
        wrapper.DataJson = json;
        wrapper.UpdatedAt = DateTime.UtcNow;
        wrapper.Version = versionMapping?.GetVersion(document);
        this.AttachInSession<T>(session, ravenId, wrapper, preparedBlobs, prune: true);

        this.Log($"RavenDB UPDATE {typeName}/{id}");
        try
        {
            await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RavenConcurrencyException) when (versionMapping != null)
        {
            throw new ConcurrencyException(typeName, id, expectedVersion);
        }

        await this.CompleteWriteAsync(write, id, versionMapping?.GetVersion(document), DocumentChangeType.Updated, document, cancellationToken).ConfigureAwait(false);
    }

    public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("upsert", typeof(T).Name, () => this.UpsertImpl(patch, jsonTypeInfo, cancellationToken));

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
                $"Upsert requires a non-default Id on the document. Set the Id property on '{typeof(T).Name}' before calling Upsert.");

        var id = accessor.GetIdAsString(patch);
        var ravenId = RavenDbDocument.RavenId(typeName, id);
        var now = DateTime.UtcNow;

        using var session = this.NewRavenSession();
        EnableOptimisticConcurrency(session);

        var wrapper = await session.LoadAsync<RavenDbDocument>(ravenId, cancellationToken).ConfigureAwait(false);

        if (wrapper == null)
        {
            versionMapping?.SetVersion(patch, 1);
            var freshJson = StripNullProperties(Serialize(patch, typeInfo, this.jsonOptions));
            var fresh = BuildWrapper(id, typeName, freshJson, now, versionMapping?.GetVersion(patch));
            this.Log($"RavenDB UPSERT (insert) {typeName}/{id}");
            await session.StoreAsync(fresh, changeVector: "", id: ravenId, token: cancellationToken).ConfigureAwait(false);
            try
            {
                await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (RavenConcurrencyException ex)
            {
                throw new InvalidOperationException($"A document of type '{typeName}' with Id '{id}' was inserted concurrently.", ex);
            }
            this.PublishChange(DocumentChangeType.Inserted, id, patch);
            await this.RunAfterWriteAsync(write.Context, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
            return;
        }

        var guardVersion = 0;
        if (versionMapping != null)
        {
            var expectedVersion = versionMapping.GetVersion(patch);
            var storedVersion = wrapper.Version ?? 0;
            if (expectedVersion > 0 && storedVersion != expectedVersion)
                throw new ConcurrencyException(typeName, id, expectedVersion, storedVersion);
            versionMapping.SetVersion(patch, storedVersion + 1);
            guardVersion = expectedVersion;
        }

        var patchJson = StripNullProperties(Serialize(patch, typeInfo, this.jsonOptions));
        var preparedBlobs = this.PrepareBlobs(patch);
        var merged = MergeJson(wrapper.DataJson, patchJson);
        this.AttachInSession<T>(session, ravenId, wrapper, preparedBlobs, prune: false);
        wrapper.DataJson = merged;
        wrapper.UpdatedAt = now;
        wrapper.Version = versionMapping?.GetVersion(patch);

        this.Log($"RavenDB UPSERT (merge) {typeName}/{id}");
        try
        {
            await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RavenConcurrencyException) when (versionMapping != null && guardVersion > 0)
        {
            throw new ConcurrencyException(typeName, id, guardVersion);
        }

        await this.CompleteWriteAsync(write, id, versionMapping?.GetVersion(patch), DocumentChangeType.Updated, patch, cancellationToken).ConfigureAwait(false);
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
        var ravenId = RavenDbDocument.RavenId(typeName, resolvedId);

        using var session = this.NewRavenSession();
        var wrapper = await session.LoadAsync<RavenDbDocument>(ravenId, cancellationToken).ConfigureAwait(false);
        if (wrapper == null || !this.StoredPassesFilters<T>(wrapper, typeInfo))
            return false;

        var node = JsonNode.Parse(wrapper.DataJson)!.AsObject();
        SetNestedProperty(node, jsonPath, value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions)));
        wrapper.DataJson = node.ToJsonString();
        wrapper.UpdatedAt = DateTime.UtcNow;

        this.Log($"RavenDB SET PROPERTY {typeName}/{resolvedId} Path={jsonPath}");
        await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
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
        var ravenId = RavenDbDocument.RavenId(typeName, resolvedId);

        using var session = this.NewRavenSession();
        var wrapper = await session.LoadAsync<RavenDbDocument>(ravenId, cancellationToken).ConfigureAwait(false);
        if (wrapper == null || !this.StoredPassesFilters<T>(wrapper, typeInfo))
            return false;

        var node = JsonNode.Parse(wrapper.DataJson)!.AsObject();
        RemoveNestedProperty(node, jsonPath);
        wrapper.DataJson = node.ToJsonString();
        wrapper.UpdatedAt = DateTime.UtcNow;

        this.Log($"RavenDB REMOVE PROPERTY {typeName}/{resolvedId} Path={jsonPath}");
        await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("get", typeof(T).Name, () => this.GetImpl(id, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    async Task<T?> GetImpl<T>(object id, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var ravenId = RavenDbDocument.RavenId(typeName, resolvedId);

        this.Log($"RavenDB GET {typeName}/{resolvedId}");
        using var session = this.NewRavenSession();
        var wrapper = await session.LoadAsync<RavenDbDocument>(ravenId, cancellationToken).ConfigureAwait(false);
        if (wrapper == null)
            return null;

        var doc = this.Materialize(wrapper.DataJson, typeInfo);
        if (doc != null && !this.PassesGlobalFilters(doc))
            return null;
        return doc;
    }

    public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("get_diff", typeof(T).Name, () => this.GetDiffImpl(id, modified, jsonTypeInfo, cancellationToken));

    async Task<JsonPatchDocument<T>?> GetDiffImpl<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = this.FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var ravenId = RavenDbDocument.RavenId(typeName, resolvedId);

        using var session = this.NewRavenSession();
        var wrapper = await session.LoadAsync<RavenDbDocument>(ravenId, cancellationToken).ConfigureAwait(false);
        if (wrapper == null || !this.StoredPassesFilters<T>(wrapper, typeInfo))
            return null;

        var modifiedJson = Serialize(modified, typeInfo, this.jsonOptions);
        return JsonDiff.CreatePatch<T>(wrapper.DataJson, modifiedJson, this.jsonOptions);
    }

    public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException(
            "RavenDB stores document bodies as opaque JSON, so a SQL/RQL WHERE clause over document fields is not supported. Use the LINQ-based Query<T>() overload instead.");

    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException(
            "RavenDB stores document bodies as opaque JSON, so a SQL/RQL WHERE clause over document fields is not supported. Use the LINQ-based Query<T>() overload instead.");

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("count", typeof(T).Name, () => this.CountImpl<T>(whereClause, parameters, cancellationToken), r => r);

    async Task<int> CountImpl<T>(string? whereClause, object? parameters, CancellationToken cancellationToken) where T : class
    {
        if (!string.IsNullOrWhiteSpace(whereClause))
            throw new NotSupportedException(
                "RavenDB does not support SQL/RQL WHERE clauses over document fields. Use the LINQ-based Query<T>() overload instead.");

        var typeName = this.ResolveTypeName<T>();
        var typeInfo = this.FindTypeInfo<T>(null);
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        this.Log($"RavenDB COUNT {typeName}");
        var count = 0;
        await foreach (var wrapper in this.StreamWrappersAsync(typeName, cancellationToken).ConfigureAwait(false))
        {
            if (!hasFilters)
            {
                count++;
            }
            else
            {
                var doc = this.Materialize(wrapper.DataJson, typeInfo);
                if (doc != null && this.PassesGlobalFilters(doc))
                    count++;
            }
        }
        return count;
    }

    public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("remove", typeof(T).Name, () => this.RemoveImpl<T>(id, cancellationToken), r => r ? 1 : 0);

    async Task<bool> RemoveImpl<T>(object id, CancellationToken cancellationToken) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var typeName = this.ResolveTypeName<T>();
        var ravenId = RavenDbDocument.RavenId(typeName, resolvedId);

        var write = await this.BeginWriteAsync<T>(DocumentOperation.Delete, null, id, null, cancellationToken).ConfigureAwait(false);
        if (!write.Proceed)
            return write.CancelResult;

        using var session = this.NewRavenSession();
        var wrapper = await session.LoadAsync<RavenDbDocument>(ravenId, cancellationToken).ConfigureAwait(false);
        if (wrapper == null || !this.StoredPassesFilters<T>(wrapper, null))
            return false;

        this.Log($"RavenDB DELETE {typeName}/{resolvedId}");
        session.Delete(wrapper);
        await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);

        this.PublishChange<T>(DocumentChangeType.Removed, resolvedId, null);
        await this.RunAfterWriteAsync(write.Context, id, null, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
        => this.Tracker.Track("clear", typeof(T).Name, () => this.ClearImpl<T>(cancellationToken), r => r);

    async Task<int> ClearImpl<T>(CancellationToken cancellationToken) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var typeInfo = this.FindTypeInfo<T>(null);
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Clear, typeName);
        if (!await this.RunBeforeBulkAsync(bulkCtx, cancellationToken).ConfigureAwait(false))
            return bulkCtx!.CancelAffected;

        var toDelete = new List<string>();
        await foreach (var wrapper in this.StreamWrappersAsync(typeName, cancellationToken).ConfigureAwait(false))
        {
            var include = true;
            if (hasFilters)
            {
                var doc = this.Materialize(wrapper.DataJson, typeInfo);
                include = doc != null && this.PassesGlobalFilters(doc);
            }
            if (include)
                toDelete.Add(RavenDbDocument.RavenId(typeName, wrapper.DocId));
        }

        this.Log($"RavenDB CLEAR {typeName} ({toDelete.Count})");
        if (toDelete.Count > 0)
        {
            using var session = this.NewRavenSession();
            foreach (var rid in toDelete)
                session.Delete(rid);
            await session.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        }

        this.PublishChange<T>(DocumentChangeType.Cleared, "", null);
        await this.RunAfterBulkAsync(bulkCtx, toDelete.Count, cancellationToken).ConfigureAwait(false);
        return toDelete.Count;
    }

    // ── IDocumentMaintenance ────────────────────────────────────────────

    /// <inheritdoc />
    public async Task ClearAll(CancellationToken cancellationToken = default)
    {
        // Whole-store wipe: enumerate the single collection (index-backed; wait for non-stale) and delete all.
        List<string> ids;
        using (var session = this.NewRavenSession())
        {
            var wrappers = await session
                .Query<RavenDbDocument>()
                .Customize(x => x.WaitForNonStaleResults())
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);
            ids = wrappers
                .Select(w => RavenDbDocument.RavenId(w.TypeName, w.DocId))
                .ToList();
        }

        if (ids.Count == 0)
            return;

        using var deleteSession = this.NewRavenSession();
        foreach (var id in ids)
            deleteSession.Delete(id);
        await deleteSession.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
    }

    // ── IUnitOfWorkEngine (compensating pattern) ────────────────────────

    Task IUnitOfWorkEngine.RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
        => this.Tracker.Track("transaction", "(transaction)", () => this.RunUnitImpl(work, cancellationToken));

    async Task RunUnitImpl(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        // RavenDB has a genuine ACID session, but to keep interceptor / change-notification semantics identical
        // to the other document providers we use the shared compensating pattern (tracked inserts are removed on
        // rollback; update/upsert/remove delegate to the live store). In-process change notifications are buffered
        // and only emitted once the unit succeeds.
        var tracker = new RavenDbCompensatingStore(this);
        var buffer = new List<Action>();
        this.PendingChanges.Value = buffer;
        try
        {
            await work(tracker, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await tracker.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
        finally
        {
            this.PendingChanges.Value = null;
        }
        foreach (var emit in buffer)
            emit();
    }

    // ── Internal query helpers (used by RavenDbDocumentQuery) ───────────

    internal async Task<int> DeleteWhereAsync<T>(Func<T, bool> predicate, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var toDelete = new List<string>();
        await foreach (var wrapper in this.StreamWrappersAsync(typeName, ct).ConfigureAwait(false))
        {
            var doc = this.Materialize(wrapper.DataJson, typeInfo);
            if (doc != null && predicate(doc))
                toDelete.Add(RavenDbDocument.RavenId(typeName, wrapper.DocId));
        }
        if (toDelete.Count == 0)
            return 0;

        using var session = this.NewRavenSession();
        foreach (var rid in toDelete)
            session.Delete(rid);
        await session.SaveChangesAsync(ct).ConfigureAwait(false);
        return toDelete.Count;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Value serialization uses reflection when type is unknown.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Value serialization uses reflection when type is unknown.")]
    internal async Task<int> UpdatePropertyWhereAsync<T>(Func<T, bool> predicate, string jsonPath, object? value, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var matchedIds = new List<string>();
        await foreach (var wrapper in this.StreamWrappersAsync(typeName, ct).ConfigureAwait(false))
        {
            var doc = this.Materialize(wrapper.DataJson, typeInfo);
            if (doc != null && predicate(doc))
                matchedIds.Add(RavenDbDocument.RavenId(typeName, wrapper.DocId));
        }
        if (matchedIds.Count == 0)
            return 0;

        var now = DateTime.UtcNow;
        var valueNode = value == null ? null : JsonNode.Parse(JsonSerializer.Serialize(value, this.jsonOptions));

        using var session = this.NewRavenSession();
        var count = 0;
        foreach (var rid in matchedIds)
        {
            var wrapper = await session.LoadAsync<RavenDbDocument>(rid, ct).ConfigureAwait(false);
            if (wrapper != null)
            {
                var node = JsonNode.Parse(wrapper.DataJson)!.AsObject();
                SetNestedProperty(node, jsonPath, valueNode?.DeepClone());
                wrapper.DataJson = node.ToJsonString();
                wrapper.UpdatedAt = now;
                count++;
            }
        }
        await session.SaveChangesAsync(ct).ConfigureAwait(false);
        return count;
    }

    // ── JSON helpers ────────────────────────────────────────────────────

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
        var found = true;
        for (var i = 0; i < parts.Length - 1 && found; i++)
        {
            if (current[parts[i]] is JsonObject child)
                current = child;
            else
                found = false;
        }
        if (found)
            current.Remove(parts[^1]);
    }

    // ── Compensating transaction wrapper ────────────────────────────────

    sealed class RavenDbCompensatingStore(RavenDbDocumentStore inner) : CompensatingStore
    {
        protected override IDocumentStore Inner => inner;

        public override async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default)
        {
            await inner.Insert(document, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            var accessor = inner.idCache.GetOrCreate(inner.FindTypeInfo(jsonTypeInfo));
            this.TrackInsert(inner.ResolveTypeNameFor<T>(), accessor.GetIdAsString(document));
        }

        protected override async Task DeleteTrackedAsync(string typeName, string id, CancellationToken ct)
        {
            using var session = inner.NewRavenSession();
            session.Delete(RavenDbDocument.RavenId(typeName, id));
            await session.SaveChangesAsync(ct).ConfigureAwait(false);
        }
    }
}
