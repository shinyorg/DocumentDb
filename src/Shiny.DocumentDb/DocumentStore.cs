using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

public partial class DocumentStore : IDocumentStore, ITemporalDocumentStore, IObservableDocumentStore, IChangeFeedDocumentStore, IDocumentMaintenance, IQueryExecutor, IUnitOfWorkEngine, IDisposable
{
    // Shared-connection mode (SQLite-style): one long-lived connection serialized by the semaphore.
    // Pooled mode (server SQL / DuckDB): per-op connections, no semaphore.
    readonly bool sharedMode;
    readonly SemaphoreSlim? sharedSemaphore;
    readonly DbConnection? sharedConnection;
    bool sharedConnectionInitialized;

    readonly DocumentStoreOptions options;
    readonly IDatabaseProvider provider;
    readonly JsonSerializerOptions jsonOptions;
    // Not readonly: the SP-taking ctor recomposes it to fan out to ILogger. The container-free ctor leaves it as
    // the raw options callback. Everything downstream (TransactionalDocumentStore, static helpers, IQueryExecutor)
    // is threaded this single delegate, so composing here routes ALL SQL logging through ILogger for free.
    Action<string>? logging;
    readonly ILogger? logger;
    readonly Func<string>? tenantIdAccessor;
    readonly IdAccessorCache idCache;
    // Fallback-only: opens a fresh child scope per unit when DI interceptors are registered and no ambient
    // DocumentContext scope is flowing. Null on the container-free `new DocumentStore(options)` path.
    readonly IServiceScopeFactory? scopeFactory;
    // Embedded OpenTelemetry: always present, zero-cost when unobserved (see Diagnostics/DocumentStoreMetrics).
    readonly Diagnostics.OperationTracker tracker;
    readonly ChangeBroadcaster broadcaster = new();
    // Lazy<Task> guarantees table init runs exactly once per table per process, lock-free on the hot path.
    readonly ConcurrentDictionary<string, Lazy<Task>> tableInitTasks = new(StringComparer.OrdinalIgnoreCase);

    public IDatabaseProvider DatabaseProvider => this.provider;

    /// <inheritdoc />
    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange<T>(CancellationToken cancellationToken = default) where T : class
        => this.broadcaster.Observe<T>(cancellationToken);

    void PublishChange<T>(DocumentChangeType changeType, string id, T? document) where T : class
    {
        if (this.broadcaster.HasSubscribers<T>())
            this.broadcaster.Publish(new DocumentChange<T> { ChangeType = changeType, Id = id, Document = document });
    }

    /// <inheritdoc />
    public async Task<IAsyncDisposable> SubscribeChanges<T>(
        Func<DocumentChange<T>, CancellationToken, Task> onChange,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(onChange);
        if (!this.provider.SupportsChangeFeed)
            throw new NotSupportedException(
                $"The configured provider '{this.provider.GetType().Name}' does not support native change feeds.");

        var typeInfo = FindTypeInfo<T>(null);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();

        // Ensure the backing table exists before the provider provisions triggers / change tracking.
        await this.ExecuteAsync(tableName, _ => Task.CompletedTask, cancellationToken).ConfigureAwait(false);

        return await this.provider.SubscribeChangesAsync(
            tableName,
            typeName,
            async (raw, ct) =>
            {
                var document = raw.Json != null
                    ? DeserializeDocument(raw.Json, typeInfo, this.jsonOptions)
                    : null;
                await onChange(
                    new DocumentChange<T> { ChangeType = raw.ChangeType, Id = raw.Id, Document = document },
                    ct).ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    public DocumentStore(DocumentStoreOptions options)
    {
        this.options = options;
        this.provider = options.DatabaseProvider;
        this.jsonOptions = options.JsonSerializerOptions ?? CreateDefaultJsonOptions(options.UseReflectionFallback);
        this.logging = options.Logging;
        this.tenantIdAccessor = options.TenantIdAccessor;
        this.sharedMode = this.provider.RequiresSingleConnection;
        if (this.sharedMode)
        {
            this.sharedSemaphore = new SemaphoreSlim(1, 1);
            this.sharedConnection = this.provider.CreateConnection();
        }
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName, options.IdConverters);
        this.tracker = new Diagnostics.OperationTracker(Diagnostics.OperationTracker.SystemName(this.provider), options.StoreName);
        options.ResolveVersionJsonPaths(this.jsonOptions);
        options.ResolveSpatialJsonPaths(this.jsonOptions);
        options.ResolveVectorJsonPaths(this.jsonOptions);
        options.Mappings.ResolveVectorIndexKinds(VectorIndexKind.Hnsw);
        options.ResolveFullTextJsonPaths(this.jsonOptions);
        options.ResolveComputedJsonNames(this.jsonOptions);
        options.ResolveComputedColumns(this.provider);
        DocumentConfigurationValidator.Validate(options);
    }

    /// <summary>
    /// Constructs the store and wires DI-registered interceptors. Resolves
    /// <c>IEnumerable&lt;IDocumentInterceptor&gt;</c> / <c>IEnumerable&lt;IDocumentBulkInterceptor&gt;</c> from
    /// <paramref name="serviceProvider"/> so container-registered interceptors run alongside the ones registered
    /// on <see cref="DocumentStoreOptions"/> (options-registered first, DI-registered after).
    /// </summary>
    public DocumentStore(DocumentStoreOptions options, IServiceProvider serviceProvider) : this(options)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        options.Interceptors.AttachServiceProvider(serviceProvider);
        this.scopeFactory = serviceProvider.GetService(typeof(IServiceScopeFactory)) as IServiceScopeFactory;

        // Structured logging: resolve an ILogger from the container and fan the SQL log out to it (Debug), in
        // addition to the raw options.Logging callback. Controlled by the "Shiny.DocumentDb" log category — no
        // logging factory registered (or Debug disabled) ⇒ the callback-only behavior is unchanged.
        this.logger = DocumentStoreLogging.CreateLogger(serviceProvider);
        if (this.logger != null)
        {
            this.logging = DocumentStoreLogging.Compose(options.Logging, this.logger);
            if (this.logger.IsEnabled(LogLevel.Debug))
                this.logger.LogDebug("Shiny.DocumentDb store initialized (provider {Provider}, shared-connection {SharedMode})",
                    this.provider.GetType().Name, this.sharedMode);
        }
    }

    public bool SupportsSpatial => this.provider.SupportsSpatial;
    public bool SupportsVector => this.provider.SupportsVector;

    void Log(string sql) => this.logging?.Invoke(sql);

    string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);

    string ResolveTableName<T>() => this.options.ResolveTableName(this.ResolveTypeName<T>());

    string Qt(string tableName) => this.provider.QuoteTable(tableName);

    Internal.VersionMapping? ResolveVersionMapping<T>() => this.options.ResolveVersionMapping(typeof(T));

    string? GetTenantFilter() => this.tenantIdAccessor != null ? " AND TenantId = @tenantId" : null;

    void AddTenantParam(DbCommand cmd)
    {
        if (this.tenantIdAccessor != null)
            AddParameter(cmd, "@tenantId", this.tenantIdAccessor());
    }

    // Injects the tenant predicate into a history-sidecar query (both the per-id WHERE and the fleet-wide
    // WHERE TypeName forms) so temporal reads are tenant-scoped like the main-table reads. No-op when
    // multi-tenancy isn't configured. Callers must also AddTenantParam.
    string ApplyTenantToHistorySql(string sql)
        => this.tenantIdAccessor == null
            ? sql
            : sql
                .Replace("WHERE Id = @id AND TypeName = @typeName", "WHERE Id = @id AND TypeName = @typeName AND TenantId = @tenantId")
                .Replace("WHERE TypeName = @typeName", "WHERE TypeName = @typeName AND TenantId = @tenantId");

    /// <summary>
    /// Translates registered global query filters for <typeparamref name="T"/> and appends them
    /// to <paramref name="cmd"/>'s <c>CommandText</c> as <c>AND ({filter})</c>. No-op when no
    /// filters are registered. Trims a trailing semicolon and re-appends it.
    /// </summary>
    void AppendGlobalFilters<T>(DbCommand cmd, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var filters = this.options.ResolveQueryFilters(typeof(T));
        if (filters.Count == 0)
            return;

        var info = this.FindTypeInfo(typeInfo)
            ?? throw new InvalidOperationException(
                $"Global query filters for '{typeof(T).Name}' require a JsonTypeInfo<{typeof(T).Name}>. " +
                "Configure a JsonSerializerContext via DocumentStoreOptions.JsonSerializerOptions, " +
                "or pass JsonTypeInfo<T> explicitly.");

        var predicates = filters.Select(f => (Expression<Func<T, bool>>)f.Predicate).ToList();
        var combined = DocumentQuery<T>.CombinePredicates(predicates);
        var (clause, parms) = JsonExpressionVisitor.Translate(combined, info, this.provider);

        var sql = cmd.CommandText.TrimEnd();
        var hasTrailingSemicolon = sql.EndsWith(';');
        if (hasTrailingSemicolon)
            sql = sql.Substring(0, sql.Length - 1).TrimEnd();
        cmd.CommandText = sql + $" AND ({clause})" + (hasTrailingSemicolon ? ";" : "");
        foreach (var kv in parms)
            AddParameter(cmd, kv.Key, kv.Value);
    }

    JsonTypeInfo<T>? FindTypeInfo<T>(JsonTypeInfo<T>? provided)
        => FindTypeInfo(provided, this.jsonOptions, this.options.UseReflectionFallback);

    async Task EnsureSharedConnectionInitializedAsync(CancellationToken ct)
    {
        if (this.sharedConnectionInitialized)
            return;

        await this.sharedConnection!.OpenAsync(ct).ConfigureAwait(false);
        await this.provider.InitializeConnectionAsync(this.sharedConnection, ct).ConfigureAwait(false);
        if (this.provider.SupportsVector && this.options.Mappings.VectorMappings.Count > 0)
            await this.provider.LoadVectorExtensionAsync(this.sharedConnection, ct).ConfigureAwait(false);
        this.sharedConnectionInitialized = true;
    }

    async Task EnsureTableInitializedAsync(DocumentStoreSession session, string tableName, CancellationToken ct)
    {
        if (this.options.SkipTableInitialization)
            return;

        Lazy<Task>? lazy = null;
        try
        {
            lazy = this.tableInitTasks.GetOrAdd(tableName,
                _ => new Lazy<Task>(() => InitAsync(), LazyThreadSafetyMode.ExecutionAndPublication));
            await lazy.Value.ConfigureAwait(false);
        }
        catch
        {
            // Drop the cached failed task so the next call gets a clean retry.
            if (lazy != null)
                ((ICollection<KeyValuePair<string, Lazy<Task>>>)this.tableInitTasks)
                    .Remove(new KeyValuePair<string, Lazy<Task>>(tableName, lazy));
            throw;
        }

        async Task InitAsync()
        {
            await using var createCmd = session.CreateCommand();
            createCmd.CommandText = this.provider.BuildCreateTableSql(tableName);
            this.Log(createCmd.CommandText);
            await createCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

            await using var indexCmd = session.CreateCommand();
            indexCmd.CommandText = this.provider.BuildCreateTypenameIndexSql(tableName);
            this.Log(indexCmd.CommandText);
            try
            {
                await indexCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Index may already exist — safe to ignore
            }

            // Create tenant column and index if multi-tenancy is enabled
            if (this.tenantIdAccessor != null)
            {
                try
                {
                    await using var tenantColCmd = session.CreateCommand();
                    tenantColCmd.CommandText = this.provider.BuildAddTenantColumnSql(tableName);
                    this.Log(tenantColCmd.CommandText);
                    await tenantColCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Column may already exist — safe to ignore
                }

                await using var tenantIdxCmd = session.CreateCommand();
                tenantIdxCmd.CommandText = this.provider.BuildCreateTenantIndexSql(tableName);
                this.Log(tenantIdxCmd.CommandText);
                try
                {
                    await tenantIdxCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Index may already exist — safe to ignore
                }
            }

            // Create spatial sidecar tables if provider supports it and any spatial mappings exist
            var spatialSql = this.provider.BuildCreateSpatialTablesSql(tableName);
            if (spatialSql != null && this.options.Mappings.SpatialMappings.Count > 0)
            {
                await using var spatialCmd = session.CreateCommand();
                spatialCmd.CommandText = spatialSql;
                this.Log(spatialCmd.CommandText);
                await spatialCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            // Create vector sidecar tables for every mapped vector type using this documents table.
            if (this.provider.SupportsVector)
            {
                foreach (var mapping in this.options.Mappings.VectorMappings.Values)
                {
                    var typeName = TypeNameResolver.Resolve(mapping.DocumentType, this.options.TypeNameResolution);
                    if (this.options.ResolveTableName(typeName) != tableName)
                        continue;

                    var sql = this.provider.BuildCreateVectorTablesSql(tableName, typeName, mapping);
                    if (sql == null) continue;
                    await using var vecCmd = session.CreateCommand();
                    vecCmd.CommandText = sql;
                    this.Log(vecCmd.CommandText);
                    await vecCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
            }

            // Create the full-text index (FTS virtual table + triggers, generated column + index, …)
            // for every mapped full-text type using this documents table. The engine maintains it
            // automatically thereafter, so there are no write-path sync hooks.
            if (this.provider.SupportsFullText)
            {
                foreach (var mapping in this.options.Mappings.FullTextMappings.Values)
                {
                    var typeName = TypeNameResolver.Resolve(mapping.DocumentType, this.options.TypeNameResolution);
                    if (this.options.ResolveTableName(typeName) != tableName)
                        continue;

                    foreach (var ftsSql in this.provider.BuildCreateFullTextSql(tableName, typeName, mapping))
                    {
                        await using var ftsCmd = session.CreateCommand();
                        ftsCmd.CommandText = ftsSql;
                        this.Log(ftsCmd.CommandText);
                        try
                        {
                            await ftsCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                        }
                        catch (Exception)
                        {
                            // Index / column / trigger may already exist — safe to ignore.
                        }
                    }
                }
            }

            // Materialize computed columns (native generated/computed column + optional index) for every
            // mapped type stored in this table that requested it, where the provider supports it. The
            // engine maintains the value thereafter, so there are no write-path sync hooks.
            if (this.provider.SupportsComputedColumns)
            {
                foreach (var mapping in this.options.Mappings.Computed.All())
                {
                    if (mapping.MaterializedColumn == null)
                        continue;

                    var typeName = TypeNameResolver.Resolve(mapping.DocumentType, this.options.TypeNameResolution);
                    if (this.options.ResolveTableName(typeName) != tableName)
                        continue;

                    var typeInfo = this.jsonOptions.GetTypeInfo(mapping.DocumentType);
                    var node = Internal.Query.ExpressionLowerer.LowerValue(mapping.Definition.Body, this.jsonOptions, typeInfo);
                    var expressionSql = Internal.Query.SqlPredicateEmitter.EmitValueInline(node, this.provider);

                    foreach (var ccSql in this.provider.BuildCreateComputedColumnSql(tableName, typeName, mapping, expressionSql))
                    {
                        await using var ccCmd = session.CreateCommand();
                        ccCmd.CommandText = ccSql;
                        this.Log(ccSql);
                        try
                        {
                            await ccCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                        }
                        catch (Exception)
                        {
                            // Column / index may already exist — safe to ignore.
                        }
                    }
                }
            }

            // Create the temporal history sidecar if the provider supports it and any temporal type
            // is stored in this table.
            var historySql = this.provider.SupportsTemporal && TableHasTemporalMapping(this.options, tableName)
                ? this.provider.BuildCreateHistoryTableSql(tableName)
                : null;
            if (historySql != null)
            {
                await using var historyCmd = session.CreateCommand();
                historyCmd.CommandText = historySql;
                this.Log(historyCmd.CommandText);
                await historyCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

                foreach (var indexSql in this.provider.BuildCreateHistoryIndexesSql(tableName))
                {
                    await using var idxCmd = session.CreateCommand();
                    idxCmd.CommandText = indexSql;
                    this.Log(idxCmd.CommandText);
                    try
                    {
                        await idxCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        // Index may already exist — safe to ignore
                    }
                }
            }

            // Create the blob sidecar if the provider supports blobs and any blob-mapped type lands here.
            var blobSql = this.provider.MaxBlobSize > 0 && TableHasBlobMapping(this.options, tableName)
                ? this.provider.BuildCreateBlobTableSql(tableName)
                : null;
            if (blobSql != null)
            {
                await using var blobCmd = session.CreateCommand();
                blobCmd.CommandText = blobSql;
                this.Log(blobCmd.CommandText);
                await blobCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }
    }

    // True when any temporal-mapped type resolves to this table — gates history sidecar creation.
    static bool TableHasTemporalMapping(DocumentStoreOptions options, string tableName)
    {
        foreach (var mapping in options.Mappings.TemporalMappings)
        {
            var typeName = TypeNameResolver.Resolve(mapping.DocumentType, options.TypeNameResolution);
            if (options.ResolveTableName(typeName) == tableName)
                return true;
        }
        return false;
    }

    static bool TableHasBlobMapping(DocumentStoreOptions options, string tableName)
    {
        foreach (var documentType in options.Mappings.BlobMappings.Keys)
        {
            var typeName = TypeNameResolver.Resolve(documentType, options.TypeNameResolution);
            if (options.ResolveTableName(typeName) == tableName)
                return true;
        }
        return false;
    }

    /// <summary>
    /// Acquires a session (per-op connection in pooled mode, shared connection under the semaphore
    /// in shared mode), runs the operation, then releases. The session must not be used after the
    /// callback returns.
    /// </summary>
    async Task<TResult> ExecuteAsync<TResult>(string tableName, Func<DocumentStoreSession, Task<TResult>> operation, CancellationToken ct)
    {
        if (this.sharedMode)
        {
            await this.sharedSemaphore!.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await this.EnsureSharedConnectionInitializedAsync(ct).ConfigureAwait(false);
                var session = new DocumentStoreSession(this.sharedConnection!);
                await this.EnsureTableInitializedAsync(session, tableName, ct).ConfigureAwait(false);
                return await operation(session).ConfigureAwait(false);
            }
            finally
            {
                this.sharedSemaphore.Release();
            }
        }
        else
        {
            await using var conn = this.provider.CreateConnection();
            await conn.OpenAsync(ct).ConfigureAwait(false);
            await this.provider.InitializeConnectionAsync(conn, ct).ConfigureAwait(false);
            if (this.provider.SupportsVector && this.options.Mappings.VectorMappings.Count > 0)
                await this.provider.LoadVectorExtensionAsync(conn, ct).ConfigureAwait(false);
            var session = new DocumentStoreSession(conn);
            await this.EnsureTableInitializedAsync(session, tableName, ct).ConfigureAwait(false);
            return await operation(session).ConfigureAwait(false);
        }
    }

    Task ExecuteAsync(string tableName, Func<DocumentStoreSession, Task> operation, CancellationToken ct)
        => this.ExecuteAsync<object?>(tableName, async session =>
        {
            await operation(session).ConfigureAwait(false);
            return null;
        }, ct);

    async Task InsertCoreAsync(DocumentStoreSession session, string tableName, string id, string typeName, string json, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;

        await using var cmd = session.CreateCommand();
        if (this.tenantIdAccessor != null)
        {
            var insertSql = this.provider.BuildInsertSql(tableName);
            cmd.CommandText = insertSql
                .Replace("(Id, TypeName, Data, CreatedAt, UpdatedAt)", "(Id, TypeName, TenantId, Data, CreatedAt, UpdatedAt)")
                // Match only the leading "(@id, @typeName," so the substitution survives providers
                // that wrap @data in a cast (Postgres → CAST(@data AS JSONB), DuckDB → CAST(@data AS JSON)).
                .Replace("(@id, @typeName,", "(@id, @typeName, @tenantId,");
            AddParameter(cmd, "@tenantId", this.tenantIdAccessor());
        }
        else
        {
            cmd.CommandText = this.provider.BuildInsertSql(tableName);
        }
        AddParameter(cmd, "@id", id);
        AddParameter(cmd, "@typeName", typeName);
        AddParameter(cmd, "@data", json);
        AddParameter(cmd, "@now", now);

        this.Log(cmd.CommandText);
        try
        {
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (this.provider.IsDuplicateKeyException(ex))
        {
            throw new InvalidOperationException(
                $"A document of type '{typeName}' with Id '{id}' already exists.", ex);
        }
    }

    const int BatchChunkSize = 500;

    static async Task<int> BatchInsertCoreAsync<T>(
        string tableName,
        string typeName,
        IEnumerable<T> documents,
        IdAccessor<T> accessor,
        JsonTypeInfo<T>? typeInfo,
        JsonSerializerOptions jsonOptions,
        Action<string>? log,
        IDatabaseProvider provider,
        Func<DbCommand> createCommand,
        Func<IdKind, string, string, CancellationToken, Task<string>> generateId,
        Internal.VersionMapping? versionMapping,
        CancellationToken ct) where T : class
    {
        // Phase 1: resolve IDs and serialize all documents
        var rows = new List<(string id, string data)>();
        long nextInt = -1;

        foreach (var document in documents)
        {
            string id;
            if (accessor.IsDefaultId(document))
            {
                if (accessor.Kind == IdKind.Custom)
                {
                    id = accessor.GenerateOrThrow();
                }
                else if (accessor.Kind == IdKind.String)
                {
                    throw new InvalidOperationException(
                        $"Insert requires a non-empty string Id on '{typeof(T).Name}'. " +
                        "String Id properties are not auto-generated during Insert.");
                }
                else if (accessor.Kind is IdKind.Int or IdKind.Long)
                {
                    if (nextInt < 0)
                    {
                        var seed = await generateId(accessor.Kind, tableName, typeName, ct).ConfigureAwait(false);
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
                    id = await generateId(accessor.Kind, tableName, typeName, ct).ConfigureAwait(false);
                }
                accessor.SetId(document, id);
            }
            else
            {
                id = accessor.GetIdAsString(document);
            }

            versionMapping?.SetVersion(document, 1);
            rows.Add((id, SerializeDocument(document, typeInfo, jsonOptions)));
        }

        if (rows.Count == 0)
            return 0;

        // Phase 2: chunk into batches and execute multi-row INSERTs
        var now = DateTimeOffset.UtcNow;
        var totalInserted = 0;

        for (var offset = 0; offset < rows.Count; offset += BatchChunkSize)
        {
            var chunkSize = Math.Min(BatchChunkSize, rows.Count - offset);

            await using var cmd = createCommand();
            cmd.CommandText = provider.BuildBatchInsertSql(tableName, chunkSize);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@now", now);

            for (var i = 0; i < chunkSize; i++)
            {
                var row = rows[offset + i];
                AddParameter(cmd, $"@id_{i}", row.id);
                AddParameter(cmd, $"@data_{i}", row.data);
            }

            log?.Invoke(cmd.CommandText);
            try
            {
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (provider.IsDuplicateKeyException(ex))
            {
                throw new InvalidOperationException(
                    $"A document of type '{typeName}' has a duplicate Id in the batch.", ex);
            }
            totalInserted += chunkSize;
        }

        return totalInserted;
    }

    // A batch operation may use the provider's fast multi-row / delete-by-id-list path only for a "plain"
    // type: no version mapping, no spatial/vector/temporal sidecar, no tenant scoping, no global query
    // filters, and no per-doc interceptors. Anything else routes through the per-document loop (inside one
    // transaction) so the existing single-doc cores apply that behavior correctly.
    static bool IsBatchFastEligible(DocumentStoreOptions options, Type documentType)
        => options.TenantIdAccessor == null
        && options.ResolveVersionMapping(documentType) == null
        && options.ResolveSpatialMapping(documentType) == null
        && options.ResolveVectorMapping(documentType) == null
        && options.ResolveTemporalMapping(documentType) == null
        // Blob payloads have to be split out to the sidecar per document, so blob-mapped types take the
        // per-document loop (still inside one unit of work) rather than the set-based fast path.
        && options.ResolveBlobMappings(documentType).Count == 0
        && options.ResolveQueryFilters(documentType).Count == 0
        // Per-doc interceptors normally force the per-document loop so each row fires Before/AfterWrite.
        // But under a suppression scope no interceptor will run anyway, so the fast path is safe again
        // (e.g. Shiny.DocumentDb.AppDataSync inbound applies, bulk import).
        && (!options.Interceptors.HasPerDoc || DocumentOperationScope.Suppressed);

    // Multi-row deep-merge upsert (RFC 7396) in chunked single round-trips. Only called for fast-eligible
    // types on providers where SupportsBatchUpsert is true (SQLite, DuckDB).
    static async Task BatchUpsertFastCoreAsync<T>(
        string tableName,
        string typeName,
        IReadOnlyList<T> patches,
        IdAccessor<T> accessor,
        JsonTypeInfo<T>? typeInfo,
        JsonSerializerOptions jsonOptions,
        Action<string>? log,
        IDatabaseProvider provider,
        Func<DbCommand> createCommand,
        CancellationToken ct) where T : class
    {
        var rows = new List<(string id, string data)>(patches.Count);
        foreach (var patch in patches)
            rows.Add((accessor.GetIdAsString(patch), StripNullProperties(SerializeDocument(patch, typeInfo, jsonOptions))));

        var now = DateTimeOffset.UtcNow;
        for (var offset = 0; offset < rows.Count; offset += BatchChunkSize)
        {
            var chunkSize = Math.Min(BatchChunkSize, rows.Count - offset);

            await using var cmd = createCommand();
            cmd.CommandText = provider.BuildBatchUpsertSql(tableName, chunkSize);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@now", now);
            for (var i = 0; i < chunkSize; i++)
            {
                var row = rows[offset + i];
                AddParameter(cmd, $"@id_{i}", row.id);
                AddParameter(cmd, $"@data_{i}", row.data);
            }
            log?.Invoke(cmd.CommandText);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    // Delete-by-id-list in chunked single round-trips. Returns the total rows actually deleted.
    static async Task<int> BatchDeleteByIdsCoreAsync(
        string tableName,
        string typeName,
        IReadOnlyList<string> ids,
        Func<DbCommand> createCommand,
        IDatabaseProvider provider,
        Action<string>? log,
        CancellationToken ct)
    {
        var total = 0;
        for (var offset = 0; offset < ids.Count; offset += BatchChunkSize)
        {
            var chunkSize = Math.Min(BatchChunkSize, ids.Count - offset);

            await using var cmd = createCommand();
            cmd.CommandText = provider.BuildBatchDeleteByIdsSql(tableName, chunkSize);
            AddParameter(cmd, "@typeName", typeName);
            for (var i = 0; i < chunkSize; i++)
                AddParameter(cmd, $"@id_{i}", ids[offset + i]);
            log?.Invoke(cmd.CommandText);
            total += await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        return total;
    }

    async Task UpdateCoreAsync(DocumentStoreSession session, string tableName, string id, string typeName, string json, int? expectedVersion, string? versionJsonPath, Action<DbCommand>? appendFilters, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;

        await using var cmd = session.CreateCommand();
        cmd.CommandText = this.provider.BuildUpdateSql(tableName);
        if (this.tenantIdAccessor != null)
        {
            cmd.CommandText = cmd.CommandText.Replace(
                "WHERE Id = @id AND TypeName = @typeName",
                "WHERE Id = @id AND TypeName = @typeName AND TenantId = @tenantId");
            AddParameter(cmd, "@tenantId", this.tenantIdAccessor());
        }
        if (expectedVersion != null && versionJsonPath != null)
        {
            cmd.CommandText = cmd.CommandText.TrimEnd().TrimEnd(';')
                // Use JsonExtractTyped so providers like PostgreSQL emit an explicit cast on the
                // extracted text (Data #>> '{Version}'::BIGINT); a bare extract returns text and
                // PG rejects "text = integer" with a 42883 operator-not-exist error.
                + $" AND {this.provider.JsonExtractTyped("Data", versionJsonPath, typeof(int))} = @expectedVersion;";
            AddParameter(cmd, "@expectedVersion", expectedVersion.Value);
        }
        AddParameter(cmd, "@id", id);
        AddParameter(cmd, "@typeName", typeName);
        AddParameter(cmd, "@data", json);
        AddParameter(cmd, "@now", now);
        appendFilters?.Invoke(cmd);

        this.Log(cmd.CommandText);
        var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (rows == 0)
        {
            if (expectedVersion != null)
                throw new ConcurrencyException(typeName, id, expectedVersion.Value);

            throw new InvalidOperationException(
                $"No document of type '{typeName}' with Id '{id}' was found to update.");
        }
    }

    async Task UpsertMergeCoreAsync(DocumentStoreSession session, string tableName, string id, string typeName, string json, int? expectedVersion, string? versionJsonPath, CancellationToken ct)
    {
        json = StripNullProperties(json);
        var now = DateTimeOffset.UtcNow;

        if (this.provider.SupportsJsonMergePatch)
        {
            await using var cmd = session.CreateCommand();
            var upsertSql = this.provider.BuildUpsertMergeSql(tableName);
            if (this.tenantIdAccessor != null)
            {
                upsertSql = upsertSql
                    .Replace("(Id, TypeName, Data, CreatedAt, UpdatedAt)", "(Id, TypeName, TenantId, Data, CreatedAt, UpdatedAt)")
                    // Match only the leading "(@id, @typeName," so the substitution survives providers
                // that wrap @data in a cast (Postgres → CAST(@data AS JSONB), DuckDB → CAST(@data AS JSON)).
                .Replace("(@id, @typeName,", "(@id, @typeName, @tenantId,");
                AddParameter(cmd, "@tenantId", this.tenantIdAccessor());
            }
            if (expectedVersion != null && versionJsonPath != null)
            {
                // Append version check to the update path of the upsert.
                // For most SQL dialects, the ON CONFLICT/ON DUPLICATE KEY UPDATE ... supports a trailing WHERE.
                upsertSql = upsertSql.TrimEnd().TrimEnd(';')
                    // Use JsonExtractTyped so providers like PostgreSQL emit an explicit cast on the
                // extracted text (Data #>> '{Version}'::BIGINT); a bare extract returns text and
                // PG rejects "text = integer" with a 42883 operator-not-exist error.
                + $" AND {this.provider.JsonExtractTyped("Data", versionJsonPath, typeof(int))} = @expectedVersion;";
                AddParameter(cmd, "@expectedVersion", expectedVersion.Value);
            }
            cmd.CommandText = upsertSql;
            AddParameter(cmd, "@id", id);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@data", json);
            AddParameter(cmd, "@now", now);

            this.Log(cmd.CommandText);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            return;
        }

        // Fallback path: providers (PostgreSQL, SQL Server) that lack a native RFC 7396
        // deep-merge function. Read-merge-write inside a transaction so the row lock taken by
        // BuildSelectDataForUpdateSql blocks concurrent writers until UPDATE/INSERT commits.
        //
        // When the session already carries a transaction — a batch of merge writes, which the caller wraps so
        // the whole batch is atomic — join it. Opening a second one on the same connection is an error on
        // every pooled provider ("a transaction is already in progress").
        if (session.Transaction != null)
        {
            await UpsertMergeFallbackAsync(
                session.Connection, session.Transaction, this.provider, this.tenantIdAccessor,
                tableName, id, typeName, json, now, expectedVersion, versionJsonPath,
                this.Log, ct).ConfigureAwait(false);
            return;
        }

        await using var ownTx = await session.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
        try
        {
            await UpsertMergeFallbackAsync(
                session.Connection, ownTx, this.provider, this.tenantIdAccessor,
                tableName, id, typeName, json, now, expectedVersion, versionJsonPath,
                this.Log, ct).ConfigureAwait(false);
            await ownTx.CommitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            await ownTx.RollbackAsync(ct).ConfigureAwait(false);
            throw;
        }
    }

    // Backs the opt-in write modes — Update(patch: true) [merge, update-only] and
    // Upsert(patchIfUpdate: false) [replace, insert-or-update]. Always a read-modify-write so it works
    // uniformly on every relational provider (one extra round-trip on these opt-in calls); reuses the
    // ambient UnitOfWork transaction when present, otherwise takes its own.
    async Task MergeOrReplaceCoreAsync(DocumentStoreSession session, string tableName, string id, string typeName, string json, int? expectedVersion, string? versionJsonPath, bool merge, bool insertIfMissing, CancellationToken ct)
    {
        if (merge)
            json = StripNullProperties(json);
        var now = DateTimeOffset.UtcNow;

        if (session.Transaction != null)
        {
            await UpsertMergeFallbackAsync(
                session.Connection, session.Transaction, this.provider, this.tenantIdAccessor,
                tableName, id, typeName, json, now, expectedVersion, versionJsonPath,
                this.Log, ct, merge: merge, insertIfMissing: insertIfMissing).ConfigureAwait(false);
            return;
        }

        // Join an ambient batch transaction rather than nesting — see UpsertMergeCoreAsync.
        if (session.Transaction != null)
        {
            await UpsertMergeFallbackAsync(
                session.Connection, session.Transaction, this.provider, this.tenantIdAccessor,
                tableName, id, typeName, json, now, expectedVersion, versionJsonPath,
                this.Log, ct, merge: merge, insertIfMissing: insertIfMissing).ConfigureAwait(false);
            return;
        }

        await using var ownTx = await session.Connection.BeginTransactionAsync(ct).ConfigureAwait(false);
        try
        {
            await UpsertMergeFallbackAsync(
                session.Connection, ownTx, this.provider, this.tenantIdAccessor,
                tableName, id, typeName, json, now, expectedVersion, versionJsonPath,
                this.Log, ct, merge: merge, insertIfMissing: insertIfMissing).ConfigureAwait(false);
            await ownTx.CommitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            await ownTx.RollbackAsync(ct).ConfigureAwait(false);
            throw;
        }
    }

    // Read-modify-write inside a caller-owned transaction, generalized over the two write axes:
    //   merge=true  → RFC 7396 deep-merge the incoming json into the existing document;
    //   merge=false → replace the document body wholesale with the incoming json;
    //   insertIfMissing=true  → INSERT the incoming json when the row doesn't exist (upsert);
    //   insertIfMissing=false → throw "no document found" when it doesn't exist (update-only).
    static async Task UpsertMergeFallbackAsync(
        DbConnection connection,
        DbTransaction? transaction,
        IDatabaseProvider provider,
        Func<string>? tenantIdAccessor,
        string tableName,
        string id,
        string typeName,
        string json,
        DateTimeOffset now,
        int? expectedVersion,
        string? versionJsonPath,
        Action<string>? log,
        CancellationToken ct,
        bool merge = true,
        bool insertIfMissing = true)
    {
        string? existingJson;
        await using (var selectCmd = connection.CreateCommand())
        {
            selectCmd.Transaction = transaction;
            var selectSql = provider.BuildSelectDataForUpdateSql(tableName);
            if (tenantIdAccessor != null)
            {
                selectSql = selectSql.Replace(
                    "WHERE Id = @id AND TypeName = @typeName",
                    "WHERE Id = @id AND TypeName = @typeName AND TenantId = @tenantId");
                AddParameter(selectCmd, "@tenantId", tenantIdAccessor());
            }
            selectCmd.CommandText = selectSql;
            AddParameter(selectCmd, "@id", id);
            AddParameter(selectCmd, "@typeName", typeName);
            log?.Invoke(selectCmd.CommandText);
            var result = await selectCmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            existingJson = result == null || result == DBNull.Value ? null : result.ToString();
        }

        if (existingJson == null)
        {
            if (!insertIfMissing)
            {
                if (expectedVersion != null)
                    throw new ConcurrencyException(typeName, id, expectedVersion.Value);
                throw new InvalidOperationException(
                    $"No document of type '{typeName}' with Id '{id}' was found to update.");
            }

            await using var insertCmd = connection.CreateCommand();
            insertCmd.Transaction = transaction;
            var insertSql = provider.BuildInsertSql(tableName);
            if (tenantIdAccessor != null)
            {
                insertSql = insertSql
                    .Replace("(Id, TypeName, Data, CreatedAt, UpdatedAt)", "(Id, TypeName, TenantId, Data, CreatedAt, UpdatedAt)")
                    // Match only the leading "(@id, @typeName," so the substitution survives providers
                // that wrap @data in a cast (Postgres → CAST(@data AS JSONB), DuckDB → CAST(@data AS JSON)).
                .Replace("(@id, @typeName,", "(@id, @typeName, @tenantId,");
                AddParameter(insertCmd, "@tenantId", tenantIdAccessor());
            }
            insertCmd.CommandText = insertSql;
            AddParameter(insertCmd, "@id", id);
            AddParameter(insertCmd, "@typeName", typeName);
            AddParameter(insertCmd, "@data", json);
            AddParameter(insertCmd, "@now", now);
            log?.Invoke(insertCmd.CommandText);
            await insertCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            return;
        }

        if (expectedVersion != null && versionJsonPath != null)
        {
            var actual = ReadIntAtJsonPath(existingJson, versionJsonPath);
            if (actual != expectedVersion)
                throw new ConcurrencyException(typeName, id, expectedVersion.Value);
        }

        var merged = merge ? Internal.JsonMergePatch.Merge(existingJson, json) : json;

        await using var updateCmd = connection.CreateCommand();
        updateCmd.Transaction = transaction;
        var updateSql = provider.BuildUpdateSql(tableName);
        if (tenantIdAccessor != null)
        {
            updateSql = updateSql.Replace(
                "WHERE Id = @id AND TypeName = @typeName",
                "WHERE Id = @id AND TypeName = @typeName AND TenantId = @tenantId");
            AddParameter(updateCmd, "@tenantId", tenantIdAccessor());
        }
        updateCmd.CommandText = updateSql;
        AddParameter(updateCmd, "@id", id);
        AddParameter(updateCmd, "@typeName", typeName);
        AddParameter(updateCmd, "@data", merged);
        AddParameter(updateCmd, "@now", now);
        log?.Invoke(updateCmd.CommandText);
        await updateCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    static int? ReadIntAtJsonPath(string json, string jsonPath)
    {
        JsonNode? node = JsonNode.Parse(json);
        foreach (var segment in jsonPath.Split('.'))
        {
            if (node is not JsonObject obj || !obj.TryGetPropertyValue(segment, out var next))
                return null;
            node = next;
        }
        return node is JsonValue v && v.TryGetValue<int>(out var i) ? i : null;
    }

    async Task<bool> SetPropertyCoreAsync(DocumentStoreSession session, string tableName, string id, string typeName, string jsonPath, object? value, Action<DbCommand>? appendFilters, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;

        await using var cmd = session.CreateCommand();
        cmd.CommandText = this.provider.BuildSetPropertySql(tableName);
        if (this.tenantIdAccessor != null)
        {
            cmd.CommandText = cmd.CommandText.Replace(
                "WHERE Id = @id AND TypeName = @typeName",
                "WHERE Id = @id AND TypeName = @typeName AND TenantId = @tenantId");
            AddParameter(cmd, "@tenantId", this.tenantIdAccessor());
        }
        AddParameter(cmd, "@path", "$." + jsonPath);
        AddParameter(cmd, "@value", this.provider.FormatPropertyValue(value));
        AddParameter(cmd, "@now", now);
        AddParameter(cmd, "@id", id);
        AddParameter(cmd, "@typeName", typeName);
        appendFilters?.Invoke(cmd);

        this.Log(cmd.CommandText);
        var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        return rows > 0;
    }

    async Task<bool> RemovePropertyCoreAsync(DocumentStoreSession session, string tableName, string id, string typeName, string jsonPath, Action<DbCommand>? appendFilters, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;

        await using var cmd = session.CreateCommand();
        cmd.CommandText = this.provider.BuildRemovePropertySql(tableName);
        if (this.tenantIdAccessor != null)
        {
            cmd.CommandText = cmd.CommandText.Replace(
                "WHERE Id = @id AND TypeName = @typeName",
                "WHERE Id = @id AND TypeName = @typeName AND TenantId = @tenantId");
            AddParameter(cmd, "@tenantId", this.tenantIdAccessor());
        }
        AddParameter(cmd, "@path", "$." + jsonPath);
        AddParameter(cmd, "@now", now);
        AddParameter(cmd, "@id", id);
        AddParameter(cmd, "@typeName", typeName);
        appendFilters?.Invoke(cmd);

        this.Log(cmd.CommandText);
        var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        return rows > 0;
    }

    async Task<string> GenerateIdAsync(DocumentStoreSession session, IdKind kind, string tableName, string typeName, CancellationToken ct)
        => await GenerateIdCoreAsync(kind, tableName, typeName, session.CreateCommand, this.provider, s => this.Log(s), ct).ConfigureAwait(false);

    static async Task<string> GenerateIdCoreAsync(IdKind kind, string tableName, string typeName, Func<DbCommand> createCommand, IDatabaseProvider provider, Action<string>? log, CancellationToken ct)
    {
        switch (kind)
        {
            case IdKind.Guid:
                return Guid.NewGuid().ToString("N");

            case IdKind.String:
                return Guid.NewGuid().ToString();

            case IdKind.Int:
            case IdKind.Long:
                await using (var cmd = createCommand())
                {
                    cmd.CommandText = provider.BuildMaxIdSql(tableName);
                    AddParameter(cmd, "@typeName", typeName);
                    log?.Invoke(cmd.CommandText);
                    var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
                    var max = result is DBNull || result is null ? 0L : Convert.ToInt64(result);
                    return (max + 1).ToString();
                }

            default:
                throw new InvalidOperationException($"Unsupported Id kind: {kind}");
        }
    }

    // ── IQueryExecutor explicit implementation ──────────────────────────

    Task<TResult> IQueryExecutor.ExecuteAsync<TResult>(string tableName, Func<DocumentStoreSession, Task<TResult>> operation, CancellationToken ct)
        => this.ExecuteAsync(tableName, operation, ct);

    IAsyncEnumerable<T> IQueryExecutor.ReadStreamAsync<T>(string tableName, Action<DbCommand> configure, Func<string, T> deserialize, CancellationToken ct)
        => this.ReadStreamAsync(tableName, configure, deserialize, ct);

    string IQueryExecutor.ResolveTypeName<T>()
        => this.ResolveTypeName<T>();

    IdAccessor<T> IQueryExecutor.GetIdAccessor<T>(JsonTypeInfo<T>? typeInfo)
        => this.idCache.GetOrCreate(typeInfo);

    void IQueryExecutor.AttachBlobLoaders<T>(T document)
        => this.AttachBlobLoaders(document);

    string IQueryExecutor.ResolveTableName<T>()
        => this.ResolveTableName<T>();

    JsonSerializerOptions IQueryExecutor.JsonOptions
        => this.jsonOptions;

    Action<string>? IQueryExecutor.Logging
        => this.logging;

    IDatabaseProvider IQueryExecutor.Provider
        => this.provider;

    string? IQueryExecutor.TenantFilter
        => this.GetTenantFilter();

    void IQueryExecutor.AddTenantParameter(DbCommand cmd)
        => this.AddTenantParam(cmd);

    void IQueryExecutor.CollectTenantParameter(IDictionary<string, object?> parameters)
    {
        if (this.tenantIdAccessor != null)
            parameters["@tenantId"] = this.tenantIdAccessor();
    }

    ChangeBroadcaster? IQueryExecutor.Broadcaster => this.broadcaster;

    DocumentStoreOptions IQueryExecutor.Options => this.options;

    Task<IReadOnlyList<VectorResult<T>>> IQueryExecutor.NearestVectorsAsync<T>(
        ReadOnlyMemory<float> query, int k, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => this.NearestVectors<T>(query, k, filter, ct);

    Task<IReadOnlyList<FullTextResult<T>>> IQueryExecutor.FullTextSearchAsync<T>(
        string searchText, int maxResults, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => this.FullTextSearch<T>(searchText, maxResults, filter, ct);

    // ── Spatial sync helpers ──────────────────────────────────────────────

    async Task SpatialUpsertAsync<T>(DocumentStoreSession session, string tableName, string id, string typeName, T document, CancellationToken ct)
    {
        var mapping = this.options.ResolveSpatialMapping(typeof(T));
        var sql = mapping != null ? this.provider.BuildSpatialUpsertSql(tableName) : null;
        if (sql == null || mapping == null)
            return;

        var geometry = mapping.ResolveGeometry(document!);
        if (geometry is null)
        {
            // No location on this document (nullable spatial property set to null). Skip indexing it, and
            // purge any stale R*Tree row from a prior version that did have a location.
            await this.SpatialDeleteAsync(session, typeof(T), tableName, id, typeName, ct).ConfigureAwait(false);
            return;
        }

        var envelope = geometry.GetEnvelope();
        await using var cmd = session.CreateCommand();
        cmd.CommandText = sql;
        AddParameter(cmd, "@spatialDocId", id);
        AddParameter(cmd, "@spatialTypeName", typeName);
        AddParameter(cmd, "@spatialMinLat", envelope.MinLatitude);
        AddParameter(cmd, "@spatialMaxLat", envelope.MaxLatitude);
        AddParameter(cmd, "@spatialMinLng", envelope.MinLongitude);
        AddParameter(cmd, "@spatialMaxLng", envelope.MaxLongitude);
        if (this.provider.RequiresSpatialWkt)
            AddParameter(cmd, "@spatialWkt", Internal.Spatial.WktWriter.ToWkt(geometry));
        if (this.provider.RequiresSpatialGeoJson)
            AddParameter(cmd, "@spatialGeoJson", Internal.Spatial.SpatialJson.ToGeoJson(geometry));
        this.Log(cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    async Task SpatialDeleteAsync(DocumentStoreSession session, Type documentType, string tableName, string id, string typeName, CancellationToken ct)
    {
        var mapping = this.options.ResolveSpatialMapping(documentType);
        var sql = mapping != null ? this.provider.BuildSpatialDeleteSql(tableName) : null;
        if (sql == null)
            return;

        await using var cmd = session.CreateCommand();
        cmd.CommandText = sql;
        AddParameter(cmd, "@spatialDocId", id);
        AddParameter(cmd, "@spatialTypeName", typeName);
        this.Log(cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    async Task SpatialClearAsync(DocumentStoreSession session, Type documentType, string tableName, string typeName, CancellationToken ct)
    {
        var mapping = this.options.ResolveSpatialMapping(documentType);
        var sql = mapping != null ? this.provider.BuildSpatialClearSql(tableName) : null;
        if (sql == null)
            return;

        await using var cmd = session.CreateCommand();
        cmd.CommandText = sql;
        AddParameter(cmd, "@typeName", typeName);
        this.Log(cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    // ── Temporal history helpers ───────────────────────────────────────────

    Task AppendHistoryAsync(DocumentStoreSession session, Type documentType, string tableName, string id, string typeName, TemporalOperation operation, string? providedJson, CancellationToken ct)
    {
        var mapping = this.options.ResolveTemporalMapping(documentType);
        return mapping == null
            ? Task.CompletedTask
            : AppendHistoryCoreAsync(session.CreateCommand, this.provider, mapping, tableName, id, typeName, operation, providedJson, this.logging, ct, this.tenantIdAccessor?.Invoke());
    }

    /// <summary>
    /// Closes the current open version and appends a new one to the history sidecar, then applies
    /// retention. For Update-style writes that don't carry the full post-image (Upsert merges,
    /// SetProperty/RemoveProperty), <paramref name="providedJson"/> is null and the post-image is
    /// read back from the main table. Removed writes store a null-body tombstone. No-op when the
    /// provider doesn't support temporal history.
    /// </summary>
    static async Task AppendHistoryCoreAsync(
        Func<DbCommand> createCommand,
        IDatabaseProvider provider,
        Internal.TemporalMapping mapping,
        string tableName,
        string id,
        string typeName,
        TemporalOperation operation,
        string? providedJson,
        Action<string>? log,
        CancellationToken ct,
        string? tenantId = null)
    {
        if (!provider.SupportsTemporal)
            return;

        var closeSql = provider.BuildHistoryCloseSql(tableName);
        var insertSql = provider.BuildHistoryInsertSql(tableName);
        if (closeSql == null || insertSql == null)
            return;

        // Multi-tenancy: scope every history statement to the current tenant so a tenant can neither read nor
        // overwrite another tenant's version rows. The history sidecar carries a TenantId column for this.
        if (tenantId != null)
        {
            closeSql = closeSql.Replace(
                "WHERE Id = @id AND TypeName = @typeName",
                "WHERE Id = @id AND TypeName = @typeName AND TenantId = @tenantId");
            insertSql = insertSql
                .Replace("(Id, TypeName, Version,", "(Id, TypeName, TenantId, Version,")
                .Replace("SELECT @id, @typeName, COALESCE(MAX(Version), 0) + 1", "SELECT @id, @typeName, @tenantId, COALESCE(MAX(Version), 0) + 1")
                .Replace("WHERE Id = @id AND TypeName = @typeName", "WHERE Id = @id AND TypeName = @typeName AND TenantId = @tenantId");
        }

        // The full post-image is only handed in for Insert/Update. Upsert (merge) and the
        // property-level paths read back the resulting document so history stores the true state.
        var data = providedJson;
        if (data == null && operation != TemporalOperation.Removed)
        {
            await using var selectCmd = createCommand();
            var tf = tenantId != null ? " AND TenantId = @tenantId" : "";
            selectCmd.CommandText = $"SELECT Data FROM {provider.QuoteTable(tableName)} WHERE Id = @id AND TypeName = @typeName{tf};";
            AddParameter(selectCmd, "@id", id);
            AddParameter(selectCmd, "@typeName", typeName);
            if (tenantId != null)
                AddParameter(selectCmd, "@tenantId", tenantId);
            log?.Invoke(selectCmd.CommandText);
            var result = await selectCmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            data = result as string;
        }

        var now = DateTimeOffset.UtcNow;
        // Scope-aware actor: resolve from the flowing session scope when one is present (a write through a
        // scoped IDocumentSession); otherwise fall back to the plain unscoped accessor.
        var scope = DocumentOperationScope.CurrentServices;
        var actor = mapping.ResolveActor != null && scope != null
            ? mapping.ResolveActor(scope)
            : mapping.CaptureActor?.Invoke();

        await using (var closeCmd = createCommand())
        {
            closeCmd.CommandText = closeSql;
            AddParameter(closeCmd, "@id", id);
            AddParameter(closeCmd, "@typeName", typeName);
            AddParameter(closeCmd, "@validTo", now);
            if (tenantId != null)
                AddParameter(closeCmd, "@tenantId", tenantId);
            log?.Invoke(closeCmd.CommandText);
            await closeCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using (var insertCmd = createCommand())
        {
            insertCmd.CommandText = insertSql;
            AddParameter(insertCmd, "@id", id);
            AddParameter(insertCmd, "@typeName", typeName);
            AddParameter(insertCmd, "@validFrom", now);
            AddParameter(insertCmd, "@operation", operation.ToString());
            AddParameter(insertCmd, "@actor", (object?)actor ?? DBNull.Value);
            AddParameter(insertCmd, "@data", (object?)data ?? DBNull.Value);
            if (tenantId != null)
                AddParameter(insertCmd, "@tenantId", tenantId);
            log?.Invoke(insertCmd.CommandText);
            await insertCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        if (mapping.Retention != null)
        {
            var pruneSql = provider.BuildHistoryPruneByAgeSql(tableName);
            if (pruneSql != null)
            {
                await using var pruneCmd = createCommand();
                pruneCmd.CommandText = pruneSql;
                AddParameter(pruneCmd, "@id", id);
                AddParameter(pruneCmd, "@typeName", typeName);
                AddParameter(pruneCmd, "@cutoff", now - mapping.Retention.Value);
                log?.Invoke(pruneCmd.CommandText);
                await pruneCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }

        if (mapping.MaxVersions != null)
        {
            var pruneSql = provider.BuildHistoryPruneByCountSql(tableName);
            if (pruneSql != null)
            {
                await using var pruneCmd = createCommand();
                pruneCmd.CommandText = pruneSql;
                AddParameter(pruneCmd, "@id", id);
                AddParameter(pruneCmd, "@typeName", typeName);
                AddParameter(pruneCmd, "@keep", mapping.MaxVersions.Value);
                log?.Invoke(pruneCmd.CommandText);
                await pruneCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }
    }

    // ── Vector sync helpers ───────────────────────────────────────────────

    async Task VectorUpsertAsync<T>(DocumentStoreSession session, string tableName, string typeName, string id, T document, CancellationToken ct) where T : class
    {
        if (!this.provider.SupportsVector) return;
        var mapping = this.options.ResolveVectorMapping(typeof(T));
        if (mapping == null) return;

        var vec = mapping.GetVector(document);
        if (vec.Length == 0) return; // skip default/empty embedding — explicit population only

        if (vec.Length != mapping.Dimensions)
            throw new ArgumentException(
                $"Vector for document '{id}' of type '{typeName}' has {vec.Length} elements; expected {mapping.Dimensions}.");

        var sql = this.provider.BuildVectorUpsertSql(tableName, typeName, mapping);
        if (sql == null) return;

        await using var cmd = session.CreateCommand();
        cmd.CommandText = sql;
        AddParameter(cmd, "@vecDocId", id);
        AddParameter(cmd, "@vecTypeName", typeName);
        AddParameter(cmd, "@embedding", this.provider.FormatVectorParameter(vec, mapping));
        this.Log(cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    async Task VectorDeleteAsync(DocumentStoreSession session, Type documentType, string tableName, string typeName, string id, CancellationToken ct)
    {
        if (!this.provider.SupportsVector) return;
        var mapping = this.options.ResolveVectorMapping(documentType);
        if (mapping == null) return;

        var sql = this.provider.BuildVectorDeleteSql(tableName, typeName);
        if (sql == null) return;

        await using var cmd = session.CreateCommand();
        cmd.CommandText = sql;
        AddParameter(cmd, "@vecDocId", id);
        AddParameter(cmd, "@vecTypeName", typeName);
        this.Log(cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    async Task VectorClearAsync(DocumentStoreSession session, Type documentType, string tableName, string typeName, CancellationToken ct)
    {
        if (!this.provider.SupportsVector) return;
        var mapping = this.options.ResolveVectorMapping(documentType);
        if (mapping == null) return;

        var sql = this.provider.BuildVectorClearSql(tableName, typeName);
        if (sql == null) return;

        await using var cmd = session.CreateCommand();
        cmd.CommandText = sql;
        AddParameter(cmd, "@vecTypeName", typeName);
        this.Log(cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    // ── Write-interceptor helpers (thin delegators to the shared pipeline) ──
    // A single write with per-document interceptors runs as an implicit one-operation unit of work so its
    // ctx.Store is the transaction-bound TransactionalDocumentStore (uncommitted-row visibility, atomic
    // side-effects, and — critically — no shared-connection deadlock when a hook reads/writes through ctx.Store).
    // Lazy: false when nothing is registered (or suppressed), so the non-transactional fast path is untouched.
    bool RequiresImplicitUnit() => this.options.Interceptors.HasPerDoc && !DocumentOperationScope.Suppressed;

    DocumentWriteContext? NewWriteContext<T>(DocumentOperation op, string typeName, object? id, T? document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        var pipeline = this.options.Interceptors;
        if (!pipeline.HasPerDoc || DocumentOperationScope.Suppressed)
            return null;
        // Lazy serialize delegate so an interceptor can read the exact JSON about to be written
        // (ctx.GetJson()). FindTypeInfo runs only when GetJson() is called, keeping AOT/throw behaviour
        // identical to the real write path.
        Func<object, string>? factory = document == null
            ? null
            : doc => SerializeDocument((T)doc, FindTypeInfo(jsonTypeInfo), this.jsonOptions);
        // ctx.Store on the top-level store is the committed-state fallback (used by the JSON lane and any write
        // that isn't routed through the implicit one-op unit). Typed single writes with per-doc interceptors are
        // re-routed through RunUnitAsync, where ctx.Store becomes the transaction-bound TransactionalDocumentStore.
        return pipeline.NewWrite(op, typeName, id, document, this, DocumentOperationScope.CurrentServices, factory);
    }

    // Returns false when an interceptor cancelled the write: the caller skips its write entirely and reports
    // ctx.CancelResult (the interceptor performed a replacement write of its own).
    Task<bool> RunBeforeWriteAsync(DocumentWriteContext? ctx, CancellationToken ct)
        => this.options.Interceptors.BeforeWrite(ctx, ct);

    Task RunAfterWriteAsync(DocumentWriteContext? ctx, object? id, int? version, CancellationToken ct)
        => this.options.Interceptors.AfterWrite(ctx, id, version, ct);

    DocumentBulkContext? NewBulkContext<T>(DocumentOperation op, string typeName, string? whereClause, IReadOnlyList<(string Property, object? Value)>? assignments, IDocumentQuery<T>? sourceQuery = null) where T : class
    {
        var ctx = this.options.Interceptors.NewBulk(op, typeName, whereClause, assignments, sourceQuery);
        // Store lets a cancelling interceptor re-issue the write itself — the only handle it has for a Clear,
        // which has no source query.
        if (ctx != null)
            ctx.Store = this;
        return ctx;
    }

    // Returns false when an interceptor cancelled the set-based write; the caller reports ctx.CancelAffected.
    Task<bool> RunBeforeBulkAsync(DocumentBulkContext? ctx, CancellationToken ct)
        => this.options.Interceptors.BeforeBulk(ctx, ct);

    Task RunAfterBulkAsync(DocumentBulkContext? ctx, int affected, CancellationToken ct)
        => this.options.Interceptors.AfterBulk(ctx, affected, ct);

    /// <summary>
    /// Validates the vector field length up front so dimension errors surface before any
    /// connection / extension-load work runs. The actual sidecar write still happens inside
    /// <see cref="VectorUpsertAsync"/> and re-checks defensively.
    /// </summary>
    void ValidateVectorDimensions<T>(T document) where T : class
    {
        var mapping = this.options.ResolveVectorMapping(typeof(T));
        if (mapping == null) return;
        var vec = mapping.GetVector(document!);
        if (vec.Length == 0) return; // unset is fine
        if (vec.Length != mapping.Dimensions)
            throw new ArgumentException(
                $"Vector for type '{typeof(T).Name}' has {vec.Length} elements; mapping expects {mapping.Dimensions}.",
                nameof(document));
    }

    // ── Query<T>() entry point ──────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        return new DocumentQuery<T>(this, FindTypeInfo(jsonTypeInfo));
    }

    // ── CRUD ────────────────────────────────────────────────────────────

    public Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("insert", typeof(T).Name, () => this.InsertImpl(document, jsonTypeInfo, cancellationToken));

    async Task InsertImpl<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        if (this.RequiresImplicitUnit())
        {
            await ((IUnitOfWorkEngine)this).RunUnitAsync((s, ct) => s.Insert(document, jsonTypeInfo, ct), cancellationToken).ConfigureAwait(false);
            return;
        }
        var typeNameForCtx = this.ResolveTypeName<T>();
        var ctx = this.NewWriteContext(DocumentOperation.Insert, typeNameForCtx, null, document, jsonTypeInfo);
        if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
            return;
        if (ctx?.Document is T mutated)
            document = mutated;
        this.ValidateVectorDimensions(document);
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var versionMapping = this.ResolveVersionMapping<T>();
        var insertedId = "";
        var capturedDoc = document;
        await this.ExecuteAsync(tableName, async session =>
        {
            var typeName2 = this.ResolveTypeName<T>();
            var isDefaultId = accessor.IsDefaultId(capturedDoc);
            // Auto-generated int/long ids come from SELECT MAX(Id)+1, which races on pooled providers (two
            // concurrent inserts pick the same id). Retry on a duplicate-key collision: regenerating re-reads
            // MAX over the now-committed conflicting row and yields a fresh id. Guid/custom ids never collide,
            // and a user-supplied duplicate must surface, not silently mutate — so only auto-gen int/long retry.
            var autoGenNumeric = isDefaultId && accessor.Kind is IdKind.Int or IdKind.Long;
            string id;
            if (isDefaultId)
            {
                if (accessor.Kind == IdKind.Custom)
                    id = accessor.GenerateOrThrow();
                else if (accessor.Kind == IdKind.String)
                    throw new InvalidOperationException(
                        $"Insert requires a non-empty string Id on '{typeof(T).Name}'. " +
                        "String Id properties are not auto-generated during Insert.");
                else
                    id = await this.GenerateIdAsync(session, accessor.Kind, tableName, typeName2, cancellationToken).ConfigureAwait(false);
                accessor.SetId(capturedDoc, id);
            }
            else
            {
                id = accessor.GetIdAsString(capturedDoc);
            }
            // Stamp blob keys/lengths/hashes before serializing — the metadata persisted in the document body
            // has to describe the rows BlobSyncAsync is about to write.
            var preparedBlobs = this.PrepareBlobs(typeof(T), capturedDoc);
            var attempt = 0;
            while (true)
            {
                versionMapping?.SetVersion(capturedDoc, 1);
                var jsonAttempt = SerializeDocument(capturedDoc, typeInfo, this.jsonOptions);
                try
                {
                    await this.InsertCoreAsync(session, tableName, id, typeName2, jsonAttempt, cancellationToken).ConfigureAwait(false);
                    break;
                }
                catch (InvalidOperationException) when (autoGenNumeric && ++attempt < 10_000)
                {
                    // MAX(Id)+1 races on pooled providers. Linear-probe the next id: under contention each
                    // concurrent insert walks up to a distinct free slot, which converges far more reliably
                    // than re-running MAX (which would just re-collide with the same peers).
                    id = (long.Parse(id, CultureInfo.InvariantCulture) + 1).ToString(CultureInfo.InvariantCulture);
                    accessor.SetId(capturedDoc, id);
                }
            }
            var json = SerializeDocument(capturedDoc, typeInfo, this.jsonOptions);
            await this.BlobSyncAsync(session, typeof(T), tableName, id, typeName2, preparedBlobs, prune: true, cancellationToken).ConfigureAwait(false);
            await this.SpatialUpsertAsync(session, tableName, id, typeName2, capturedDoc, cancellationToken).ConfigureAwait(false);
            await this.VectorUpsertAsync(session, tableName, typeName2, id, capturedDoc, cancellationToken).ConfigureAwait(false);
            await this.AppendHistoryAsync(session, typeof(T), tableName, id, typeName2, TemporalOperation.Inserted, json, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(capturedDoc) ?? 1, cancellationToken).ConfigureAwait(false);
            insertedId = id;
        }, cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Inserted, insertedId, document);
    }

    public Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("batch_insert", typeof(T).Name, () => this.BatchInsertImpl(documents, jsonTypeInfo, cancellationToken), r => r);

    async Task<int> BatchInsertImpl<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var versionMapping = this.ResolveVersionMapping<T>();
        // Materialize so we can replay the inserted documents to observers after commit.
        var docList = documents as IReadOnlyList<T> ?? documents.ToList();

        if (docList.Count == 0)
            return 0;

        // Types that need tenant scoping / a version column / spatial|vector|temporal sidecars / global query
        // filters / per-doc interceptors are NOT eligible for the fast multi-row insert (which has none of that
        // plumbing) — they fall back to a per-document loop reusing the single-doc Insert, which handles tenant
        // injection, spatial + vector sidecars, temporal history, filters, interceptors and auto-embed, all in
        // one transaction. Historically BatchInsert skipped the tenant column and the spatial sidecar entirely.
        if (!IsBatchFastEligible(this.options, typeof(T)))
        {
            await ((IUnitOfWorkEngine)this).RunUnitAsync(async (tx, ct) =>
            {
                foreach (var document in docList)
                    await tx.Insert(document, typeInfo, ct).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);
            return docList.Count;
        }

        // Fast eligible path: no interceptors/sidecars (auto-embed types carry a vector mapping and so are never
        // fast-eligible — they take the per-document loop above, where their interceptor fires per doc).
        foreach (var doc in docList)
            this.ValidateVectorDimensions(doc);

        List<DocumentWriteContext>? ctxs = null;
        var count = await this.ExecuteAsync(tableName, async session =>
        {
            await using var transaction = await session.Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                DbCommand txCreateCommand() { var c = session.Connection.CreateCommand(); c.Transaction = transaction; return c; }
                var inserted = await BatchInsertCoreAsync(
                    tableName, typeName, docList, accessor, typeInfo,
                    this.jsonOptions, this.logging, this.provider,
                    txCreateCommand,
                    (kind, tbl, tn, ct) => GenerateIdCoreAsync(kind, tbl, tn, txCreateCommand, this.provider, this.logging, ct),
                    versionMapping,
                    cancellationToken
                ).ConfigureAwait(false);

                // Vector sidecar inserts — once per doc inside the same txn so a vector write
                // failure rolls the whole batch back.
                if (this.provider.SupportsVector && this.options.ResolveVectorMapping(typeof(T)) != null)
                {
                    var txSession = new DocumentStoreSession(session.Connection, transaction);
                    foreach (var doc in docList)
                    {
                        var id = accessor.GetIdAsString(doc);
                        await this.VectorUpsertAsync(txSession, tableName, typeName, id, doc, cancellationToken).ConfigureAwait(false);
                    }
                }

                // Temporal history — one Inserted version per doc inside the same txn. The post-image
                // is read back from the freshly inserted rows.
                var temporalMapping = this.options.ResolveTemporalMapping(typeof(T));
                if (temporalMapping != null && this.provider.SupportsTemporal)
                {
                    foreach (var doc in docList)
                    {
                        var id = accessor.GetIdAsString(doc);
                        await AppendHistoryCoreAsync(txCreateCommand, this.provider, temporalMapping, tableName, id, typeName, TemporalOperation.Inserted, null, this.logging, cancellationToken).ConfigureAwait(false);
                    }
                }

                // Per-doc AfterWrite — inside the txn, with generated ids populated.
                if (ctxs != null)
                {
                    for (var i = 0; i < docList.Count; i++)
                    {
                        var doc = docList[i];
                        await this.RunAfterWriteAsync(ctxs[i], accessor.GetIdAsString(doc), versionMapping?.GetVersion(doc) ?? 1, cancellationToken).ConfigureAwait(false);
                    }
                }

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return inserted;
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }
        }, cancellationToken).ConfigureAwait(false);

        if (count > 0 && this.broadcaster.HasSubscribers<T>())
        {
            foreach (var document in docList)
                this.broadcaster.Publish(new DocumentChange<T>
                {
                    ChangeType = DocumentChangeType.Inserted,
                    Id = accessor.GetIdAsString(document),
                    Document = document
                });
        }
        return count;
    }

    public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("update", typeof(T).Name, () => this.UpdateImpl(document, jsonTypeInfo, cancellationToken));

    async Task UpdateImpl<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        if (this.RequiresImplicitUnit())
        {
            await ((IUnitOfWorkEngine)this).RunUnitAsync((s, ct) => s.Update(document, jsonTypeInfo, ct), cancellationToken).ConfigureAwait(false);
            return;
        }
        var typeNameForCtx = this.ResolveTypeName<T>();
        var ctx = this.NewWriteContext(DocumentOperation.Update, typeNameForCtx, null, document, jsonTypeInfo);
        if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
            return;
        if (ctx?.Document is T mutated)
            document = mutated;
        this.ValidateVectorDimensions(document);
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var versionMapping = this.ResolveVersionMapping<T>();
        var updatedId = "";
        var capturedDoc = document;
        await this.ExecuteAsync(tableName, async session =>
        {
            if (accessor.IsDefaultId(capturedDoc))
                throw new InvalidOperationException(
                    $"Update requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Update.");

            var id = accessor.GetIdAsString(capturedDoc);
            var typeName = this.ResolveTypeName<T>();

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                expectedVersion = versionMapping.GetVersion(capturedDoc);
                versionMapping.SetVersion(capturedDoc, expectedVersion.Value + 1);
            }

            var preparedBlobs = this.PrepareBlobs(typeof(T), capturedDoc);
            var json = SerializeDocument(capturedDoc, typeInfo, this.jsonOptions);
            try
            {
                await this.UpdateCoreAsync(session, tableName, id, typeName, json, expectedVersion, versionMapping?.JsonPath, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken).ConfigureAwait(false);
            }
            catch (ConcurrencyException)
            {
                // The CAS failed — restore the caller's in-memory version so a naive retry with the same object
                // doesn't send a doubly-bumped version (and so the object isn't left inconsistent with storage).
                if (versionMapping != null && expectedVersion != null)
                    versionMapping.SetVersion(capturedDoc, expectedVersion.Value);
                throw;
            }
            await this.BlobSyncAsync(session, typeof(T), tableName, id, typeName, preparedBlobs, prune: true, cancellationToken).ConfigureAwait(false);
            await this.SpatialUpsertAsync(session, tableName, id, typeName, capturedDoc, cancellationToken).ConfigureAwait(false);
            await this.VectorUpsertAsync(session, tableName, typeName, id, capturedDoc, cancellationToken).ConfigureAwait(false);
            await this.AppendHistoryAsync(session, typeof(T), tableName, id, typeName, TemporalOperation.Updated, json, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(capturedDoc), cancellationToken).ConfigureAwait(false);
            updatedId = id;
        }, cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Updated, updatedId, document);
    }

    public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("upsert", typeof(T).Name, () => this.UpsertImpl(patch, jsonTypeInfo, cancellationToken));

    async Task UpsertImpl<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        if (this.RequiresImplicitUnit())
        {
            await ((IUnitOfWorkEngine)this).RunUnitAsync((s, ct) => s.Upsert(patch, jsonTypeInfo, ct), cancellationToken).ConfigureAwait(false);
            return;
        }
        var typeNameForCtx = this.ResolveTypeName<T>();
        var ctx = this.NewWriteContext(DocumentOperation.Upsert, typeNameForCtx, null, patch, jsonTypeInfo);
        if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
            return;
        if (ctx?.Document is T mutated)
            patch = mutated;
        this.ValidateVectorDimensions(patch);
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var versionMapping = this.ResolveVersionMapping<T>();
        var upsertedId = "";
        var capturedPatch = patch;
        await this.ExecuteAsync(tableName, async session =>
        {
            if (accessor.IsDefaultId(capturedPatch))
                throw new InvalidOperationException(
                    $"Upsert requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

            var id = accessor.GetIdAsString(capturedPatch);
            var typeName = this.ResolveTypeName<T>();

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                expectedVersion = versionMapping.GetVersion(capturedPatch);
                if (expectedVersion > 0)
                    versionMapping.SetVersion(capturedPatch, expectedVersion.Value + 1);
                else
                    versionMapping.SetVersion(capturedPatch, 1);
            }

            var preparedBlobs = this.PrepareBlobs(typeof(T), capturedPatch);
            var json = SerializeDocument(capturedPatch, typeInfo, this.jsonOptions);
            await this.UpsertMergeCoreAsync(session, tableName, id, typeName, json, expectedVersion > 0 ? expectedVersion : null, versionMapping?.JsonPath, cancellationToken).ConfigureAwait(false);
            // Upsert is a merge, so blobs absent from the patch stay put — prune would delete rows the merged
            // document still references.
            await this.BlobSyncAsync(session, typeof(T), tableName, id, typeName, preparedBlobs, prune: false, cancellationToken).ConfigureAwait(false);
            await this.SpatialUpsertAsync(session, tableName, id, typeName, capturedPatch, cancellationToken).ConfigureAwait(false);
            await this.VectorUpsertAsync(session, tableName, typeName, id, capturedPatch, cancellationToken).ConfigureAwait(false);
            // Upsert merges (RFC 7396); read back the post-merge document for the history snapshot.
            await this.AppendHistoryAsync(session, typeof(T), tableName, id, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(capturedPatch), cancellationToken).ConfigureAwait(false);
            upsertedId = id;
        }, cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Updated, upsertedId, patch);
    }

    public async Task Update<T>(T document, bool patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        if (!patch)
        {
            // Full-document replace — the existing, optimized single-statement path.
            await this.Update(document, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (this.RequiresImplicitUnit())
        {
            await ((IUnitOfWorkEngine)this).RunUnitAsync((s, ct) => ((TransactionalDocumentStore)s).UpdateMerge(document, jsonTypeInfo, ct), cancellationToken).ConfigureAwait(false);
            return;
        }

        // patch: true → RFC 7396 deep-merge into the existing document (the row must already exist).
        var typeNameForCtx = this.ResolveTypeName<T>();
        var ctx = this.NewWriteContext(DocumentOperation.Update, typeNameForCtx, null, document, jsonTypeInfo);
        if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
            return;
        if (ctx?.Document is T mutated)
            document = mutated;
        this.ValidateVectorDimensions(document);
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var versionMapping = this.ResolveVersionMapping<T>();
        var updatedId = "";
        var capturedDoc = document;
        await this.ExecuteAsync(tableName, async session =>
        {
            if (accessor.IsDefaultId(capturedDoc))
                throw new InvalidOperationException(
                    $"Update requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Update.");

            var id = accessor.GetIdAsString(capturedDoc);
            var typeName = this.ResolveTypeName<T>();

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                // Explicit optimistic concurrency (a loaded doc carries version > 0): CAS on it and bump.
                // A partial patch leaves the version unset (0) — don't CAS, and (below) don't let the patch's
                // 0 clobber the stored version through the merge.
                expectedVersion = versionMapping.GetVersion(capturedDoc);
                if (expectedVersion > 0)
                    versionMapping.SetVersion(capturedDoc, expectedVersion.Value + 1);
            }

            var json = SerializeDocument(capturedDoc, typeInfo, this.jsonOptions);
            if (versionMapping != null && !(expectedVersion > 0))
                json = RemoveJsonProperty(json, versionMapping.JsonPath);
            await this.MergeOrReplaceCoreAsync(session, tableName, id, typeName, json, expectedVersion > 0 ? expectedVersion : null, versionMapping?.JsonPath, merge: true, insertIfMissing: false, cancellationToken).ConfigureAwait(false);
            await this.SpatialUpsertAsync(session, tableName, id, typeName, capturedDoc, cancellationToken).ConfigureAwait(false);
            await this.VectorUpsertAsync(session, tableName, typeName, id, capturedDoc, cancellationToken).ConfigureAwait(false);
            // Merge changed the row; read back the post-merge document for the history snapshot.
            await this.AppendHistoryAsync(session, typeof(T), tableName, id, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(capturedDoc), cancellationToken).ConfigureAwait(false);
            updatedId = id;
        }, cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Updated, updatedId, document);
    }

    public async Task Upsert<T>(T patch, bool patchIfUpdate, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        if (patchIfUpdate)
        {
            // RFC 7396 merge-on-update — the existing default behavior.
            await this.Upsert(patch, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (this.RequiresImplicitUnit())
        {
            await ((IUnitOfWorkEngine)this).RunUnitAsync((s, ct) => ((TransactionalDocumentStore)s).UpsertReplace(patch, jsonTypeInfo, ct), cancellationToken).ConfigureAwait(false);
            return;
        }

        // patchIfUpdate: false → replace the document body wholesale if it exists, insert-as-is otherwise.
        var typeNameForCtx = this.ResolveTypeName<T>();
        var ctx = this.NewWriteContext(DocumentOperation.Upsert, typeNameForCtx, null, patch, jsonTypeInfo);
        if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
            return;
        if (ctx?.Document is T mutated)
            patch = mutated;
        this.ValidateVectorDimensions(patch);
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var versionMapping = this.ResolveVersionMapping<T>();
        var upsertedId = "";
        var capturedPatch = patch;
        await this.ExecuteAsync(tableName, async session =>
        {
            if (accessor.IsDefaultId(capturedPatch))
                throw new InvalidOperationException(
                    $"Upsert requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

            var id = accessor.GetIdAsString(capturedPatch);
            var typeName = this.ResolveTypeName<T>();

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                expectedVersion = versionMapping.GetVersion(capturedPatch);
                if (expectedVersion > 0)
                    versionMapping.SetVersion(capturedPatch, expectedVersion.Value + 1);
                else
                    versionMapping.SetVersion(capturedPatch, 1);
            }

            var json = SerializeDocument(capturedPatch, typeInfo, this.jsonOptions);
            await this.MergeOrReplaceCoreAsync(session, tableName, id, typeName, json, expectedVersion > 0 ? expectedVersion : null, versionMapping?.JsonPath, merge: false, insertIfMissing: true, cancellationToken).ConfigureAwait(false);
            await this.SpatialUpsertAsync(session, tableName, id, typeName, capturedPatch, cancellationToken).ConfigureAwait(false);
            await this.VectorUpsertAsync(session, tableName, typeName, id, capturedPatch, cancellationToken).ConfigureAwait(false);
            // Replace wrote the body verbatim; snapshot that exact json.
            await this.AppendHistoryAsync(session, typeof(T), tableName, id, typeName, TemporalOperation.Updated, json, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(capturedPatch), cancellationToken).ConfigureAwait(false);
            upsertedId = id;
        }, cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Updated, upsertedId, patch);
    }

    public async Task<int> BatchUpsert<T>(IEnumerable<T> patches, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var list = patches as IReadOnlyList<T> ?? patches.ToList();
        if (list.Count == 0)
            return 0;

        if (this.provider.SupportsBatchUpsert && IsBatchFastEligible(this.options, typeof(T)))
        {
            foreach (var patch in list)
                if (accessor.IsDefaultId(patch))
                    throw new InvalidOperationException(
                        $"Upsert requires a non-default Id on the document. " +
                        $"Set the Id property on '{typeof(T).Name}' before calling BatchUpsert.");

            await this.ExecuteAsync(tableName, async session =>
            {
                await using var transaction = await session.Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    DbCommand txCreateCommand() { var c = session.Connection.CreateCommand(); c.Transaction = transaction; return c; }
                    await BatchUpsertFastCoreAsync(tableName, typeName, list, accessor, typeInfo, this.jsonOptions, this.logging, this.provider, txCreateCommand, cancellationToken).ConfigureAwait(false);
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    throw;
                }
            }, cancellationToken).ConfigureAwait(false);

            foreach (var patch in list)
                this.PublishChange(DocumentChangeType.Updated, accessor.GetIdAsString(patch), patch);
            return list.Count;
        }

        // Fallback: atomic per-document loop reusing the single-doc Upsert (version / sidecar / tenant /
        // filters / interceptors all handled there). One transaction → all-or-nothing.
        await ((IUnitOfWorkEngine)this).RunUnitAsync(async (tx, ct) =>
        {
            foreach (var patch in list)
                await tx.Upsert(patch, typeInfo, ct).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
        return list.Count;
    }

    public async Task<int> BatchUpdate<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var list = documents as IReadOnlyList<T> ?? documents.ToList();
        if (list.Count == 0)
            return 0;

        // Heterogeneous-value updates have no clean multi-row form; loop the single-doc Update inside one
        // transaction (round-trip + commit batching, all-or-nothing on a missing row or version conflict).
        await ((IUnitOfWorkEngine)this).RunUnitAsync(async (tx, ct) =>
        {
            foreach (var document in list)
                await tx.Update(document, typeInfo, ct).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
        return list.Count;
    }

    public async Task<int> BatchRemove<T>(IEnumerable<object> ids, CancellationToken cancellationToken = default) where T : class
    {
        var accessor = this.idCache.GetOrCreate<T>(null);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var idList = ids as IReadOnlyList<object> ?? ids.ToList();
        if (idList.Count == 0)
            return 0;

        if (IsBatchFastEligible(this.options, typeof(T)))
        {
            var resolved = new List<string>(idList.Count);
            foreach (var id in idList)
                resolved.Add(accessor.ResolveId(id));

            var deleted = await this.ExecuteAsync(tableName, async session =>
            {
                await using var transaction = await session.Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    DbCommand txCreateCommand() { var c = session.Connection.CreateCommand(); c.Transaction = transaction; return c; }
                    var n = await BatchDeleteByIdsCoreAsync(tableName, typeName, resolved, txCreateCommand, this.provider, this.logging, cancellationToken).ConfigureAwait(false);
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                    return n;
                }
                catch
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    throw;
                }
            }, cancellationToken).ConfigureAwait(false);

            // A bulk delete can't report which specific ids matched; publish Removed for every requested id.
            if (deleted > 0)
                foreach (var id in resolved)
                    this.PublishChange<T>(DocumentChangeType.Removed, id, null);
            return deleted;
        }

        // Fallback: atomic per-id loop reusing the single-doc Remove (sidecar cleanup / history / filters).
        var removed = 0;
        await ((IUnitOfWorkEngine)this).RunUnitAsync(async (tx, ct) =>
        {
            foreach (var id in idList)
                if (await tx.Remove<T>(id, ct).ConfigureAwait(false))
                    removed++;
        }, cancellationToken).ConfigureAwait(false);
        return removed;
    }

    public async Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var updated = await this.ExecuteAsync(tableName, async session =>
        {
            var ok = await this.SetPropertyCoreAsync(session, tableName, resolvedId, typeName, jsonPath, value, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken).ConfigureAwait(false);
            if (ok)
                await this.AppendHistoryAsync(session, typeof(T), tableName, resolvedId, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
            return ok;
        }, cancellationToken).ConfigureAwait(false);
        if (updated)
            this.PublishChange<T>(DocumentChangeType.Updated, resolvedId, null);
        return updated;
    }

    public async Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var updated = await this.ExecuteAsync(tableName, async session =>
        {
            var ok = await this.RemovePropertyCoreAsync(session, tableName, resolvedId, typeName, jsonPath, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken).ConfigureAwait(false);
            if (ok)
                await this.AppendHistoryAsync(session, typeof(T), tableName, resolvedId, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
            return ok;
        }, cancellationToken).ConfigureAwait(false);
        if (updated)
            this.PublishChange<T>(DocumentChangeType.Updated, resolvedId, null);
        return updated;
    }

    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("get", typeof(T).Name, () => this.GetImpl(id, jsonTypeInfo, cancellationToken), r => r is null ? 0 : 1);

    Task<T?> GetImpl<T>(object id, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT Data FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.AddTenantParam(cmd);
            this.AppendGlobalFilters(cmd, typeInfo);

            this.Log(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return result is string json
                ? this.Materialize(json, typeInfo)
                : null;
        }, cancellationToken);
    }

    public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT Data FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.AddTenantParam(cmd);
            this.AppendGlobalFilters(cmd, typeInfo);

            this.Log(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (result is not string originalJson)
                return null;

            var modifiedJson = SerializeDocument(modified, typeInfo, this.jsonOptions);
            return JsonDiff.CreatePatch<T>(originalJson, modifiedJson, this.jsonOptions);
        }, cancellationToken);
    }

    // ── String-based query ──────────────────────────────────────────────

    public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("query", typeof(T).Name, () => this.QueryImpl(whereClause, jsonTypeInfo, parameters, cancellationToken), r => r.Count);

    Task<IReadOnlyList<T>> QueryImpl<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo, object? parameters, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName{GetTenantFilter() ?? ""} AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.AddTenantParam(cmd);
            BindParameters(cmd, parameters);

            this.Log(cmd.CommandText);
            return await ReadListAsync<T>(cmd, json => this.Materialize(json, typeInfo)!, cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    // ── String-based streaming ──────────────────────────────────────────

    async IAsyncEnumerable<T> ReadStreamAsync<T>(
        string tableName,
        Action<DbCommand> configureCommand,
        Func<string, T> deserialize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (this.sharedMode)
        {
            await this.sharedSemaphore!.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await this.EnsureSharedConnectionInitializedAsync(ct).ConfigureAwait(false);
                var session = new DocumentStoreSession(this.sharedConnection!);
                await this.EnsureTableInitializedAsync(session, tableName, ct).ConfigureAwait(false);

                await using var cmd = session.CreateCommand();
                configureCommand(cmd);
                this.Log(cmd.CommandText);
                await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    yield return deserialize(reader.GetString(0));
            }
            finally
            {
                this.sharedSemaphore.Release();
            }
        }
        else
        {
            await using var conn = this.provider.CreateConnection();
            await conn.OpenAsync(ct).ConfigureAwait(false);
            await this.provider.InitializeConnectionAsync(conn, ct).ConfigureAwait(false);
            var session = new DocumentStoreSession(conn);
            await this.EnsureTableInitializedAsync(session, tableName, ct).ConfigureAwait(false);

            await using var cmd = session.CreateCommand();
            configureCommand(cmd);
            this.Log(cmd.CommandText);
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                yield return deserialize(reader.GetString(0));
        }
    }

    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.TrackStream("query_stream", typeof(T).Name, this.QueryStreamImpl(whereClause, jsonTypeInfo, parameters, cancellationToken), cancellationToken);

    IAsyncEnumerable<T> QueryStreamImpl<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo, object? parameters, CancellationToken cancellationToken) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var tableName = this.ResolveTableName<T>();
        return this.ReadStreamAsync<T>(
            tableName,
            cmd =>
            {
                var sql = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName{GetTenantFilter() ?? ""} AND ({whereClause})";
                cmd.CommandText = sql + ";";
                AddParameter(cmd, "@typeName", typeName);
                this.AddTenantParam(cmd);
                BindParameters(cmd, parameters);
            },
            json => this.Materialize(json, typeInfo)!,
            cancellationToken);
    }

    // ── Count / Remove / Clear ──────────────────────────────────────────

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("count", typeof(T).Name, () => this.CountImpl<T>(whereClause, parameters, cancellationToken), r => r);

    Task<int> CountImpl<T>(string? whereClause, object? parameters, CancellationToken cancellationToken) where T : class
    {
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            var sql = $"SELECT COUNT(*) FROM {Qt(tableName)} WHERE TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            if (!string.IsNullOrWhiteSpace(whereClause))
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.AddTenantParam(cmd);
            BindParameters(cmd, parameters);
            this.AppendGlobalFilters<T>(cmd, null);

            this.Log(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return Convert.ToInt32(result);
        }, cancellationToken);
    }

    public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("remove", typeof(T).Name, () => this.RemoveImpl<T>(id, cancellationToken), r => r ? 1 : 0);

    async Task<bool> RemoveImpl<T>(object id, CancellationToken cancellationToken) where T : class
    {
        if (this.RequiresImplicitUnit())
        {
            var deleted = false;
            await ((IUnitOfWorkEngine)this).RunUnitAsync(async (s, ct) => deleted = await s.Remove<T>(id, ct).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
            return deleted;
        }
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var typeNameForCtx = this.ResolveTypeName<T>();
        var ctx = this.NewWriteContext<T>(DocumentOperation.Delete, typeNameForCtx, id, null);
        if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
            return ctx!.CancelResult;

        // The SQL, sidecar cleanup and history append are shared with the JSON-collection surface; only the
        // typed id resolution, interceptor context and change publication stay here.
        var removed = await this.RemoveRowCoreAsync(
            this.BuildTarget(typeof(T)),
            resolvedId,
            cmd => this.AppendGlobalFilters<T>(cmd, null),
            () => this.RunAfterWriteAsync(ctx, id, null, cancellationToken),
            cancellationToken).ConfigureAwait(false);

        if (removed)
            this.PublishChange<T>(DocumentChangeType.Removed, resolvedId, null);
        return removed;
    }

    public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("clear", typeof(T).Name, () => this.ClearImpl<T>(cancellationToken), r => r);

    async Task<int> ClearImpl<T>(CancellationToken cancellationToken) where T : class
    {
        var tableName = this.ResolveTableName<T>();
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;

        // A filtered Clear deletes only the matching rows, but the by-type spatial/vector sidecar clear can't
        // target that subset — so a bulk delete would orphan the sidecar rows of the deleted docs (or, if we
        // cleared by type, wrongly drop the sidecars of the surviving ones). Remove each matching doc through
        // the single-doc path, which purges its own sidecar rows.
        if (hasFilters
            && (this.options.ResolveSpatialMapping(typeof(T)) != null
                || this.options.ResolveVectorMapping(typeof(T)) != null
                || this.options.ResolveBlobMappings(typeof(T)).Count > 0))
        {
            var accessor = this.idCache.GetOrCreate<T>(null);
            var matches = await this.Query<T>().ToList().ConfigureAwait(false);
            var removed = 0;
            foreach (var doc in matches)
                if (await this.Remove<T>(accessor.GetIdAsString(doc), cancellationToken).ConfigureAwait(false))
                    removed++;
            return removed;
        }
        var bulkCtx = this.NewBulkContext<T>(DocumentOperation.Clear, this.ResolveTypeName<T>(), null, null);
        if (!await this.RunBeforeBulkAsync(bulkCtx, cancellationToken).ConfigureAwait(false))
            return bulkCtx!.CancelAffected;
        var deleted = await this.ExecuteAsync(tableName, async session =>
        {
            var typeName = this.ResolveTypeName<T>();

            await using var cmd = session.CreateCommand();
            var sql = $"DELETE FROM {Qt(tableName)} WHERE TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            this.AddTenantParam(cmd);
            this.AppendGlobalFilters<T>(cmd, null);

            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (rows > 0 && !hasFilters)
            {
                await this.SpatialClearAsync(session, typeof(T), tableName, typeName, cancellationToken).ConfigureAwait(false);
                await this.VectorClearAsync(session, typeof(T), tableName, typeName, cancellationToken).ConfigureAwait(false);
                await this.BlobClearAsync(session, typeof(T), tableName, typeName, cancellationToken).ConfigureAwait(false);
            }
            return rows;
        }, cancellationToken).ConfigureAwait(false);
        await this.RunAfterBulkAsync(bulkCtx, deleted, cancellationToken).ConfigureAwait(false);
        if (deleted > 0)
            this.PublishChange<T>(DocumentChangeType.Cleared, "", null);
        return deleted;
    }

    /// <inheritdoc />
    public async Task ClearAll(CancellationToken cancellationToken = default)
    {
        await this.ExecuteAsync(this.options.TableName, async session =>
        {
            var tables = new List<string>();
            await using (var listCmd = session.CreateCommand())
            {
                listCmd.CommandText = this.provider.BuildListTablesSql();
                this.Log(listCmd.CommandText);
                await using var reader = await listCmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    tables.Add(reader.GetString(0));
            }

            foreach (var table in tables)
            {
                // Skip FTS5 shadow tables — they are managed by their virtual table and DELETE on them errors
                // ("database disk image is malformed"). Clearing the FTS virtual table (or the underlying
                // document table) maintains them automatically.
                if (IsFtsShadowTable(table))
                    continue;
                await using var deleteCmd = session.CreateCommand();
                deleteCmd.CommandText = $"DELETE FROM {Qt(table)};";
                this.Log(deleteCmd.CommandText);
                await deleteCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    static bool IsFtsShadowTable(string table)
        => table.EndsWith("_data", StringComparison.Ordinal)
        || table.EndsWith("_idx", StringComparison.Ordinal)
        || table.EndsWith("_docsize", StringComparison.Ordinal)
        || table.EndsWith("_content", StringComparison.Ordinal)
        || table.EndsWith("_config", StringComparison.Ordinal);

    // ── Transaction ─────────────────────────────────────────────────────
    // The unit of work is now IDocumentSession (store.OpenSession()). Explicit transactions are provided by
    // IExplicitTransactionEngine (DocumentStore.Session.cs). Concrete methods (not just the IDocumentStore
    // default) so `concreteStore.OpenSession()` resolves without an interface cast.

    /// <inheritdoc />
    public IDocumentSession OpenSession() => new DocumentSession(this, null, null);
    /// <inheritdoc />
    public IDocumentSession OpenSession(IServiceProvider scope) => new DocumentSession(this, scope, null);

    // Internal transaction engine. The single place a transaction opens — every write that needs
    // grouping (UnitOfWork.SaveChanges) and the convenience methods route their atomic work through
    // here. Not public: see IUnitOfWorkEngine.
    // A user-initiated UnitOfWork emits one "transaction" span; the guard makes the inner writes span-free (an
    // implicit one-op unit nested inside a public op emits nothing at all — the public op already recorded it).
    Task IUnitOfWorkEngine.RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
        => this.tracker.Track("transaction", "(transaction)", () => this.RunUnitImpl(work, cancellationToken));

    async Task RunUnitImpl(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        // When a scoped interceptor is registered but no ambient scope is flowing (raw singleton store, Orleans
        // grain write, background worker), open ONE fresh child scope for the whole unit and flow it as the
        // ambient DI scope — so every write in the unit shares it (not one scope per row). If a DocumentContext
        // already flowed a scope, honor it. No-op entirely when nothing needs a scope (hot path untouched).
        Microsoft.Extensions.DependencyInjection.IServiceScope? fallbackScope = null;
        IDisposable? servicesFlow = null;
        if (DocumentOperationScope.CurrentServices == null && this.scopeFactory != null && this.options.Interceptors.HasDiInterceptors)
        {
            fallbackScope = this.scopeFactory.CreateScope();
            servicesFlow = DocumentOperationScope.EnterServices(fallbackScope.ServiceProvider);
        }
        try
        {
            // Buffer change notifications and only emit them once the transaction commits.
            var pendingChanges = new List<Action>();
            await this.ExecuteAsync(this.options.TableName, async session =>
            {
                // Pin the session's connection/transaction for the duration of the work so every
                // op runs on the same physical connection.
                await using var transaction = await session.Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    var txStore = new TransactionalDocumentStore(session.Connection, transaction, this.options, this.provider, this.jsonOptions, this.logging, this.idCache, this.tableInitTasks, this.broadcaster, pendingChanges, this);
                    await work(txStore, cancellationToken).ConfigureAwait(false);
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    pendingChanges.Clear();
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    throw;
                }
            }, cancellationToken).ConfigureAwait(false);

            // Emit outside the store lock so observer callbacks can re-enter the store safely.
            foreach (var emit in pendingChanges)
                emit();
        }
        finally
        {
            servicesFlow?.Dispose();
            if (fallbackScope is IAsyncDisposable asyncScope)
                await asyncScope.DisposeAsync().ConfigureAwait(false);
            else
                fallbackScope?.Dispose();
        }
    }

    // ── Index management ────────────────────────────────────────────────

    public Task CreateIndexAsync<T>(Expression<Func<T, object>> expression, JsonTypeInfo<T> jsonTypeInfo, CancellationToken cancellationToken = default) where T : class
    {
        var jsonPath = IndexExpressionHelper.ResolveJsonPath(expression, this.jsonOptions, jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var tableName = this.ResolveTableName<T>();
        var indexName = IndexExpressionHelper.BuildIndexName(typeName, jsonPath);

        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = this.provider.BuildCreateJsonIndexSql(indexName, tableName, jsonPath, typeName);
            this.Log(cmd.CommandText);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    /// <summary>
    /// Creates a composite (multi-column) JSON expression index over the given property chains.
    /// Pass a single expression for a normal single-column index.
    /// </summary>
    public Task CreateIndexAsync<T>(
        JsonTypeInfo<T> jsonTypeInfo,
        params IEnumerable<Expression<Func<T, object>>> expressions) where T : class
        => this.CreateIndexAsync(jsonTypeInfo, CancellationToken.None, expressions);

    public Task CreateIndexAsync<T>(
        JsonTypeInfo<T> jsonTypeInfo,
        CancellationToken cancellationToken,
        params IEnumerable<Expression<Func<T, object>>> expressions) where T : class
    {
        var paths = expressions
            .Select(e => IndexExpressionHelper.ResolveJsonPath(e, this.jsonOptions, jsonTypeInfo))
            .ToList();
        if (paths.Count == 0)
            throw new ArgumentException("At least one indexed expression is required.", nameof(expressions));

        var typeName = this.ResolveTypeName<T>();
        var tableName = this.ResolveTableName<T>();
        var indexName = IndexExpressionHelper.BuildIndexName(typeName, paths);

        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = this.provider.BuildCreateJsonIndexSql(indexName, tableName, paths, typeName);
            this.Log(cmd.CommandText);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    public Task DropIndexAsync<T>(Expression<Func<T, object>> expression, JsonTypeInfo<T> jsonTypeInfo, CancellationToken cancellationToken = default) where T : class
    {
        var jsonPath = IndexExpressionHelper.ResolveJsonPath(expression, this.jsonOptions, jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var tableName = this.ResolveTableName<T>();
        var indexName = IndexExpressionHelper.BuildIndexName(typeName, jsonPath);

        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = this.provider.BuildDropIndexSql(indexName, tableName);
            this.Log(cmd.CommandText);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    /// <summary>
    /// Drops the composite (multi-column) JSON index whose key matches the given expressions, in order.
    /// </summary>
    public Task DropIndexAsync<T>(
        JsonTypeInfo<T> jsonTypeInfo,
        params IEnumerable<Expression<Func<T, object>>> expressions) where T : class
        => this.DropIndexAsync(jsonTypeInfo, CancellationToken.None, expressions);

    public Task DropIndexAsync<T>(
        JsonTypeInfo<T> jsonTypeInfo,
        CancellationToken cancellationToken,
        params IEnumerable<Expression<Func<T, object>>> expressions) where T : class
    {
        var paths = expressions
            .Select(e => IndexExpressionHelper.ResolveJsonPath(e, this.jsonOptions, jsonTypeInfo))
            .ToList();
        if (paths.Count == 0)
            throw new ArgumentException("At least one indexed expression is required.", nameof(expressions));

        var typeName = this.ResolveTypeName<T>();
        var tableName = this.ResolveTableName<T>();
        var indexName = IndexExpressionHelper.BuildIndexName(typeName, paths);

        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = this.provider.BuildDropIndexSql(indexName, tableName);
            this.Log(cmd.CommandText);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    public Task DropAllIndexesAsync<T>(CancellationToken cancellationToken = default) where T : class
    {
        var typeName = this.ResolveTypeName<T>();
        var tableName = this.ResolveTableName<T>();
        var sanitizedType = typeName.Replace('.', '_');
        var prefix = $"idx_json_{sanitizedType}_%";

        return this.ExecuteAsync(tableName, async session =>
        {
            await using var queryCmd = session.CreateCommand();
            queryCmd.CommandText = this.provider.BuildListJsonIndexesSql(tableName, prefix);
            AddParameter(queryCmd, "@prefix", prefix);

            this.Log(queryCmd.CommandText);
            var indexNames = new List<string>();
            await using (var reader = await queryCmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    indexNames.Add(reader.GetString(0));
            }

            foreach (var indexName in indexNames)
            {
                await using var dropCmd = session.CreateCommand();
                dropCmd.CommandText = this.provider.BuildDropIndexSql(indexName, tableName);
                this.Log(dropCmd.CommandText);
                await dropCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }, cancellationToken);
    }

    // ── Static helpers ──────────────────────────────────────────────────

    // The default options used when the caller supplies no JsonSerializerOptions. In reflection mode we
    // attach a DefaultJsonTypeInfoResolver up front so metadata is available immediately (e.g. when
    // materializing a computed column at table init) rather than only after the first JsonSerializer call
    // lazily attaches one. In strict mode (UseReflectionFallback = false) we leave the resolver null so a
    // query against an unregistered type still fails fast with a clear error.
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "The reflection resolver is only attached when UseReflectionFallback is true (the non-AOT path).")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "The reflection resolver is only attached when UseReflectionFallback is true (the non-AOT path).")]
    internal static JsonSerializerOptions CreateDefaultJsonOptions(bool useReflectionFallback) => new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        TypeInfoResolver = useReflectionFallback ? new DefaultJsonTypeInfoResolver() : null
    };

    static JsonTypeInfo<T>? FindTypeInfo<T>(JsonTypeInfo<T>? provided, JsonSerializerOptions options, bool useReflectionFallback)
    {
        if (provided != null)
            return provided;

        if (options.TryGetTypeInfo(typeof(T), out var info) && info is JsonTypeInfo<T> typed)
            return typed;

        if (!useReflectionFallback)
            throw new InvalidOperationException(
                $"No JsonTypeInfo registered for type '{typeof(T).FullName}'. " +
                $"Register it in your JsonSerializerContext or pass a JsonTypeInfo<{typeof(T).Name}> explicitly.");

        return null;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    static string SerializeDocument<T>(T value, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? JsonSerializer.Serialize(value, typeInfo) : JsonSerializer.Serialize(value, options);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    static T? DeserializeDocument<T>(string json, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
        => typeInfo != null ? JsonSerializer.Deserialize(json, typeInfo) : JsonSerializer.Deserialize<T>(json, options);

    /// <summary>
    /// Deserialize a document and stamp blob loaders onto it, so metadata-only blobs can self-load. The
    /// single materialization seam for the store's non-LINQ read paths (LINQ goes through
    /// <c>DocumentQuery&lt;T&gt;.Deserialize</c>, which stamps identically). No-op stamping for non-blob types.
    /// </summary>
    T? Materialize<T>(string json, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var document = DeserializeDocument(json, typeInfo, this.jsonOptions);
        if (document != null)
            this.AttachBlobLoaders(document);
        return document;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    static string ResolvePropertyPath<T>(Expression<Func<T, object>> property, JsonSerializerOptions options, JsonTypeInfo<T>? typeInfo)
        => typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, options, typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, options);

    [UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "Reflection over an anonymous-type parameter bag. DAM cannot be applied to an object "
                      + "parameter (IL2098), so this branch is genuinely not trim-safe: under trimming/AOT pass the "
                      + "IDictionary<string, object?> overload, which takes the fully-analyzable path above.")]
    static void BindParameters(DbCommand cmd, object? parameters)
    {
        if (parameters is null)
            return;

        if (parameters is IDictionary<string, object?> dict)
        {
            foreach (var kvp in dict)
            {
                var paramName = kvp.Key.StartsWith('@') ? kvp.Key : "@" + kvp.Key;
                AddParameter(cmd, paramName, kvp.Value ?? DBNull.Value);
            }
            return;
        }

        foreach (var prop in parameters.GetType().GetProperties())
        {
            var value = prop.GetValue(parameters);
            AddParameter(cmd, "@" + prop.Name, value ?? DBNull.Value);
        }
    }

    static void AddParameter(DbCommand cmd, string name, object? value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(p);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Only serializes System.String which has a built-in converter.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Only serializes System.String which has a built-in converter.")]
    public static string ToJsonLiteral(object? value) => value switch
    {
        null => "null",
        bool b => b ? "true" : "false",
        string s => JsonSerializer.Serialize(s),
        // Scalars that live in the document as JSON *strings* have to be quoted and formatted exactly the way a
        // normal document write serializes them — an invariant ToString() here yields malformed JSON (the whole
        // json_set statement is then rejected) and, where it doesn't, a format the ISO-8601 string comparisons
        // used for date predicates cannot match.
        DateTime dt => JsonSerializer.Serialize(dt),
        DateTimeOffset dto => JsonSerializer.Serialize(dto),
        DateOnly d => JsonSerializer.Serialize(d),
        TimeOnly t => JsonSerializer.Serialize(t),
        TimeSpan ts => JsonSerializer.Serialize(ts),
        Guid g => JsonSerializer.Serialize(g),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "null"
    };

    static string StripNullProperties(string json)
    {
        var node = JsonNode.Parse(json);
        if (node is not JsonObject obj)
            return json;

        StripNullsRecursive(obj);
        return obj.ToJsonString();
    }

    // Drops a (dotted) property from a serialized JSON object — used to keep an unset version field out of a
    // partial merge patch so the merge preserves the stored version rather than clobbering it.
    static string RemoveJsonProperty(string json, string dottedPath)
    {
        if (JsonNode.Parse(json) is not JsonObject obj)
            return json;

        var segments = dottedPath.Split('.');
        JsonObject? cursor = obj;
        for (var i = 0; i < segments.Length - 1 && cursor != null; i++)
            cursor = cursor[segments[i]] as JsonObject;
        cursor?.Remove(segments[^1]);
        return obj.ToJsonString();
    }

    // Recursive: a null at any depth gets dropped before the patch reaches the merge step.
    // Otherwise RFC 7396 deep-merge providers (SQLite json_patch, MySQL JSON_MERGE_PATCH) would
    // treat the null as "delete this field" and silently wipe nested defaults the user did not
    // intend to clear (e.g. `new Patch { Inner = new InnerType { City = "X" } }` would null out
    // Inner.Street/State because their C# defaults serialized as null).
    static void StripNullsRecursive(JsonObject obj)
    {
        foreach (var key in obj.Where(kv => kv.Value is null).Select(kv => kv.Key).ToList())
            obj.Remove(key);

        foreach (var kv in obj)
            if (kv.Value is JsonObject child)
                StripNullsRecursive(child);
    }

    static async Task<IReadOnlyList<T>> ReadListAsync<T>(DbCommand cmd, Func<string, T> deserialize, CancellationToken ct)
    {
        var list = new List<T>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var json = reader.GetString(0);
            list.Add(deserialize(json));
        }
        return list;
    }

    // ── Spatial queries ──────────────────────────────────────────────────

    public Task<IReadOnlyList<SpatialResult<T>>> WithinRadius<T>(
        GeoPoint center,
        double radiusMeters,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        if (!this.provider.SupportsSpatial)
            throw new NotSupportedException("Spatial queries are not supported by this provider.");

        var typeInfo = FindTypeInfo<T>(null);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var box = GeoMath.BoundingBox(center, radiusMeters);

        return this.ExecuteAsync(tableName, async session =>
        {
            string? additionalWhere = null;
            Dictionary<string, object?>? filterParams = null;

            if (filter != null)
            {
                var translated = JsonExpressionVisitor.Translate(filter, typeInfo!, this.provider);
                additionalWhere = translated.WhereClause;
                filterParams = translated.Parameters;
            }

            var sql = this.provider.BuildSpatialBoundingBoxQuerySql(tableName, additionalWhere)!;
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@minLat", box.MinLatitude);
            AddParameter(cmd, "@maxLat", box.MaxLatitude);
            AddParameter(cmd, "@minLng", box.MinLongitude);
            AddParameter(cmd, "@maxLng", box.MaxLongitude);

            if (filterParams != null)
            {
                foreach (var kvp in filterParams)
                    AddParameter(cmd, kvp.Key, kvp.Value ?? DBNull.Value);
            }

            this.Log(cmd.CommandText);
            var candidates = await ReadListAsync(cmd, json => this.Materialize(json, typeInfo)!, cancellationToken).ConfigureAwait(false);

            var mapping = this.options.ResolveSpatialMapping(typeof(T))!;
            var results = new List<SpatialResult<T>>();
            foreach (var doc in candidates)
            {
                var stored = mapping.ResolveGeometry(doc);
                if (stored is not null)
                {
                    var distance = Internal.Spatial.GeoDistance.PointToGeometry(center, stored);
                    if (distance <= radiusMeters)
                        results.Add(new SpatialResult<T> { Document = doc, DistanceMeters = distance });
                }
            }

            results.Sort((a, b) => a.DistanceMeters.CompareTo(b.DistanceMeters));
            return (IReadOnlyList<SpatialResult<T>>)results;
        }, cancellationToken);
    }

    public Task<IReadOnlyList<T>> WithinBoundingBox<T>(
        GeoBoundingBox box,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        if (!this.provider.SupportsSpatial)
            throw new NotSupportedException("Spatial queries are not supported by this provider.");

        var typeInfo = FindTypeInfo<T>(null);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();

        return this.ExecuteAsync(tableName, async session =>
        {
            string? additionalWhere = null;
            Dictionary<string, object?>? filterParams = null;

            if (filter != null)
            {
                var translated = JsonExpressionVisitor.Translate(filter, typeInfo!, this.provider);
                additionalWhere = translated.WhereClause;
                filterParams = translated.Parameters;
            }

            var sql = this.provider.BuildSpatialBoundingBoxQuerySql(tableName, additionalWhere)!;
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@minLat", box.MinLatitude);
            AddParameter(cmd, "@maxLat", box.MaxLatitude);
            AddParameter(cmd, "@minLng", box.MinLongitude);
            AddParameter(cmd, "@maxLng", box.MaxLongitude);

            if (filterParams != null)
            {
                foreach (var kvp in filterParams)
                    AddParameter(cmd, kvp.Key, kvp.Value ?? DBNull.Value);
            }

            this.Log(cmd.CommandText);
            return await ReadListAsync(cmd, json => this.Materialize(json, typeInfo)!, cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    public Task<IReadOnlyList<SpatialResult<T>>> NearestNeighbors<T>(
        GeoPoint center,
        int count,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        if (!this.provider.SupportsSpatial)
            throw new NotSupportedException("Spatial queries are not supported by this provider.");

        var typeInfo = FindTypeInfo<T>(null);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();

        return this.ExecuteAsync(tableName, async session =>
        {
            var radiusMeters = 10_000.0;
            List<SpatialResult<T>> results;

            string? additionalWhere = null;
            Dictionary<string, object?>? filterParams = null;

            if (filter != null)
            {
                var translated = JsonExpressionVisitor.Translate(filter, typeInfo!, this.provider);
                additionalWhere = translated.WhereClause;
                filterParams = translated.Parameters;
            }

            var mapping = this.options.ResolveSpatialMapping(typeof(T))!;
            var sql = this.provider.BuildSpatialBoundingBoxQuerySql(tableName, additionalWhere)!;

            do
            {
                var box = GeoMath.BoundingBox(center, radiusMeters);

                await using var cmd = session.CreateCommand();
                cmd.CommandText = sql + ";";
                AddParameter(cmd, "@typeName", typeName);
                AddParameter(cmd, "@minLat", box.MinLatitude);
                AddParameter(cmd, "@maxLat", box.MaxLatitude);
                AddParameter(cmd, "@minLng", box.MinLongitude);
                AddParameter(cmd, "@maxLng", box.MaxLongitude);

                if (filterParams != null)
                {
                    foreach (var kvp in filterParams)
                        AddParameter(cmd, kvp.Key, kvp.Value ?? DBNull.Value);
                }

                this.Log(cmd.CommandText);
                var candidates = await ReadListAsync(cmd, json => this.Materialize(json, typeInfo)!, cancellationToken).ConfigureAwait(false);

                results = new List<SpatialResult<T>>();
                foreach (var doc in candidates)
                {
                    var stored = mapping.ResolveGeometry(doc);
                    if (stored is not null)
                    {
                        var distance = Internal.Spatial.GeoDistance.PointToGeometry(center, stored);
                        results.Add(new SpatialResult<T> { Document = doc, DistanceMeters = distance });
                    }
                }

                if (results.Count >= count)
                    break;

                radiusMeters *= 2;
            }
            while (radiusMeters <= 20_000_000);

            results.Sort((a, b) => a.DistanceMeters.CompareTo(b.DistanceMeters));
            if (results.Count > count)
                results.RemoveRange(count, results.Count - count);

            return (IReadOnlyList<SpatialResult<T>>)results;
        }, cancellationToken);
    }

    // ── Vector queries ────────────────────────────────────────────────────

    public Task<IReadOnlyList<VectorResult<T>>> NearestVectors<T>(
        ReadOnlyMemory<float> query,
        int k,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        if (!this.provider.SupportsVector)
            throw new NotSupportedException("Vector queries are not supported by this provider.");

        var mapping = this.options.ResolveVectorMapping(typeof(T))
            ?? throw new InvalidOperationException(
                $"No vector mapping is registered for '{typeof(T).Name}'. " +
                "Call DocumentStoreOptions.MapVectorProperty<T>(...) at startup.");

        if (query.Length != mapping.Dimensions)
            throw new ArgumentException(
                $"Query vector has {query.Length} dimensions; mapping expects {mapping.Dimensions}.",
                nameof(query));

        if (k <= 0)
            throw new ArgumentOutOfRangeException(nameof(k), "k must be > 0.");

        var typeInfo = FindTypeInfo<T>(null);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();

        return this.ExecuteAsync(tableName, async session =>
        {
            string? additionalWhere = null;
            Dictionary<string, object?>? filterParams = null;
            if (filter != null)
            {
                var translated = JsonExpressionVisitor.Translate(filter, typeInfo!, this.provider);
                additionalWhere = translated.WhereClause;
                filterParams = translated.Parameters;
            }

            var (sql, vecParams) = this.provider.BuildVectorSearchSql(tableName, typeName, mapping, query, k, additionalWhere);
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql;
            AddParameter(cmd, "@typeName", typeName);
            foreach (var kv in vecParams)
                AddParameter(cmd, kv.Key, kv.Value);
            if (filterParams != null)
            {
                foreach (var kvp in filterParams)
                    AddParameter(cmd, kvp.Key, kvp.Value ?? DBNull.Value);
            }
            this.Log(cmd.CommandText);

            var results = new List<VectorResult<T>>();
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var json = reader.GetString(0);
                var score = reader.IsDBNull(1) ? float.NaN : Convert.ToSingle(reader.GetValue(1));
                // Relational providers rank DotProduct with the DB's negated inner-product "distance" operator
                // (so ascending order = nearest). VectorResult.Score is documented as the raw inner product
                // (higher = closer), so negate it back — the result order is unchanged.
                if (mapping.Metric == VectorDistance.DotProduct)
                    score = -score;
                var doc = this.Materialize(json, typeInfo)!;
                results.Add(new VectorResult<T> { Document = doc, Score = score });
            }
            return (IReadOnlyList<VectorResult<T>>)results;
        }, cancellationToken);
    }

    // ── Full-text queries ─────────────────────────────────────────────────

    public bool SupportsFullText => this.provider.SupportsFullText;

    public Task<IReadOnlyList<FullTextResult<T>>> FullTextSearch<T>(
        string searchText,
        int maxResults = 50,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        if (!this.provider.SupportsFullText)
            throw new NotSupportedException("Full-text queries are not supported by this provider.");

        var mapping = this.options.ResolveFullTextMapping(typeof(T))
            ?? throw new InvalidOperationException(
                $"No full-text mapping is registered for '{typeof(T).Name}'. " +
                $"Call DocumentStoreOptions.MapFullTextProperty<{typeof(T).Name}>(...) at startup.");

        ArgumentException.ThrowIfNullOrEmpty(searchText);
        if (maxResults <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxResults), "maxResults must be > 0.");

        var typeInfo = FindTypeInfo<T>(null);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();

        return this.ExecuteAsync(tableName, async session =>
        {
            // Snapshot-index providers (DuckDB fts) must rebuild before each query.
            if (this.provider.FullTextIndexRequiresRebuild)
            {
                foreach (var ddl in this.provider.BuildCreateFullTextSql(tableName, typeName, mapping))
                {
                    await using var rebuild = session.CreateCommand();
                    rebuild.CommandText = ddl;
                    this.Log(rebuild.CommandText);
                    await rebuild.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            string? additionalWhere = null;
            Dictionary<string, object?>? filterParams = null;
            if (filter != null)
            {
                var translated = JsonExpressionVisitor.Translate(filter, typeInfo!, this.provider);
                additionalWhere = translated.WhereClause;
                filterParams = translated.Parameters;
            }

            var (sql, ftsParams) = this.provider.BuildFullTextSearchSql(tableName, typeName, mapping, searchText, maxResults, additionalWhere);
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql;
            AddParameter(cmd, "@typeName", typeName);
            foreach (var kv in ftsParams)
                AddParameter(cmd, kv.Key, kv.Value);
            if (filterParams != null)
            {
                foreach (var kvp in filterParams)
                    AddParameter(cmd, kvp.Key, kvp.Value ?? DBNull.Value);
            }
            this.Log(cmd.CommandText);

            var results = new List<FullTextResult<T>>();
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var json = reader.GetString(0);
                var score = reader.IsDBNull(1) ? double.NaN : Convert.ToDouble(reader.GetValue(1));
                var doc = this.Materialize(json, typeInfo)!;
                results.Add(new FullTextResult<T> { Document = doc, Score = score });
            }
            return (IReadOnlyList<FullTextResult<T>>)results;
        }, cancellationToken);
    }

    // ── Temporal history queries ────────────────────────────────────────
    // These live on the concrete DocumentStore (not IDocumentStore), matching the Backup /
    // ClearAllAsync precedent for provider-specific surface.

    void EnsureTemporal<T>()
    {
        if (!this.provider.SupportsTemporal)
            throw new NotSupportedException(
                $"The configured provider '{this.provider.GetType().Name}' does not support temporal history.");
        if (this.options.ResolveTemporalMapping(typeof(T)) == null)
            throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' is not configured for temporal history. " +
                $"Call options.MapTemporal<{typeof(T).Name}>() during setup.");
    }

    /// <summary>
    /// Returns every recorded version of a document, oldest first. Requires
    /// <see cref="DocumentStoreOptions.MapTemporal{T}"/> for <typeparamref name="T"/>.
    /// </summary>
    public Task<IReadOnlyList<DocumentVersion<T>>> History<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("history", typeof(T).Name, () => this.HistoryImpl(id, jsonTypeInfo, cancellationToken), r => r.Count);

    Task<IReadOnlyList<DocumentVersion<T>>> HistoryImpl<T>(object id, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        return this.ReadVersionsAsync(tableName, this.ApplyTenantToHistorySql(this.provider.BuildHistorySelectSql(tableName)), cmd =>
        {
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", typeName);
            this.AddTenantParam(cmd);
        }, typeInfo, cancellationToken);
    }

    /// <summary>
    /// Returns every version authored by <paramref name="actor"/> across all documents of the type,
    /// oldest first — a per-user audit trail. Requires <see cref="DocumentStoreOptions.MapTemporal{T}"/>
    /// and a configured <see cref="TemporalOptions.CaptureActor"/>.
    /// </summary>
    public Task<IReadOnlyList<DocumentVersion<T>>> ChangesByActor<T>(string actor, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("changes_by_actor", typeof(T).Name, () => this.ChangesByActorImpl(actor, jsonTypeInfo, cancellationToken), r => r.Count);

    Task<IReadOnlyList<DocumentVersion<T>>> ChangesByActorImpl<T>(string actor, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        ArgumentNullException.ThrowIfNull(actor);
        this.EnsureTemporal<T>();
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        return this.ReadVersionsAsync(tableName, this.ApplyTenantToHistorySql(this.provider.BuildHistoryByActorSql(tableName)), cmd =>
        {
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@actor", actor);
            this.AddTenantParam(cmd);
        }, typeInfo, cancellationToken);
    }

    /// <summary>
    /// Returns every version whose <see cref="DocumentVersion{T}.ValidFrom"/> falls in
    /// <c>[from, to)</c> across all documents of the type, oldest first — an audit log over a time
    /// window. Requires <see cref="DocumentStoreOptions.MapTemporal{T}"/>.
    /// </summary>
    public Task<IReadOnlyList<DocumentVersion<T>>> ChangesBetween<T>(DateTimeOffset from, DateTimeOffset to, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.tracker.Track("changes_between", typeof(T).Name, () => this.ChangesBetweenImpl(from, to, jsonTypeInfo, cancellationToken), r => r.Count);

    Task<IReadOnlyList<DocumentVersion<T>>> ChangesBetweenImpl<T>(DateTimeOffset from, DateTimeOffset to, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var fromUtc = from.ToUniversalTime();
        var toUtc = to.ToUniversalTime();
        return this.ReadVersionsAsync(tableName, this.ApplyTenantToHistorySql(this.provider.BuildHistoryBetweenSql(tableName)), cmd =>
        {
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@fromTs", fromUtc);
            AddParameter(cmd, "@toTs", toUtc);
            this.AddTenantParam(cmd);
        }, typeInfo, cancellationToken);
    }

    /// <summary>
    /// Returns the live state of every document of the type as of <paramref name="asOf"/> — a
    /// fleet-wide point-in-time snapshot. Documents that did not yet exist, or had been removed, at
    /// that instant are excluded. Requires <see cref="DocumentStoreOptions.MapTemporal{T}"/>.
    /// </summary>
    public Task<IReadOnlyList<T>> AsOfAll<T>(DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var sql = this.ApplyTenantToHistorySql(this.provider.BuildHistoryAsOfAllSql(tableName));
        var asOfUtc = asOf.ToUniversalTime();
        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql;
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@asOf", asOfUtc);
            this.AddTenantParam(cmd);
            this.Log(cmd.CommandText);
            return await ReadListAsync(cmd, json => this.Materialize(json, typeInfo)!, cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    // Reads the standard 7-column version-row shape into DocumentVersion<T>:
    //   0 Id, 1 Version, 2 ValidFrom, 3 ValidTo, 4 Operation, 5 Actor, 6 Data
    Task<IReadOnlyList<DocumentVersion<T>>> ReadVersionsAsync<T>(string tableName, string sql, Action<DbCommand> bind, JsonTypeInfo<T>? typeInfo, CancellationToken ct) where T : class
        => this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql;
            bind(cmd);
            this.Log(cmd.CommandText);

            var list = new List<DocumentVersion<T>>();
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var doc = reader.IsDBNull(6) ? null : this.Materialize(reader.GetString(6), typeInfo);
                list.Add(new DocumentVersion<T>
                {
                    Id = reader.GetString(0),
                    Version = ReadInt64(reader, 1),
                    ValidFrom = ReadDateTimeOffset(reader, 2),
                    ValidTo = reader.IsDBNull(3) ? null : ReadDateTimeOffset(reader, 3),
                    Operation = Enum.Parse<TemporalOperation>(reader.GetString(4)),
                    Actor = reader.IsDBNull(5) ? null : reader.GetString(5),
                    Document = doc
                });
            }
            return (IReadOnlyList<DocumentVersion<T>>)list;
        }, ct);

    // History columns vary in CLR type across providers: Version comes back as Int64 on most engines but
    // Decimal on Oracle (NUMBER); ValidFrom/ValidTo come back as DateTimeOffset on SQLite/PostgreSQL/
    // MySQL/DuckDB but DateTime on SQL Server (DATETIME2). These coerce either shape uniformly. All
    // history timestamps are written in UTC, so an offset-less DateTime is interpreted as UTC.
    static long ReadInt64(DbDataReader reader, int ordinal)
        => Convert.ToInt64(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    static DateTimeOffset ReadDateTimeOffset(DbDataReader reader, int ordinal)
        => reader.GetValue(ordinal) switch
        {
            DateTimeOffset dto => dto,
            DateTime { Kind: DateTimeKind.Utc } dt => new DateTimeOffset(dt),
            DateTime { Kind: DateTimeKind.Local } dt => new DateTimeOffset(dt.ToUniversalTime()),
            DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
            var other => DateTimeOffset.Parse(Convert.ToString(other, CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
        };

    /// <summary>
    /// Returns the document's state as of <paramref name="asOf"/>, or null if it did not exist (or
    /// had been removed) at that instant. Requires <see cref="DocumentStoreOptions.MapTemporal{T}"/>.
    /// </summary>
    public Task<T?> AsOf<T>(object id, DateTimeOffset asOf, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var sql = this.ApplyTenantToHistorySql(this.provider.BuildHistoryAsOfSql(tableName)!);
        var asOfUtc = asOf.ToUniversalTime();
        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql;
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@asOf", asOfUtc);
            this.AddTenantParam(cmd);
            this.Log(cmd.CommandText);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                return null;
            return reader.IsDBNull(0) ? null : this.Materialize(reader.GetString(0), typeInfo);
        }, cancellationToken);
    }

    /// <summary>
    /// Restores a prior version as the current document state. This writes a new current version
    /// (it does not rewrite history): the document is re-inserted if it had been removed, otherwise
    /// overwritten. Returns the restored document, or null if the version does not exist (or was a
    /// removal tombstone). Requires <see cref="DocumentStoreOptions.MapTemporal{T}"/>.
    /// <para>
    /// <b>Contract when the live document had been removed:</b> the restore re-creates it as a fresh live
    /// row — its <c>CreatedAt</c> is stamped to the restore time and, if a version property is mapped, its
    /// version counter restarts at 1. Restoring is treated as beginning a new live lifecycle; the append-only
    /// history (including the original creation and the removal tombstone) is preserved and continues its own
    /// monotonic version sequence. When the live document still exists, the restore is an overwrite and the
    /// version bumps forward from the current value as a normal update would.
    /// </para>
    /// </summary>
    public async Task<T?> Restore<T>(object id, long version, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();

        var json = await this.ReadVersionDataAsync(tableName, resolvedId, typeName, version, cancellationToken).ConfigureAwait(false);
        var doc = json != null ? this.Materialize(json, typeInfo) : null;
        if (doc == null)
            return null;

        var versionMapping = this.options.ResolveVersionMapping(typeof(T));
        var current = await this.Get(id, typeInfo, cancellationToken).ConfigureAwait(false);
        // Blobs are not versioned — restore the fields from history but keep blobs as they are now, so the
        // persisted blob metadata never describes a superseded payload.
        this.RestampBlobsFromCurrent(doc, current);
        // A restore is a real document change; interceptors fire but are flagged Temporal so they can
        // distinguish it from a direct write.
        using (DocumentOperationScope.Push(DocumentOperationSource.Temporal))
        {
            if (current == null)
            {
                await this.Insert(doc, typeInfo, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // Align the optimistic-concurrency token to the live row so the restore (a deliberate
                // overwrite) isn't rejected as a stale write; Update then bumps it forward.
                versionMapping?.SetVersion(doc, versionMapping.GetVersion(current));
                await this.Update(doc, typeInfo, cancellationToken).ConfigureAwait(false);
            }
        }
        return doc;
    }

    /// <summary>
    /// Returns an RFC 6902 <see cref="JsonPatchDocument{T}"/> describing the change from
    /// <paramref name="fromVersion"/> to <paramref name="toVersion"/> of a document — the temporal
    /// analogue of <see cref="GetDiff{T}"/>. Returns null if either version is missing or is a
    /// removal tombstone (no body to diff). Requires <see cref="DocumentStoreOptions.MapTemporal{T}"/>.
    /// </summary>
    public async Task<JsonPatchDocument<T>?> GetDiffBetween<T>(object id, long fromVersion, long toVersion, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        this.EnsureTemporal<T>();
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();

        var fromJson = await this.ReadVersionDataAsync(tableName, resolvedId, typeName, fromVersion, cancellationToken).ConfigureAwait(false);
        var toJson = await this.ReadVersionDataAsync(tableName, resolvedId, typeName, toVersion, cancellationToken).ConfigureAwait(false);
        if (fromJson == null || toJson == null)
            return null;
        return JsonDiff.CreatePatch<T>(fromJson, toJson, this.jsonOptions);
    }

    Task<string?> ReadVersionDataAsync(string tableName, string id, string typeName, long version, CancellationToken ct)
    {
        var sql = this.ApplyTenantToHistorySql(this.provider.BuildHistoryVersionSql(tableName)!);
        return this.ExecuteAsync(tableName, async session =>
        {
            await using var cmd = session.CreateCommand();
            cmd.CommandText = sql;
            AddParameter(cmd, "@id", id);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@version", version);
            this.AddTenantParam(cmd);
            this.Log(cmd.CommandText);
            // null row -> null; tombstone (NULL Data column) -> DBNull -> null.
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return result as string;
        }, ct);
    }

    public void Dispose()
    {
        this.sharedConnection?.Dispose();
        this.sharedSemaphore?.Dispose();
        GC.SuppressFinalize(this);
    }

    // ── TransactionalDocumentStore ──────────────────────────────────────

    sealed class TransactionalDocumentStore : IDocumentStore, IQueryExecutor
    {
        readonly DbConnection connection;
        readonly DbTransaction transaction;
        readonly DocumentStoreOptions options;
        readonly IDatabaseProvider provider;
        readonly JsonSerializerOptions jsonOptions;
        readonly Action<string>? logging;
        readonly IdAccessorCache idCache;
        readonly ConcurrentDictionary<string, Lazy<Task>> tableInitTasks;
        readonly ChangeBroadcaster broadcaster;
        readonly List<Action> pendingChanges;
        readonly DocumentStore parent;

        public TransactionalDocumentStore(
            DbConnection connection,
            DbTransaction transaction,
            DocumentStoreOptions options,
            IDatabaseProvider provider,
            JsonSerializerOptions jsonOptions,
            Action<string>? logging,
            IdAccessorCache idCache,
            ConcurrentDictionary<string, Lazy<Task>> tableInitTasks,
            ChangeBroadcaster broadcaster,
            List<Action> pendingChanges,
            DocumentStore parent)
        {
            this.connection = connection;
            this.transaction = transaction;
            this.options = options;
            this.provider = provider;
            this.jsonOptions = jsonOptions;
            this.logging = logging;
            this.idCache = idCache;
            this.tableInitTasks = tableInitTasks;
            this.broadcaster = broadcaster;
            this.pendingChanges = pendingChanges;
            this.parent = parent;
        }

        // Spatial/vector sidecars live on the parent store; a unit-of-work write must maintain them too, using
        // this unit's connection + transaction so the sidecar row commits atomically with the document.
        DocumentStoreSession SidecarSession() => new(this.connection, this.transaction);
        Task SpatialSync<T>(string tableName, string id, string typeName, T document, CancellationToken ct) where T : class
            => this.parent.SpatialUpsertAsync(this.SidecarSession(), tableName, id, typeName, document, ct);
        Task VectorSync<T>(string tableName, string typeName, string id, T document, CancellationToken ct) where T : class
            => this.parent.VectorUpsertAsync(this.SidecarSession(), tableName, typeName, id, document, ct);

        IReadOnlyList<PreparedBlob> PrepareBlobs<T>(T document) where T : class
            => this.parent.PrepareBlobs(typeof(T), document);
        Task BlobSync<T>(string tableName, string id, string typeName, IReadOnlyList<PreparedBlob> blobs, bool prune, CancellationToken ct) where T : class
            => this.parent.BlobSyncAsync(this.SidecarSession(), typeof(T), tableName, id, typeName, blobs, prune, ct);
        Task BlobDelete(Type documentType, string tableName, string id, string typeName, CancellationToken ct)
            => this.parent.BlobDeleteAllAsync(this.SidecarSession(), documentType, tableName, id, typeName, ct);

        void QueueChange<T>(DocumentChangeType changeType, string id, T? document) where T : class
            => this.pendingChanges.Add(() => this.broadcaster.Publish(new DocumentChange<T> { ChangeType = changeType, Id = id, Document = document }));

        // Write-interceptor helpers — mirror DocumentStore so interceptors fire for writes inside a unit.
        DocumentWriteContext? NewWriteContext<T>(DocumentOperation op, string typeName, object? id, T? document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
        {
            var pipeline = this.options.Interceptors;
            if (!pipeline.HasPerDoc || DocumentOperationScope.Suppressed)
                return null;
            Func<object, string>? factory = document == null
                ? null
                : doc => SerializeDocument((T)doc, this.FindTypeInfo(jsonTypeInfo), this.jsonOptions);
            // ctx.Store = this transaction-bound store, so a hook's reads/writes ride the unit's connection+txn
            // (uncommitted-row visibility, atomic side effects). ctx.Services = the ambient scope (a request scope
            // via DocumentContext, or the unit's fallback child scope opened in RunUnitAsync).
            return pipeline.NewWrite(op, typeName, id, document, this, DocumentOperationScope.CurrentServices, factory);
        }

        // False when an interceptor cancelled the write — the caller skips it and reports ctx.CancelResult.
        Task<bool> RunBeforeWriteAsync(DocumentWriteContext? ctx, CancellationToken ct)
            => this.options.Interceptors.BeforeWrite(ctx, ct);

        Task RunAfterWriteAsync(DocumentWriteContext? ctx, object? id, int? version, CancellationToken ct)
            => this.options.Interceptors.AfterWrite(ctx, id, version, ct);

        Task AppendHistory(Type documentType, string tableName, string id, string typeName, TemporalOperation operation, string? providedJson, CancellationToken ct)
        {
            var mapping = this.options.ResolveTemporalMapping(documentType);
            return mapping == null
                ? Task.CompletedTask
                : AppendHistoryCoreAsync(this.CreateCommand, this.provider, mapping, tableName, id, typeName, operation, providedJson, this.logging, ct, this.options.TenantIdAccessor?.Invoke());
        }

        void Log(string sql) => this.logging?.Invoke(sql);

        string Qt(string tableName) => this.provider.QuoteTable(tableName);

        string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);

        string ResolveTableName<T>() => this.options.ResolveTableName(this.ResolveTypeName<T>());

        JsonTypeInfo<T>? FindTypeInfo<T>(JsonTypeInfo<T>? provided)
            => DocumentStore.FindTypeInfo(provided, this.jsonOptions, this.options.UseReflectionFallback);

        DbCommand CreateCommand()
        {
            var cmd = this.connection.CreateCommand();
            cmd.Transaction = this.transaction;
            return cmd;
        }

        async Task EnsureTableAsync(string tableName, CancellationToken ct)
        {
            if (this.options.SkipTableInitialization)
                return;

            // Inside a transaction we always run DDL on this pinned connection; the parent's
            // shared init cache still ensures we only do it once per table per process.
            Lazy<Task>? lazy = null;
            try
            {
                lazy = this.tableInitTasks.GetOrAdd(tableName,
                    _ => new Lazy<Task>(() => InitAsync(), LazyThreadSafetyMode.ExecutionAndPublication));
                await lazy.Value.ConfigureAwait(false);
            }
            catch
            {
                if (lazy != null)
                    ((ICollection<KeyValuePair<string, Lazy<Task>>>)this.tableInitTasks)
                        .Remove(new KeyValuePair<string, Lazy<Task>>(tableName, lazy));
                throw;
            }

            async Task InitAsync()
            {
                await using var cmd = this.CreateCommand();
                cmd.CommandText = this.provider.BuildCreateTableSql(tableName);
                this.Log(cmd.CommandText);
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

                await using var indexCmd = this.CreateCommand();
                indexCmd.CommandText = this.provider.BuildCreateTypenameIndexSql(tableName);
                this.Log(indexCmd.CommandText);
                try
                {
                    await indexCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Index may already exist — safe to ignore
                }

                var historySql = this.provider.SupportsTemporal && TableHasTemporalMapping(this.options, tableName)
                    ? this.provider.BuildCreateHistoryTableSql(tableName)
                    : null;
                if (historySql != null)
                {
                    await using var historyCmd = this.CreateCommand();
                    historyCmd.CommandText = historySql;
                    this.Log(historyCmd.CommandText);
                    await historyCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

                    foreach (var indexSql in this.provider.BuildCreateHistoryIndexesSql(tableName))
                    {
                        await using var idxCmd = this.CreateCommand();
                        idxCmd.CommandText = indexSql;
                        this.Log(idxCmd.CommandText);
                        try
                        {
                            await idxCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                        }
                        catch (Exception)
                        {
                            // Index may already exist — safe to ignore
                        }
                    }
                }

                // Both init paths share one tableInitTasks cache, so whichever runs first wins — the blob DDL
                // has to be here as well as in the parent's InitAsync or it is silently skipped.
                var blobSql = this.provider.MaxBlobSize > 0 && TableHasBlobMapping(this.options, tableName)
                    ? this.provider.BuildCreateBlobTableSql(tableName)
                    : null;
                if (blobSql != null)
                {
                    await using var blobCmd = this.CreateCommand();
                    blobCmd.CommandText = blobSql;
                    this.Log(blobCmd.CommandText);
                    await blobCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
            }
        }

        // ── IQueryExecutor ──────────────────────────────────────────────

        Task<TResult> IQueryExecutor.ExecuteAsync<TResult>(string tableName, Func<DocumentStoreSession, Task<TResult>> operation, CancellationToken ct)
        {
            return RunAsync();

            async Task<TResult> RunAsync()
            {
                await this.EnsureTableAsync(tableName, ct).ConfigureAwait(false);
                var session = new DocumentStoreSession(this.connection, this.transaction);
                return await operation(session).ConfigureAwait(false);
            }
        }

        IAsyncEnumerable<T> IQueryExecutor.ReadStreamAsync<T>(string tableName, Action<DbCommand> configure, Func<string, T> deserialize, CancellationToken ct)
            => ReadStreamInternalAsync(tableName, configure, deserialize, ct);

        string IQueryExecutor.ResolveTypeName<T>() => this.ResolveTypeName<T>();

        string IQueryExecutor.ResolveTableName<T>() => this.ResolveTableName<T>();

        IdAccessor<T> IQueryExecutor.GetIdAccessor<T>(JsonTypeInfo<T>? typeInfo) => this.idCache.GetOrCreate(typeInfo);

        void IQueryExecutor.AttachBlobLoaders<T>(T document) => this.parent.AttachBlobLoaders(document);

        JsonSerializerOptions IQueryExecutor.JsonOptions => this.jsonOptions;

        Action<string>? IQueryExecutor.Logging => this.logging;

        IDatabaseProvider IQueryExecutor.Provider => this.provider;

        string? IQueryExecutor.TenantFilter => this.options.TenantIdAccessor != null ? " AND TenantId = @tenantId" : null;

        void IQueryExecutor.AddTenantParameter(DbCommand cmd)
        {
            if (this.options.TenantIdAccessor != null)
                AddParameter(cmd, "@tenantId", this.options.TenantIdAccessor());
        }

        void IQueryExecutor.CollectTenantParameter(IDictionary<string, object?> parameters)
        {
            if (this.options.TenantIdAccessor != null)
                parameters["@tenantId"] = this.options.TenantIdAccessor();
        }

        ChangeBroadcaster? IQueryExecutor.Broadcaster => this.broadcaster;

        DocumentStoreOptions IQueryExecutor.Options => this.options;

        Task<IReadOnlyList<VectorResult<T>>> IQueryExecutor.NearestVectorsAsync<T>(
            ReadOnlyMemory<float> query, int k, Expression<Func<T, bool>>? filter, CancellationToken ct)
            => throw new NotSupportedException(
                "Vector search inside a transaction is not supported. Run NearestVectors against the outer store.");

        Task<IReadOnlyList<FullTextResult<T>>> IQueryExecutor.FullTextSearchAsync<T>(
            string searchText, int maxResults, Expression<Func<T, bool>>? filter, CancellationToken ct)
            => throw new NotSupportedException(
                "Full-text search inside a transaction is not supported. Run FullTextSearch against the outer store.");

        string? GetTenantFilter() => this.options.TenantIdAccessor != null ? " AND TenantId = @tenantId" : null;

        void AppendGlobalFilters<T>(DbCommand cmd, JsonTypeInfo<T>? typeInfo) where T : class
        {
            var filters = this.options.ResolveQueryFilters(typeof(T));
            if (filters.Count == 0)
                return;

            var info = this.FindTypeInfo(typeInfo)
                ?? throw new InvalidOperationException(
                    $"Global query filters for '{typeof(T).Name}' require a JsonTypeInfo<{typeof(T).Name}>.");

            var predicates = filters.Select(f => (Expression<Func<T, bool>>)f.Predicate).ToList();
            var combined = DocumentQuery<T>.CombinePredicates(predicates);
            var (clause, parms) = JsonExpressionVisitor.Translate(combined, info, this.provider);

            var sql = cmd.CommandText.TrimEnd();
            var hasTrailingSemicolon = sql.EndsWith(';');
            if (hasTrailingSemicolon)
                sql = sql.Substring(0, sql.Length - 1).TrimEnd();
            cmd.CommandText = sql + $" AND ({clause})" + (hasTrailingSemicolon ? ";" : "");
            foreach (var kv in parms)
                AddParameter(cmd, kv.Key, kv.Value);
        }

        void AddTenantParam(DbCommand cmd)
        {
            if (this.options.TenantIdAccessor != null)
                AddParameter(cmd, "@tenantId", this.options.TenantIdAccessor());
        }

        async IAsyncEnumerable<T> ReadStreamInternalAsync<T>(
            string tableName,
            Action<DbCommand> configure,
            Func<string, T> deserialize,
            [EnumeratorCancellation] CancellationToken ct)
        {
            await this.EnsureTableAsync(tableName, ct).ConfigureAwait(false);
            await using var cmd = this.CreateCommand();
            configure(cmd);
            this.Log(cmd.CommandText);
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                yield return deserialize(reader.GetString(0));
        }

        // ── Query<T>() ─────────────────────────────────────────────────

        public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
        {
            return new DocumentQuery<T>(this, FindTypeInfo(jsonTypeInfo));
        }

        // ── CRUD ────────────────────────────────────────────────────────

        async Task InsertCoreAsync(string tableName, string id, string typeName, string json, CancellationToken ct)
        {
            await this.EnsureTableAsync(tableName, ct).ConfigureAwait(false);
            var now = DateTimeOffset.UtcNow;
            await using var cmd = this.CreateCommand();
            if (this.options.TenantIdAccessor != null)
            {
                var insertSql = this.provider.BuildInsertSql(tableName);
                cmd.CommandText = insertSql
                    .Replace("(Id, TypeName, Data, CreatedAt, UpdatedAt)", "(Id, TypeName, TenantId, Data, CreatedAt, UpdatedAt)")
                    // Match only the leading "(@id, @typeName," so the substitution survives providers
                // that wrap @data in a cast (Postgres → CAST(@data AS JSONB), DuckDB → CAST(@data AS JSON)).
                .Replace("(@id, @typeName,", "(@id, @typeName, @tenantId,");
                AddParameter(cmd, "@tenantId", this.options.TenantIdAccessor());
            }
            else
            {
                cmd.CommandText = this.provider.BuildInsertSql(tableName);
            }
            AddParameter(cmd, "@id", id);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@data", json);
            AddParameter(cmd, "@now", now);
            this.Log(cmd.CommandText);
            try
            {
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (this.provider.IsDuplicateKeyException(ex))
            {
                throw new InvalidOperationException(
                    $"A document of type '{typeName}' with Id '{id}' already exists.", ex);
            }
        }

        async Task UpdateCoreAsync(string tableName, string id, string typeName, string json, int? expectedVersion, string? versionJsonPath, Action<DbCommand>? appendFilters, CancellationToken ct)
        {
            await this.EnsureTableAsync(tableName, ct).ConfigureAwait(false);
            var now = DateTimeOffset.UtcNow;
            await using var cmd = this.CreateCommand();
            cmd.CommandText = this.provider.BuildUpdateSql(tableName);
            if (this.options.TenantIdAccessor != null)
            {
                cmd.CommandText = cmd.CommandText.Replace(
                    "WHERE Id = @id AND TypeName = @typeName",
                    "WHERE Id = @id AND TypeName = @typeName AND TenantId = @tenantId");
                AddParameter(cmd, "@tenantId", this.options.TenantIdAccessor());
            }
            if (expectedVersion != null && versionJsonPath != null)
            {
                cmd.CommandText = cmd.CommandText.TrimEnd().TrimEnd(';')
                    // Use JsonExtractTyped so providers like PostgreSQL emit an explicit cast on the
                // extracted text (Data #>> '{Version}'::BIGINT); a bare extract returns text and
                // PG rejects "text = integer" with a 42883 operator-not-exist error.
                + $" AND {this.provider.JsonExtractTyped("Data", versionJsonPath, typeof(int))} = @expectedVersion;";
                AddParameter(cmd, "@expectedVersion", expectedVersion.Value);
            }
            AddParameter(cmd, "@id", id);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@data", json);
            AddParameter(cmd, "@now", now);
            appendFilters?.Invoke(cmd);
            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (rows == 0)
            {
                if (expectedVersion != null)
                    throw new ConcurrencyException(typeName, id, expectedVersion.Value);

                throw new InvalidOperationException(
                    $"No document of type '{typeName}' with Id '{id}' was found to update.");
            }
        }

        async Task UpsertMergeCoreAsync(string tableName, string id, string typeName, string json, int? expectedVersion, string? versionJsonPath, CancellationToken ct)
        {
            await this.EnsureTableAsync(tableName, ct).ConfigureAwait(false);
            json = StripNullProperties(json);
            var now = DateTimeOffset.UtcNow;

            if (this.provider.SupportsJsonMergePatch)
            {
                await using var cmd = this.CreateCommand();
                var upsertSql = this.provider.BuildUpsertMergeSql(tableName);
                if (this.options.TenantIdAccessor != null)
                {
                    upsertSql = upsertSql
                        .Replace("(Id, TypeName, Data, CreatedAt, UpdatedAt)", "(Id, TypeName, TenantId, Data, CreatedAt, UpdatedAt)")
                        // Match only the leading "(@id, @typeName," so the substitution survives providers
                // that wrap @data in a cast (Postgres → CAST(@data AS JSONB), DuckDB → CAST(@data AS JSON)).
                .Replace("(@id, @typeName,", "(@id, @typeName, @tenantId,");
                    AddParameter(cmd, "@tenantId", this.options.TenantIdAccessor());
                }
                if (expectedVersion != null && versionJsonPath != null)
                {
                    upsertSql = upsertSql.TrimEnd().TrimEnd(';')
                        // Use JsonExtractTyped so providers like PostgreSQL emit an explicit cast on the
                // extracted text (Data #>> '{Version}'::BIGINT); a bare extract returns text and
                // PG rejects "text = integer" with a 42883 operator-not-exist error.
                + $" AND {this.provider.JsonExtractTyped("Data", versionJsonPath, typeof(int))} = @expectedVersion;";
                    AddParameter(cmd, "@expectedVersion", expectedVersion.Value);
                }
                cmd.CommandText = upsertSql;
                AddParameter(cmd, "@id", id);
                AddParameter(cmd, "@typeName", typeName);
                AddParameter(cmd, "@data", json);
                AddParameter(cmd, "@now", now);
                this.Log(cmd.CommandText);
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                return;
            }

            // Fallback for PG / SQL Server. The outer user-owned transaction already
            // provides isolation; the row lock from BuildSelectDataForUpdateSql blocks
            // concurrent writers within that transaction's scope.
            await UpsertMergeFallbackAsync(
                this.connection, this.transaction, this.provider, this.options.TenantIdAccessor,
                tableName, id, typeName, json, now, expectedVersion, versionJsonPath,
                this.Log, ct).ConfigureAwait(false);
        }

        async Task<bool> SetPropertyCoreAsync(string tableName, string id, string typeName, string jsonPath, object? value, Action<DbCommand>? appendFilters, CancellationToken ct)
        {
            await this.EnsureTableAsync(tableName, ct).ConfigureAwait(false);
            var now = DateTimeOffset.UtcNow;
            await using var cmd = this.CreateCommand();
            cmd.CommandText = this.provider.BuildSetPropertySql(tableName);
            if (this.options.TenantIdAccessor != null)
            {
                cmd.CommandText = cmd.CommandText.Replace(
                    "WHERE Id = @id AND TypeName = @typeName",
                    "WHERE Id = @id AND TypeName = @typeName AND TenantId = @tenantId");
                AddParameter(cmd, "@tenantId", this.options.TenantIdAccessor());
            }
            AddParameter(cmd, "@path", "$." + jsonPath);
            AddParameter(cmd, "@value", this.provider.FormatPropertyValue(value));
            AddParameter(cmd, "@now", now);
            AddParameter(cmd, "@id", id);
            AddParameter(cmd, "@typeName", typeName);
            appendFilters?.Invoke(cmd);
            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            return rows > 0;
        }

        async Task<bool> RemovePropertyCoreAsync(string tableName, string id, string typeName, string jsonPath, Action<DbCommand>? appendFilters, CancellationToken ct)
        {
            await this.EnsureTableAsync(tableName, ct).ConfigureAwait(false);
            var now = DateTimeOffset.UtcNow;
            await using var cmd = this.CreateCommand();
            cmd.CommandText = this.provider.BuildRemovePropertySql(tableName);
            if (this.options.TenantIdAccessor != null)
            {
                cmd.CommandText = cmd.CommandText.Replace(
                    "WHERE Id = @id AND TypeName = @typeName",
                    "WHERE Id = @id AND TypeName = @typeName AND TenantId = @tenantId");
                AddParameter(cmd, "@tenantId", this.options.TenantIdAccessor());
            }
            AddParameter(cmd, "@path", "$." + jsonPath);
            AddParameter(cmd, "@now", now);
            AddParameter(cmd, "@id", id);
            AddParameter(cmd, "@typeName", typeName);
            appendFilters?.Invoke(cmd);
            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            return rows > 0;
        }

        async Task<string> GenerateIdAsync(IdKind kind, string tableName, string typeName, CancellationToken ct)
        {
            switch (kind)
            {
                case IdKind.Guid:
                    return Guid.NewGuid().ToString("N");

                case IdKind.String:
                    return Guid.NewGuid().ToString();

                case IdKind.Int:
                case IdKind.Long:
                    await using (var cmd = this.CreateCommand())
                    {
                        cmd.CommandText = this.provider.BuildMaxIdSql(tableName);
                        AddParameter(cmd, "@typeName", typeName);
                        this.Log(cmd.CommandText);
                        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
                        var max = result is DBNull || result is null ? 0L : Convert.ToInt64(result);
                        return (max + 1).ToString();
                    }

                default:
                    throw new InvalidOperationException($"Unsupported Id kind: {kind}");
            }
        }

        Internal.VersionMapping? ResolveVersionMapping<T>() => this.options.ResolveVersionMapping(typeof(T));

        public async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);
            var tableName = this.ResolveTableName<T>();
            var versionMapping = this.ResolveVersionMapping<T>();

            var insertTypeName = this.ResolveTypeName<T>();
            // The BeforeWrite interceptor runs here (same order as the non-tx Insert), so a unit-of-work /
            // batch-fallback insert of a vector type still generates its auto-embed embedding.
            var ctx = this.NewWriteContext(DocumentOperation.Insert, insertTypeName, null, document, jsonTypeInfo);
            if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
                return;
            if (ctx?.Document is T mutated)
                document = mutated;

            string id;
            if (accessor.IsDefaultId(document))
            {
                if (accessor.Kind == IdKind.Custom)
                {
                    id = accessor.GenerateOrThrow();
                }
                else if (accessor.Kind == IdKind.String)
                {
                    throw new InvalidOperationException(
                        $"Insert requires a non-empty string Id on '{typeof(T).Name}'. " +
                        "String Id properties are not auto-generated during Insert.");
                }
                else
                {
                    var typeName = this.ResolveTypeName<T>();
                    id = await this.GenerateIdAsync(accessor.Kind, tableName, typeName, cancellationToken).ConfigureAwait(false);
                }
                accessor.SetId(document, id);
            }
            else
            {
                id = accessor.GetIdAsString(document);
            }
            versionMapping?.SetVersion(document, 1);
            var preparedBlobs = this.PrepareBlobs(document);
            var json = SerializeDocument(document, typeInfo, this.jsonOptions);
            await this.InsertCoreAsync(tableName, id, insertTypeName, json, cancellationToken).ConfigureAwait(false);
            await this.BlobSync<T>(tableName, id, insertTypeName, preparedBlobs, prune: true, cancellationToken).ConfigureAwait(false);
            await this.SpatialSync(tableName, id, insertTypeName, document, cancellationToken).ConfigureAwait(false);
            await this.VectorSync(tableName, insertTypeName, id, document, cancellationToken).ConfigureAwait(false);
            await this.AppendHistory(typeof(T), tableName, id, insertTypeName, TemporalOperation.Inserted, json, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document) ?? 1, cancellationToken).ConfigureAwait(false);
            this.QueueChange(DocumentChangeType.Inserted, id, document);
        }

        public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);
            var tableName = this.ResolveTableName<T>();
            var typeName = this.ResolveTypeName<T>();
            var versionMapping = this.ResolveVersionMapping<T>();
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);

            var docList = documents as IReadOnlyList<T> ?? documents.ToList();

            // Per-doc BeforeWrite before serialization in the core.
            DocumentWriteContext[]? ctxs = null;
            if (this.options.Interceptors.HasPerDoc)
            {
                var mutable = docList.ToList();
                ctxs = await this.options.Interceptors.BeforeWriteBatch(mutable, typeName, this, DocumentOperationScope.CurrentServices, cancellationToken, doc => SerializeDocument((T)doc, typeInfo, this.jsonOptions)).ConfigureAwait(false);
                docList = mutable;
            }

            var count = await BatchInsertCoreAsync(
                tableName, typeName, docList, accessor, typeInfo,
                this.jsonOptions, this.logging, this.provider,
                this.CreateCommand,
                this.GenerateIdAsync,
                versionMapping,
                cancellationToken
            ).ConfigureAwait(false);

            if (count > 0)
            {
                var temporalMapping = this.options.ResolveTemporalMapping(typeof(T));
                for (var i = 0; i < docList.Count; i++)
                {
                    var document = docList[i];
                    var docId = accessor.GetIdAsString(document);
                    if (temporalMapping != null)
                        await AppendHistoryCoreAsync(this.CreateCommand, this.provider, temporalMapping, tableName, docId, typeName, TemporalOperation.Inserted, null, this.logging, cancellationToken).ConfigureAwait(false);
                    if (ctxs != null)
                        await this.RunAfterWriteAsync(ctxs[i], docId, versionMapping?.GetVersion(document) ?? 1, cancellationToken).ConfigureAwait(false);
                    this.QueueChange(DocumentChangeType.Inserted, docId, document);
                }
            }
            return count;
        }

        public async Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);
            var versionMapping = this.ResolveVersionMapping<T>();
            var typeName = this.ResolveTypeName<T>();

            var ctx = this.NewWriteContext(DocumentOperation.Update, typeName, null, document, jsonTypeInfo);
            if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
                return;
            if (ctx?.Document is T mutated)
                document = mutated;

            if (accessor.IsDefaultId(document))
                throw new InvalidOperationException(
                    $"Update requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Update.");

            var id = accessor.GetIdAsString(document);

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                expectedVersion = versionMapping.GetVersion(document);
                versionMapping.SetVersion(document, expectedVersion.Value + 1);
            }

            var preparedBlobs = this.PrepareBlobs(document);
            var json = SerializeDocument(document, typeInfo, this.jsonOptions);
            var updateTableName = this.ResolveTableName<T>();
            await this.UpdateCoreAsync(updateTableName, id, typeName, json, expectedVersion, versionMapping?.JsonPath, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken).ConfigureAwait(false);
            await this.BlobSync<T>(updateTableName, id, typeName, preparedBlobs, prune: true, cancellationToken).ConfigureAwait(false);
            await this.SpatialSync(updateTableName, id, typeName, document, cancellationToken).ConfigureAwait(false);
            await this.VectorSync(updateTableName, typeName, id, document, cancellationToken).ConfigureAwait(false);
            await this.AppendHistory(typeof(T), updateTableName, id, typeName, TemporalOperation.Updated, json, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document), cancellationToken).ConfigureAwait(false);
            this.QueueChange(DocumentChangeType.Updated, id, document);
        }

        public async Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);
            var versionMapping = this.ResolveVersionMapping<T>();
            var typeName = this.ResolveTypeName<T>();

            // The BeforeWrite interceptor runs here (same order as the non-tx Upsert), so an upsert of a vector
            // type inside a unit (or via the implicit one-op unit) still auto-embeds.
            var ctx = this.NewWriteContext(DocumentOperation.Upsert, typeName, null, patch, jsonTypeInfo);
            if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
                return;
            if (ctx?.Document is T mutated)
                patch = mutated;

            if (accessor.IsDefaultId(patch))
                throw new InvalidOperationException(
                    $"Upsert requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

            var id = accessor.GetIdAsString(patch);

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                expectedVersion = versionMapping.GetVersion(patch);
                if (expectedVersion > 0)
                    versionMapping.SetVersion(patch, expectedVersion.Value + 1);
                else
                    versionMapping.SetVersion(patch, 1);
            }

            var preparedBlobs = this.PrepareBlobs(patch);
            var json = SerializeDocument(patch, typeInfo, this.jsonOptions);
            var upsertTableName = this.ResolveTableName<T>();
            await this.UpsertMergeCoreAsync(upsertTableName, id, typeName, json, expectedVersion > 0 ? expectedVersion : null, versionMapping?.JsonPath, cancellationToken).ConfigureAwait(false);
            await this.BlobSync<T>(upsertTableName, id, typeName, preparedBlobs, prune: false, cancellationToken).ConfigureAwait(false);
            await this.SpatialSync(upsertTableName, id, typeName, patch, cancellationToken).ConfigureAwait(false);
            await this.VectorSync(upsertTableName, typeName, id, patch, cancellationToken).ConfigureAwait(false);
            await this.AppendHistory(typeof(T), upsertTableName, id, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
            this.QueueChange(DocumentChangeType.Updated, id, patch);
        }

        // RFC 7396 merge-update (Update(patch: true)) on the unit's pinned connection — the transaction-bound
        // twin of DocumentStore.Update(patch: true). Lets a single merge-update with per-doc interceptors run as
        // an implicit one-op unit so ctx.Store is transaction-visible. Reuses the parent's merge/replace core.
        internal async Task UpdateMerge<T>(T document, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);
            var versionMapping = this.ResolveVersionMapping<T>();
            var typeName = this.ResolveTypeName<T>();

            var ctx = this.NewWriteContext(DocumentOperation.Update, typeName, null, document, jsonTypeInfo);
            if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
                return;
            if (ctx?.Document is T mutated)
                document = mutated;
            this.parent.ValidateVectorDimensions(document);

            if (accessor.IsDefaultId(document))
                throw new InvalidOperationException(
                    $"Update requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Update.");
            var id = accessor.GetIdAsString(document);
            var tableName = this.ResolveTableName<T>();

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                expectedVersion = versionMapping.GetVersion(document);
                if (expectedVersion > 0)
                    versionMapping.SetVersion(document, expectedVersion.Value + 1);
            }
            var preparedBlobs = this.PrepareBlobs(document);
            var json = SerializeDocument(document, typeInfo, this.jsonOptions);
            if (versionMapping != null && !(expectedVersion > 0))
                json = RemoveJsonProperty(json, versionMapping.JsonPath);
            await this.parent.MergeOrReplaceCoreAsync(this.SidecarSession(), tableName, id, typeName, json, expectedVersion > 0 ? expectedVersion : null, versionMapping?.JsonPath, merge: true, insertIfMissing: false, cancellationToken).ConfigureAwait(false);
            await this.BlobSync<T>(tableName, id, typeName, preparedBlobs, prune: false, cancellationToken).ConfigureAwait(false);
            await this.SpatialSync(tableName, id, typeName, document, cancellationToken).ConfigureAwait(false);
            await this.VectorSync(tableName, typeName, id, document, cancellationToken).ConfigureAwait(false);
            await this.AppendHistory(typeof(T), tableName, id, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(document), cancellationToken).ConfigureAwait(false);
            this.QueueChange(DocumentChangeType.Updated, id, document);
        }

        // Wholesale replace-on-upsert (Upsert(patchIfUpdate: false)) on the unit's pinned connection — the
        // transaction-bound twin of DocumentStore.Upsert(patchIfUpdate: false).
        internal async Task UpsertReplace<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo, CancellationToken cancellationToken) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);
            var versionMapping = this.ResolveVersionMapping<T>();
            var typeName = this.ResolveTypeName<T>();

            var ctx = this.NewWriteContext(DocumentOperation.Upsert, typeName, null, patch, jsonTypeInfo);
            if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
                return;
            if (ctx?.Document is T mutated)
                patch = mutated;
            this.parent.ValidateVectorDimensions(patch);

            if (accessor.IsDefaultId(patch))
                throw new InvalidOperationException(
                    $"Upsert requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");
            var id = accessor.GetIdAsString(patch);
            var tableName = this.ResolveTableName<T>();

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                expectedVersion = versionMapping.GetVersion(patch);
                if (expectedVersion > 0)
                    versionMapping.SetVersion(patch, expectedVersion.Value + 1);
                else
                    versionMapping.SetVersion(patch, 1);
            }
            var preparedBlobs = this.PrepareBlobs(patch);
            var json = SerializeDocument(patch, typeInfo, this.jsonOptions);
            await this.parent.MergeOrReplaceCoreAsync(this.SidecarSession(), tableName, id, typeName, json, expectedVersion > 0 ? expectedVersion : null, versionMapping?.JsonPath, merge: false, insertIfMissing: true, cancellationToken).ConfigureAwait(false);
            await this.BlobSync<T>(tableName, id, typeName, preparedBlobs, prune: true, cancellationToken).ConfigureAwait(false);
            await this.SpatialSync(tableName, id, typeName, patch, cancellationToken).ConfigureAwait(false);
            await this.VectorSync(tableName, typeName, id, patch, cancellationToken).ConfigureAwait(false);
            await this.AppendHistory(typeof(T), tableName, id, typeName, TemporalOperation.Updated, json, cancellationToken).ConfigureAwait(false);
            await this.RunAfterWriteAsync(ctx, id, versionMapping?.GetVersion(patch), cancellationToken).ConfigureAwait(false);
            this.QueueChange(DocumentChangeType.Updated, id, patch);
        }

        public async Task<int> BatchUpsert<T>(IEnumerable<T> patches, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);
            var tableName = this.ResolveTableName<T>();
            var typeName = this.ResolveTypeName<T>();
            var list = patches as IReadOnlyList<T> ?? patches.ToList();
            if (list.Count == 0)
                return 0;

            if (this.provider.SupportsBatchUpsert && IsBatchFastEligible(this.options, typeof(T)))
            {
                await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
                foreach (var patch in list)
                    if (accessor.IsDefaultId(patch))
                        throw new InvalidOperationException(
                            $"Upsert requires a non-default Id on the document. " +
                            $"Set the Id property on '{typeof(T).Name}' before calling BatchUpsert.");
                await BatchUpsertFastCoreAsync(tableName, typeName, list, accessor, typeInfo, this.jsonOptions, this.logging, this.provider, this.CreateCommand, cancellationToken).ConfigureAwait(false);
                foreach (var patch in list)
                    this.QueueChange(DocumentChangeType.Updated, accessor.GetIdAsString(patch), patch);
                return list.Count;
            }

            foreach (var patch in list)
                await this.Upsert(patch, typeInfo, cancellationToken).ConfigureAwait(false);
            return list.Count;
        }

        public async Task<int> BatchUpdate<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var list = documents as IReadOnlyList<T> ?? documents.ToList();
            if (list.Count == 0)
                return 0;
            foreach (var document in list)
                await this.Update(document, typeInfo, cancellationToken).ConfigureAwait(false);
            return list.Count;
        }

        public async Task<int> BatchRemove<T>(IEnumerable<object> ids, CancellationToken cancellationToken = default) where T : class
        {
            var accessor = this.idCache.GetOrCreate<T>(null);
            var tableName = this.ResolveTableName<T>();
            var typeName = this.ResolveTypeName<T>();
            var idList = ids as IReadOnlyList<object> ?? ids.ToList();
            if (idList.Count == 0)
                return 0;

            if (IsBatchFastEligible(this.options, typeof(T)))
            {
                await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
                var resolved = new List<string>(idList.Count);
                foreach (var id in idList)
                    resolved.Add(accessor.ResolveId(id));
                var n = await BatchDeleteByIdsCoreAsync(tableName, typeName, resolved, this.CreateCommand, this.provider, this.logging, cancellationToken).ConfigureAwait(false);
                if (n > 0)
                    foreach (var id in resolved)
                        this.QueueChange<T>(DocumentChangeType.Removed, id, null);
                return n;
            }

            var removed = 0;
            foreach (var id in idList)
                if (await this.Remove<T>(id, cancellationToken).ConfigureAwait(false))
                    removed++;
            return removed;
        }

        public async Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
            var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
            var tableName = this.ResolveTableName<T>();
            var typeName = this.ResolveTypeName<T>();
            var updated = await this.SetPropertyCoreAsync(tableName, resolvedId, typeName, jsonPath, value, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken).ConfigureAwait(false);
            if (updated)
            {
                await this.AppendHistory(typeof(T), tableName, resolvedId, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
                this.QueueChange<T>(DocumentChangeType.Updated, resolvedId, null);
            }
            return updated;
        }

        public async Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
            var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
            var tableName = this.ResolveTableName<T>();
            var typeName = this.ResolveTypeName<T>();
            var updated = await this.RemovePropertyCoreAsync(tableName, resolvedId, typeName, jsonPath, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken).ConfigureAwait(false);
            if (updated)
            {
                await this.AppendHistory(typeof(T), tableName, resolvedId, typeName, TemporalOperation.Updated, null, cancellationToken).ConfigureAwait(false);
                this.QueueChange<T>(DocumentChangeType.Updated, resolvedId, null);
            }
            return updated;
        }

        public async Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
            var tableName = this.ResolveTableName<T>();
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            await using var cmd = this.CreateCommand();
            var sql = $"SELECT Data FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.AddTenantParam(cmd);
            this.AppendGlobalFilters(cmd, typeInfo);

            this.Log(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return result is string json
                ? DeserializeDocument(json, typeInfo, this.jsonOptions)
                : null;
        }

        public async Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
            var tableName = this.ResolveTableName<T>();
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            await using var cmd = this.CreateCommand();
            var sql = $"SELECT Data FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.AddTenantParam(cmd);
            this.AppendGlobalFilters(cmd, typeInfo);

            this.Log(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (result is not string originalJson)
                return null;

            var modifiedJson = SerializeDocument(modified, typeInfo, this.jsonOptions);
            return JsonDiff.CreatePatch<T>(originalJson, modifiedJson, this.jsonOptions);
        }

        // ── String-based query ──────────────────────────────────────────

        public async Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var tableName = this.ResolveTableName<T>();
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            await using var cmd = this.CreateCommand();
            var sql = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName{GetTenantFilter() ?? ""} AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.AddTenantParam(cmd);
            BindParameters(cmd, parameters);
            this.Log(cmd.CommandText);
            return await ReadListAsync<T>(cmd, json => DeserializeDocument(json, typeInfo, this.jsonOptions)!, cancellationToken).ConfigureAwait(false);
        }

        // ── String-based streaming ──────────────────────────────────────

        public async IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var tableName = this.ResolveTableName<T>();
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            await using var cmd = this.CreateCommand();
            var sql = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName{GetTenantFilter() ?? ""} AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.AddTenantParam(cmd);
            BindParameters(cmd, parameters);

            this.Log(cmd.CommandText);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                yield return DeserializeDocument(reader.GetString(0), typeInfo, this.jsonOptions)!;
        }

        // ── Count / Remove / Clear ──────────────────────────────────────

        public async Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        {
            var tableName = this.ResolveTableName<T>();
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            await using var cmd = this.CreateCommand();
            var sql = $"SELECT COUNT(*) FROM {Qt(tableName)} WHERE TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            if (!string.IsNullOrWhiteSpace(whereClause))
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.AddTenantParam(cmd);
            BindParameters(cmd, parameters);
            this.AppendGlobalFilters<T>(cmd, null);

            this.Log(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return Convert.ToInt32(result);
        }

        public async Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
        {
            var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
            var tableName = this.ResolveTableName<T>();
            var typeName = this.ResolveTypeName<T>();
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            var ctx = this.NewWriteContext<T>(DocumentOperation.Delete, typeName, id, null);
            if (!await this.RunBeforeWriteAsync(ctx, cancellationToken).ConfigureAwait(false))
                return ctx!.CancelResult;
            int rows;
            await using (var cmd = this.CreateCommand())
            {
                var sql = $"DELETE FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName";
                sql += GetTenantFilter() ?? "";
                cmd.CommandText = sql + ";";
                AddParameter(cmd, "@id", resolvedId);
                AddParameter(cmd, "@typeName", typeName);
                this.AddTenantParam(cmd);
                this.AppendGlobalFilters<T>(cmd, null);
                this.Log(cmd.CommandText);
                rows = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            if (rows > 0)
            {
                await this.BlobDelete(typeof(T), tableName, resolvedId, typeName, cancellationToken).ConfigureAwait(false);
                await this.AppendHistory(typeof(T), tableName, resolvedId, typeName, TemporalOperation.Removed, null, cancellationToken).ConfigureAwait(false);
                await this.RunAfterWriteAsync(ctx, id, null, cancellationToken).ConfigureAwait(false);
                this.QueueChange<T>(DocumentChangeType.Removed, resolvedId, null);
            }
            return rows > 0;
        }

        public async Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
        {
            var tableName = this.ResolveTableName<T>();
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            await using var cmd = this.CreateCommand();
            var sql = $"DELETE FROM {Qt(tableName)} WHERE TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.AppendGlobalFilters<T>(cmd, null);
            this.AddTenantParam(cmd);
            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (rows > 0)
                this.QueueChange<T>(DocumentChangeType.Cleared, "", null);
            return rows;
        }

        public IDocumentSession OpenSession()
            => throw new InvalidOperationException("Nested sessions / units of work are not supported inside a transaction.");
        public IDocumentSession OpenSession(IServiceProvider scope)
            => throw new InvalidOperationException("Nested sessions / units of work are not supported inside a transaction.");

        // ── JSON collections — not supported inside a unit of work ──────
        // These are a store-level feature; UnitOfWork buffers typed operations only.

        const string JsonLaneInUnitError =
            "JSON collections (store.Collection) are not available inside a UnitOfWork. Use session.Store.Collection(...).";

        public IJsonDocumentCollection Collection(string name, string idProperty = "id")
            => throw new NotSupportedException(JsonLaneInUnitError);

        public IJsonDocumentCollection Collection(Type type)
            => throw new NotSupportedException(JsonLaneInUnitError);
    }
}
