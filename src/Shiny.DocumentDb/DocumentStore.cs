using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

public class DocumentStore : IDocumentStore, IObservableDocumentStore, IChangeFeedDocumentStore, IQueryExecutor, IDisposable
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
    readonly Action<string>? logging;
    readonly Func<string>? tenantIdAccessor;
    readonly IdAccessorCache idCache;
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
        this.jsonOptions = options.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        this.logging = options.Logging;
        this.tenantIdAccessor = options.TenantIdAccessor;
        this.sharedMode = this.provider.RequiresSingleConnection;
        if (this.sharedMode)
        {
            this.sharedSemaphore = new SemaphoreSlim(1, 1);
            this.sharedConnection = this.provider.CreateConnection();
        }
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName);
        options.ResolveVersionJsonPaths(this.jsonOptions);
        options.ResolveSpatialJsonPaths(this.jsonOptions);
    }

    public bool SupportsSpatial => this.provider.SupportsSpatial;

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
        this.sharedConnectionInitialized = true;
    }

    async Task EnsureTableInitializedAsync(DocumentStoreSession session, string tableName, CancellationToken ct)
    {
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
            if (spatialSql != null && this.options.spatialMappings.Count > 0)
            {
                await using var spatialCmd = session.CreateCommand();
                spatialCmd.CommandText = spatialSql;
                this.Log(spatialCmd.CommandText);
                await spatialCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }
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
                if (accessor.Kind == IdKind.String)
                    throw new InvalidOperationException(
                        $"Insert requires a non-empty string Id on '{typeof(T).Name}'. " +
                        "String Id properties are not auto-generated during Insert.");

                if (accessor.Kind is IdKind.Int or IdKind.Long)
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
        // deep-merge function. Read-merge-write inside an owned transaction so the row lock
        // taken by BuildSelectDataForUpdateSql blocks concurrent writers until UPDATE/INSERT
        // commits.
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
        CancellationToken ct)
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

        var merged = Internal.JsonMergePatch.Merge(existingJson, json);

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

    ChangeBroadcaster? IQueryExecutor.Broadcaster => this.broadcaster;

    DocumentStoreOptions IQueryExecutor.Options => this.options;

    // ── Spatial sync helpers ──────────────────────────────────────────────

    async Task SpatialUpsertAsync<T>(DocumentStoreSession session, string tableName, string id, string typeName, T document, CancellationToken ct)
    {
        var mapping = this.options.ResolveSpatialMapping(typeof(T));
        var sql = mapping != null ? this.provider.BuildSpatialUpsertSql(tableName) : null;
        if (sql == null || mapping == null)
            return;

        var point = mapping.GetGeoPoint(document!);
        await using var cmd = session.CreateCommand();
        cmd.CommandText = sql;
        AddParameter(cmd, "@spatialDocId", id);
        AddParameter(cmd, "@spatialTypeName", typeName);
        AddParameter(cmd, "@spatialLat", point.Latitude);
        AddParameter(cmd, "@spatialLng", point.Longitude);
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

    // ── Query<T>() entry point ──────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        return new DocumentQuery<T>(this, FindTypeInfo(jsonTypeInfo));
    }

    // ── CRUD ────────────────────────────────────────────────────────────

    public async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var versionMapping = this.ResolveVersionMapping<T>();
        var insertedId = "";
        await this.ExecuteAsync(tableName, async session =>
        {
            string id;
            if (accessor.IsDefaultId(document))
            {
                if (accessor.Kind == IdKind.String)
                    throw new InvalidOperationException(
                        $"Insert requires a non-empty string Id on '{typeof(T).Name}'. " +
                        "String Id properties are not auto-generated during Insert.");

                var typeName = this.ResolveTypeName<T>();
                id = await this.GenerateIdAsync(session, accessor.Kind, tableName, typeName, cancellationToken).ConfigureAwait(false);
                accessor.SetId(document, id);
            }
            else
            {
                id = accessor.GetIdAsString(document);
            }
            versionMapping?.SetVersion(document, 1);
            var typeName2 = this.ResolveTypeName<T>();
            var json = SerializeDocument(document, typeInfo, this.jsonOptions);
            await this.InsertCoreAsync(session, tableName, id, typeName2, json, cancellationToken).ConfigureAwait(false);
            await this.SpatialUpsertAsync(session, tableName, id, typeName2, document, cancellationToken).ConfigureAwait(false);
            insertedId = id;
        }, cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Inserted, insertedId, document);
    }

    public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();
        var versionMapping = this.ResolveVersionMapping<T>();
        // Materialize so we can replay the inserted documents to observers after commit.
        var docList = documents as IReadOnlyList<T> ?? documents.ToList();

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

    public async Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var versionMapping = this.ResolveVersionMapping<T>();
        var updatedId = "";
        await this.ExecuteAsync(tableName, async session =>
        {
            if (accessor.IsDefaultId(document))
                throw new InvalidOperationException(
                    $"Update requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Update.");

            var id = accessor.GetIdAsString(document);
            var typeName = this.ResolveTypeName<T>();

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                expectedVersion = versionMapping.GetVersion(document);
                versionMapping.SetVersion(document, expectedVersion.Value + 1);
            }

            var json = SerializeDocument(document, typeInfo, this.jsonOptions);
            await this.UpdateCoreAsync(session, tableName, id, typeName, json, expectedVersion, versionMapping?.JsonPath, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken).ConfigureAwait(false);
            await this.SpatialUpsertAsync(session, tableName, id, typeName, document, cancellationToken).ConfigureAwait(false);
            updatedId = id;
        }, cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Updated, updatedId, document);
    }

    public async Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var versionMapping = this.ResolveVersionMapping<T>();
        var upsertedId = "";
        await this.ExecuteAsync(tableName, async session =>
        {
            if (accessor.IsDefaultId(patch))
                throw new InvalidOperationException(
                    $"Upsert requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

            var id = accessor.GetIdAsString(patch);
            var typeName = this.ResolveTypeName<T>();

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                expectedVersion = versionMapping.GetVersion(patch);
                if (expectedVersion > 0)
                    versionMapping.SetVersion(patch, expectedVersion.Value + 1);
                else
                    versionMapping.SetVersion(patch, 1);
            }

            var json = SerializeDocument(patch, typeInfo, this.jsonOptions);
            await this.UpsertMergeCoreAsync(session, tableName, id, typeName, json, expectedVersion > 0 ? expectedVersion : null, versionMapping?.JsonPath, cancellationToken).ConfigureAwait(false);
            await this.SpatialUpsertAsync(session, tableName, id, typeName, patch, cancellationToken).ConfigureAwait(false);
            upsertedId = id;
        }, cancellationToken).ConfigureAwait(false);
        this.PublishChange(DocumentChangeType.Updated, upsertedId, patch);
    }

    public async Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var tableName = this.ResolveTableName<T>();
        var updated = await this.ExecuteAsync(tableName,
            session => this.SetPropertyCoreAsync(session, tableName, resolvedId, this.ResolveTypeName<T>(), jsonPath, value, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken),
            cancellationToken).ConfigureAwait(false);
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
        var updated = await this.ExecuteAsync(tableName,
            session => this.RemovePropertyCoreAsync(session, tableName, resolvedId, this.ResolveTypeName<T>(), jsonPath, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken),
            cancellationToken).ConfigureAwait(false);
        if (updated)
            this.PublishChange<T>(DocumentChangeType.Updated, resolvedId, null);
        return updated;
    }

    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
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
                ? DeserializeDocument(json, typeInfo, this.jsonOptions)
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
            return await ReadListAsync<T>(cmd, json => DeserializeDocument(json, typeInfo, this.jsonOptions)!, cancellationToken).ConfigureAwait(false);
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
            json => DeserializeDocument(json, typeInfo, this.jsonOptions)!,
            cancellationToken);
    }

    // ── Count / Remove / Clear ──────────────────────────────────────────

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
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

    public async Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var tableName = this.ResolveTableName<T>();
        var removed = await this.ExecuteAsync(tableName, async session =>
        {
            var typeName = this.ResolveTypeName<T>();

            await using var cmd = session.CreateCommand();
            var sql = $"DELETE FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", typeName);
            this.AddTenantParam(cmd);
            this.AppendGlobalFilters<T>(cmd, null);

            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (rows > 0)
                await this.SpatialDeleteAsync(session, typeof(T), tableName, resolvedId, typeName, cancellationToken).ConfigureAwait(false);
            return rows > 0;
        }, cancellationToken).ConfigureAwait(false);
        if (removed)
            this.PublishChange<T>(DocumentChangeType.Removed, resolvedId, null);
        return removed;
    }

    public async Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
    {
        var tableName = this.ResolveTableName<T>();
        var hasFilters = this.options.ResolveQueryFilters(typeof(T)).Count > 0;
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
                await this.SpatialClearAsync(session, typeof(T), tableName, typeName, cancellationToken).ConfigureAwait(false);
            return rows;
        }, cancellationToken).ConfigureAwait(false);
        if (deleted > 0)
            this.PublishChange<T>(DocumentChangeType.Cleared, "", null);
        return deleted;
    }

    // ── Transaction ─────────────────────────────────────────────────────

    public async Task RunInTransaction(Func<IDocumentStore, Task> operation, CancellationToken cancellationToken = default)
    {
        // Buffer change notifications and only emit them once the transaction commits.
        var pendingChanges = new List<Action>();
        await this.ExecuteAsync(this.options.TableName, async session =>
        {
            // Pin the session's connection/transaction for the duration of the user callback so
            // every nested op runs on the same physical connection.
            await using var transaction = await session.Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var txStore = new TransactionalDocumentStore(session.Connection, transaction, this.options, this.provider, this.jsonOptions, this.logging, this.idCache, this.tableInitTasks, this.broadcaster, pendingChanges);
                await operation(txStore).ConfigureAwait(false);
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

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    static string ResolvePropertyPath<T>(Expression<Func<T, object>> property, JsonSerializerOptions options, JsonTypeInfo<T>? typeInfo)
        => typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, options, typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, options);

    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Parameter binding via reflection is intentional; dictionary overload available for AOT.")]
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
            var candidates = await ReadListAsync(cmd, json => DeserializeDocument(json, typeInfo, this.jsonOptions)!, cancellationToken).ConfigureAwait(false);

            var mapping = this.options.ResolveSpatialMapping(typeof(T))!;
            var results = new List<SpatialResult<T>>();
            foreach (var doc in candidates)
            {
                var point = mapping.GetGeoPoint(doc);
                var distance = GeoMath.HaversineDistance(center, point);
                if (distance <= radiusMeters)
                    results.Add(new SpatialResult<T> { Document = doc, DistanceMeters = distance });
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
            return await ReadListAsync(cmd, json => DeserializeDocument(json, typeInfo, this.jsonOptions)!, cancellationToken).ConfigureAwait(false);
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
                var candidates = await ReadListAsync(cmd, json => DeserializeDocument(json, typeInfo, this.jsonOptions)!, cancellationToken).ConfigureAwait(false);

                results = new List<SpatialResult<T>>();
                foreach (var doc in candidates)
                {
                    var point = mapping.GetGeoPoint(doc);
                    var distance = GeoMath.HaversineDistance(center, point);
                    results.Add(new SpatialResult<T> { Document = doc, DistanceMeters = distance });
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
            List<Action> pendingChanges)
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
        }

        void QueueChange<T>(DocumentChangeType changeType, string id, T? document) where T : class
            => this.pendingChanges.Add(() => this.broadcaster.Publish(new DocumentChange<T> { ChangeType = changeType, Id = id, Document = document }));

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

        JsonSerializerOptions IQueryExecutor.JsonOptions => this.jsonOptions;

        Action<string>? IQueryExecutor.Logging => this.logging;

        IDatabaseProvider IQueryExecutor.Provider => this.provider;

        string? IQueryExecutor.TenantFilter => this.options.TenantIdAccessor != null ? " AND TenantId = @tenantId" : null;

        void IQueryExecutor.AddTenantParameter(DbCommand cmd)
        {
            if (this.options.TenantIdAccessor != null)
                AddParameter(cmd, "@tenantId", this.options.TenantIdAccessor());
        }

        ChangeBroadcaster? IQueryExecutor.Broadcaster => this.broadcaster;

        DocumentStoreOptions IQueryExecutor.Options => this.options;

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

            string id;
            if (accessor.IsDefaultId(document))
            {
                if (accessor.Kind == IdKind.String)
                    throw new InvalidOperationException(
                        $"Insert requires a non-empty string Id on '{typeof(T).Name}'. " +
                        "String Id properties are not auto-generated during Insert.");

                var typeName = this.ResolveTypeName<T>();
                id = await this.GenerateIdAsync(accessor.Kind, tableName, typeName, cancellationToken).ConfigureAwait(false);
                accessor.SetId(document, id);
            }
            else
            {
                id = accessor.GetIdAsString(document);
            }
            versionMapping?.SetVersion(document, 1);
            var json = SerializeDocument(document, typeInfo, this.jsonOptions);
            await this.InsertCoreAsync(tableName, id, this.ResolveTypeName<T>(), json, cancellationToken).ConfigureAwait(false);
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
                foreach (var document in docList)
                    this.QueueChange(DocumentChangeType.Inserted, accessor.GetIdAsString(document), document);
            }
            return count;
        }

        public async Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);
            var versionMapping = this.ResolveVersionMapping<T>();

            if (accessor.IsDefaultId(document))
                throw new InvalidOperationException(
                    $"Update requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Update.");

            var id = accessor.GetIdAsString(document);
            var typeName = this.ResolveTypeName<T>();

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                expectedVersion = versionMapping.GetVersion(document);
                versionMapping.SetVersion(document, expectedVersion.Value + 1);
            }

            var json = SerializeDocument(document, typeInfo, this.jsonOptions);
            await this.UpdateCoreAsync(this.ResolveTableName<T>(), id, typeName, json, expectedVersion, versionMapping?.JsonPath, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken).ConfigureAwait(false);
            this.QueueChange(DocumentChangeType.Updated, id, document);
        }

        public async Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);
            var versionMapping = this.ResolveVersionMapping<T>();

            if (accessor.IsDefaultId(patch))
                throw new InvalidOperationException(
                    $"Upsert requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

            var id = accessor.GetIdAsString(patch);
            var typeName = this.ResolveTypeName<T>();

            int? expectedVersion = null;
            if (versionMapping != null)
            {
                expectedVersion = versionMapping.GetVersion(patch);
                if (expectedVersion > 0)
                    versionMapping.SetVersion(patch, expectedVersion.Value + 1);
                else
                    versionMapping.SetVersion(patch, 1);
            }

            var json = SerializeDocument(patch, typeInfo, this.jsonOptions);
            await this.UpsertMergeCoreAsync(this.ResolveTableName<T>(), id, typeName, json, expectedVersion > 0 ? expectedVersion : null, versionMapping?.JsonPath, cancellationToken).ConfigureAwait(false);
            this.QueueChange(DocumentChangeType.Updated, id, patch);
        }

        public async Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
            var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
            var tableName = this.ResolveTableName<T>();
            var updated = await this.SetPropertyCoreAsync(tableName, resolvedId, this.ResolveTypeName<T>(), jsonPath, value, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken).ConfigureAwait(false);
            if (updated)
                this.QueueChange<T>(DocumentChangeType.Updated, resolvedId, null);
            return updated;
        }

        public async Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
            var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
            var tableName = this.ResolveTableName<T>();
            var updated = await this.RemovePropertyCoreAsync(tableName, resolvedId, this.ResolveTypeName<T>(), jsonPath, cmd => this.AppendGlobalFilters(cmd, typeInfo), cancellationToken).ConfigureAwait(false);
            if (updated)
                this.QueueChange<T>(DocumentChangeType.Updated, resolvedId, null);
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
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            await using var cmd = this.CreateCommand();
            var sql = $"DELETE FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName";
            sql += GetTenantFilter() ?? "";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.AddTenantParam(cmd);
            this.AppendGlobalFilters<T>(cmd, null);
            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (rows > 0)
                this.QueueChange<T>(DocumentChangeType.Removed, resolvedId, null);
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

        public Task RunInTransaction(Func<IDocumentStore, Task> operation, CancellationToken cancellationToken = default)
        {
            throw new InvalidOperationException("Nested transactions are not supported.");
        }

    }
}
