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

public class DocumentStore : IDocumentStore, IQueryExecutor, IDisposable
{
    readonly SemaphoreSlim semaphore = new(1, 1);
    readonly DbConnection connection;
    readonly DocumentStoreOptions options;
    readonly IDatabaseProvider provider;
    readonly JsonSerializerOptions jsonOptions;
    readonly Action<string>? logging;
    readonly IdAccessorCache idCache;
    readonly HashSet<string> initializedTables = new(StringComparer.OrdinalIgnoreCase);
    bool connectionInitialized;

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
        this.connection = this.provider.CreateConnection();
        this.idCache = new IdAccessorCache(options.ResolveIdPropertyName);
    }

    void Log(string sql) => this.logging?.Invoke(sql);

    string ResolveTypeName<T>() => TypeNameResolver.Resolve(typeof(T), this.options.TypeNameResolution);

    string ResolveTableName<T>() => this.options.ResolveTableName(this.ResolveTypeName<T>());

    string Qt(string tableName) => this.provider.QuoteTable(tableName);

    JsonTypeInfo<T>? FindTypeInfo<T>(JsonTypeInfo<T>? provided)
        => FindTypeInfo(provided, this.jsonOptions, this.options.UseReflectionFallback);

    async Task EnsureConnectionInitializedAsync(CancellationToken ct)
    {
        if (this.connectionInitialized)
            return;

        await this.connection.OpenAsync(ct).ConfigureAwait(false);
        await this.provider.InitializeConnectionAsync(this.connection, ct).ConfigureAwait(false);
        this.connectionInitialized = true;
    }

    async Task EnsureTableInitializedAsync(string tableName, CancellationToken ct)
    {
        await this.EnsureConnectionInitializedAsync(ct).ConfigureAwait(false);

        if (!this.initializedTables.Add(tableName))
            return;

        await using var createCmd = this.connection.CreateCommand();
        createCmd.CommandText = this.provider.BuildCreateTableSql(tableName);
        this.Log(createCmd.CommandText);
        await createCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        await using var indexCmd = this.connection.CreateCommand();
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

    async Task<TResult> ExecuteAsync<TResult>(string tableName, Func<Task<TResult>> operation, CancellationToken ct)
    {
        await this.semaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await this.EnsureTableInitializedAsync(tableName, ct).ConfigureAwait(false);
            return await operation().ConfigureAwait(false);
        }
        finally
        {
            this.semaphore.Release();
        }
    }

    async Task ExecuteAsync(string tableName, Func<Task> operation, CancellationToken ct)
    {
        await this.semaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await this.EnsureTableInitializedAsync(tableName, ct).ConfigureAwait(false);
            await operation().ConfigureAwait(false);
        }
        finally
        {
            this.semaphore.Release();
        }
    }

    async Task<TResult> ExecuteWithResultAsync<TResult>(string tableName, Func<Task<TResult>> operation, CancellationToken ct)
    {
        await this.semaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await this.EnsureTableInitializedAsync(tableName, ct).ConfigureAwait(false);
            return await operation().ConfigureAwait(false);
        }
        finally
        {
            this.semaphore.Release();
        }
    }

    async Task InsertCoreAsync(string tableName, string id, string typeName, string json, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;

        await using var cmd = this.connection.CreateCommand();
        cmd.CommandText = this.provider.BuildInsertSql(tableName);
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

    async Task UpdateCoreAsync(string tableName, string id, string typeName, string json, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;

        await using var cmd = this.connection.CreateCommand();
        cmd.CommandText = this.provider.BuildUpdateSql(tableName);
        AddParameter(cmd, "@id", id);
        AddParameter(cmd, "@typeName", typeName);
        AddParameter(cmd, "@data", json);
        AddParameter(cmd, "@now", now);

        this.Log(cmd.CommandText);
        var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (rows == 0)
            throw new InvalidOperationException(
                $"No document of type '{typeName}' with Id '{id}' was found to update.");
    }

    async Task UpsertMergeCoreAsync(string tableName, string id, string typeName, string json, CancellationToken ct)
    {
        json = StripNullProperties(json);
        var now = DateTimeOffset.UtcNow;

        await using var cmd = this.connection.CreateCommand();
        cmd.CommandText = this.provider.BuildUpsertMergeSql(tableName);
        AddParameter(cmd, "@id", id);
        AddParameter(cmd, "@typeName", typeName);
        AddParameter(cmd, "@data", json);
        AddParameter(cmd, "@now", now);

        this.Log(cmd.CommandText);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    async Task<bool> SetPropertyCoreAsync(string tableName, string id, string typeName, string jsonPath, object? value, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;

        await using var cmd = this.connection.CreateCommand();
        cmd.CommandText = this.provider.BuildSetPropertySql(tableName);
        AddParameter(cmd, "@path", "$." + jsonPath);
        AddParameter(cmd, "@value", this.provider.FormatPropertyValue(value));
        AddParameter(cmd, "@now", now);
        AddParameter(cmd, "@id", id);
        AddParameter(cmd, "@typeName", typeName);

        this.Log(cmd.CommandText);
        var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        return rows > 0;
    }

    async Task<bool> RemovePropertyCoreAsync(string tableName, string id, string typeName, string jsonPath, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;

        await using var cmd = this.connection.CreateCommand();
        cmd.CommandText = this.provider.BuildRemovePropertySql(tableName);
        AddParameter(cmd, "@path", "$." + jsonPath);
        AddParameter(cmd, "@now", now);
        AddParameter(cmd, "@id", id);
        AddParameter(cmd, "@typeName", typeName);

        this.Log(cmd.CommandText);
        var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        return rows > 0;
    }

    async Task<string> GenerateIdAsync(IdKind kind, string tableName, string typeName, CancellationToken ct)
        => await GenerateIdCoreAsync(kind, tableName, typeName, this.connection.CreateCommand, this.provider, s => this.Log(s), ct).ConfigureAwait(false);

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

    Task<TResult> IQueryExecutor.ExecuteAsync<TResult>(Func<Task<TResult>> operation, CancellationToken ct)
        => this.ExecuteAsync(this.options.TableName, operation, ct);

    IAsyncEnumerable<T> IQueryExecutor.ReadStreamAsync<T>(Action<DbCommand> configure, Func<string, T> deserialize, CancellationToken ct)
        => this.ReadStreamAsync(configure, deserialize, ct);

    DbCommand IQueryExecutor.CreateCommand()
        => this.connection.CreateCommand();

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

    // ── Query<T>() entry point ──────────────────────────────────────────

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        return new DocumentQuery<T>(this, FindTypeInfo(jsonTypeInfo));
    }

    // ── CRUD ────────────────────────────────────────────────────────────

    public Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName, async () =>
        {
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
            var json = SerializeDocument(document, typeInfo, this.jsonOptions);
            await this.InsertCoreAsync(tableName, id, this.ResolveTypeName<T>(), json, cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    public Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        var typeName = this.ResolveTypeName<T>();

        return this.ExecuteWithResultAsync(tableName, async () =>
        {
            await using var transaction = await this.connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                DbCommand txCreateCommand() { var c = this.connection.CreateCommand(); c.Transaction = transaction; return c; }
                var count = await BatchInsertCoreAsync(
                    tableName, typeName, documents, accessor, typeInfo,
                    this.jsonOptions, this.logging, this.provider,
                    txCreateCommand,
                    (kind, tbl, tn, ct) => GenerateIdCoreAsync(kind, tbl, tn, txCreateCommand, this.provider, this.logging, ct),
                    cancellationToken
                ).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return count;
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }
        }, cancellationToken);
    }

    public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName, async () =>
        {
            if (accessor.IsDefaultId(document))
                throw new InvalidOperationException(
                    $"Update requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Update.");

            var id = accessor.GetIdAsString(document);
            var json = SerializeDocument(document, typeInfo, this.jsonOptions);
            await this.UpdateCoreAsync(tableName, id, this.ResolveTypeName<T>(), json, cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var accessor = this.idCache.GetOrCreate(typeInfo);
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName, async () =>
        {
            if (accessor.IsDefaultId(patch))
                throw new InvalidOperationException(
                    $"Upsert requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

            var id = accessor.GetIdAsString(patch);
            var json = SerializeDocument(patch, typeInfo, this.jsonOptions);
            await this.UpsertMergeCoreAsync(tableName, id, this.ResolveTypeName<T>(), json, cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    public Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName,
            () => this.SetPropertyCoreAsync(tableName, resolvedId, this.ResolveTypeName<T>(), jsonPath, value, cancellationToken),
            cancellationToken);
    }

    public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName,
            () => this.RemovePropertyCoreAsync(tableName, resolvedId, this.ResolveTypeName<T>(), jsonPath, cancellationToken),
            cancellationToken);
    }

    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName, async () =>
        {
            await using var cmd = this.connection.CreateCommand();
            cmd.CommandText = $"SELECT Data FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName;";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());

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
        return this.ExecuteAsync(tableName, async () =>
        {
            await using var cmd = this.connection.CreateCommand();
            cmd.CommandText = $"SELECT Data FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName;";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());

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
        return this.ExecuteAsync(tableName, async () =>
        {
            await using var cmd = this.connection.CreateCommand();
            cmd.CommandText = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName AND ({whereClause});";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            BindParameters(cmd, parameters);

            this.Log(cmd.CommandText);
            return await ReadListAsync<T>(cmd, json => DeserializeDocument(json, typeInfo, this.jsonOptions)!, cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    // ── String-based streaming ──────────────────────────────────────────

    async IAsyncEnumerable<T> ReadStreamAsync<T>(
        Action<DbCommand> configureCommand,
        Func<string, T> deserialize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await this.semaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await this.EnsureConnectionInitializedAsync(ct).ConfigureAwait(false);

            await using var cmd = this.connection.CreateCommand();
            configureCommand(cmd);

            this.Log(cmd.CommandText);
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var json = reader.GetString(0);
                yield return deserialize(json);
            }
        }
        finally
        {
            this.semaphore.Release();
        }
    }

    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
    {
        var typeInfo = FindTypeInfo(jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var tableName = this.ResolveTableName<T>();
        return this.ReadStreamAsync<T>(
            cmd =>
            {
                cmd.CommandText = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName AND ({whereClause});";
                AddParameter(cmd, "@typeName", typeName);
                BindParameters(cmd, parameters);
            },
            json => DeserializeDocument(json, typeInfo, this.jsonOptions)!,
            cancellationToken);
    }

    // ── Count / Remove / Clear ──────────────────────────────────────────

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
    {
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName, async () =>
        {
            await using var cmd = this.connection.CreateCommand();
            var sql = $"SELECT COUNT(*) FROM {Qt(tableName)} WHERE TypeName = @typeName";
            if (!string.IsNullOrWhiteSpace(whereClause))
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            BindParameters(cmd, parameters);

            this.Log(cmd.CommandText);
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return Convert.ToInt32(result);
        }, cancellationToken);
    }

    public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class
    {
        var resolvedId = this.idCache.GetOrCreate<T>(null).ResolveId(id);
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName, async () =>
        {
            await using var cmd = this.connection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName;";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());

            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return rows > 0;
        }, cancellationToken);
    }

    public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
    {
        var tableName = this.ResolveTableName<T>();
        return this.ExecuteAsync(tableName, async () =>
        {
            await using var cmd = this.connection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {Qt(tableName)} WHERE TypeName = @typeName;";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());

            this.Log(cmd.CommandText);
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken);
    }

    // ── Transaction ─────────────────────────────────────────────────────

    public Task RunInTransaction(Func<IDocumentStore, Task> operation, CancellationToken cancellationToken = default)
    {
        return this.ExecuteAsync(this.options.TableName, async () =>
        {
            await using var transaction = await this.connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var txStore = new TransactionalDocumentStore(this.connection, transaction, this.options, this.provider, this.jsonOptions, this.logging, this.idCache, this.initializedTables);
                await operation(txStore).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }
        }, cancellationToken);
    }

    // ── Index management ────────────────────────────────────────────────

    public Task CreateIndexAsync<T>(Expression<Func<T, object>> expression, JsonTypeInfo<T> jsonTypeInfo, CancellationToken cancellationToken = default) where T : class
    {
        var jsonPath = IndexExpressionHelper.ResolveJsonPath(expression, this.jsonOptions, jsonTypeInfo);
        var typeName = this.ResolveTypeName<T>();
        var tableName = this.ResolveTableName<T>();
        var indexName = IndexExpressionHelper.BuildIndexName(typeName, jsonPath);

        return this.ExecuteAsync(tableName, async () =>
        {
            await using var cmd = this.connection.CreateCommand();
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

        return this.ExecuteAsync(tableName, async () =>
        {
            await using var cmd = this.connection.CreateCommand();
            cmd.CommandText = this.provider.BuildDropIndexSql(indexName);
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

        return this.ExecuteAsync(tableName, async () =>
        {
            await using var queryCmd = this.connection.CreateCommand();
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
                await using var dropCmd = this.connection.CreateCommand();
                dropCmd.CommandText = this.provider.BuildDropIndexSql(indexName);
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

        foreach (var key in obj.Where(kv => kv.Value is null).Select(kv => kv.Key).ToList())
            obj.Remove(key);

        return obj.ToJsonString();
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

    public Task Backup(string destinationPath, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);

        if (!this.provider.SupportsBackup)
            throw new NotSupportedException("The current database provider does not support backup.");

        return this.ExecuteAsync(this.options.TableName, () =>
            this.provider.BackupAsync(this.connection, destinationPath, cancellationToken),
            cancellationToken);
    }

    public void Dispose()
    {
        this.connection.Dispose();
        this.semaphore.Dispose();
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
        readonly HashSet<string> initializedTables;

        public TransactionalDocumentStore(
            DbConnection connection,
            DbTransaction transaction,
            DocumentStoreOptions options,
            IDatabaseProvider provider,
            JsonSerializerOptions jsonOptions,
            Action<string>? logging,
            IdAccessorCache idCache,
            HashSet<string> initializedTables)
        {
            this.connection = connection;
            this.transaction = transaction;
            this.options = options;
            this.provider = provider;
            this.jsonOptions = jsonOptions;
            this.logging = logging;
            this.idCache = idCache;
            this.initializedTables = initializedTables;
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
            if (!this.initializedTables.Add(tableName))
                return;

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

        // ── IQueryExecutor ──────────────────────────────────────────────

        Task<TResult> IQueryExecutor.ExecuteAsync<TResult>(Func<Task<TResult>> operation, CancellationToken ct)
            => operation();

        IAsyncEnumerable<T> IQueryExecutor.ReadStreamAsync<T>(Action<DbCommand> configure, Func<string, T> deserialize, CancellationToken ct)
            => ReadStreamInternalAsync(configure, deserialize, ct);

        DbCommand IQueryExecutor.CreateCommand() => this.CreateCommand();

        string IQueryExecutor.ResolveTypeName<T>() => this.ResolveTypeName<T>();

        string IQueryExecutor.ResolveTableName<T>() => this.ResolveTableName<T>();

        JsonSerializerOptions IQueryExecutor.JsonOptions => this.jsonOptions;

        Action<string>? IQueryExecutor.Logging => this.logging;

        IDatabaseProvider IQueryExecutor.Provider => this.provider;

        async IAsyncEnumerable<T> ReadStreamInternalAsync<T>(
            Action<DbCommand> configure,
            Func<string, T> deserialize,
            [EnumeratorCancellation] CancellationToken ct)
        {
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
            cmd.CommandText = this.provider.BuildInsertSql(tableName);
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

        async Task UpdateCoreAsync(string tableName, string id, string typeName, string json, CancellationToken ct)
        {
            await this.EnsureTableAsync(tableName, ct).ConfigureAwait(false);
            var now = DateTimeOffset.UtcNow;
            await using var cmd = this.CreateCommand();
            cmd.CommandText = this.provider.BuildUpdateSql(tableName);
            AddParameter(cmd, "@id", id);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@data", json);
            AddParameter(cmd, "@now", now);
            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (rows == 0)
                throw new InvalidOperationException(
                    $"No document of type '{typeName}' with Id '{id}' was found to update.");
        }

        async Task UpsertMergeCoreAsync(string tableName, string id, string typeName, string json, CancellationToken ct)
        {
            await this.EnsureTableAsync(tableName, ct).ConfigureAwait(false);
            json = StripNullProperties(json);
            var now = DateTimeOffset.UtcNow;
            await using var cmd = this.CreateCommand();
            cmd.CommandText = this.provider.BuildUpsertMergeSql(tableName);
            AddParameter(cmd, "@id", id);
            AddParameter(cmd, "@typeName", typeName);
            AddParameter(cmd, "@data", json);
            AddParameter(cmd, "@now", now);
            this.Log(cmd.CommandText);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        async Task<bool> SetPropertyCoreAsync(string tableName, string id, string typeName, string jsonPath, object? value, CancellationToken ct)
        {
            await this.EnsureTableAsync(tableName, ct).ConfigureAwait(false);
            var now = DateTimeOffset.UtcNow;
            await using var cmd = this.CreateCommand();
            cmd.CommandText = this.provider.BuildSetPropertySql(tableName);
            AddParameter(cmd, "@path", "$." + jsonPath);
            AddParameter(cmd, "@value", this.provider.FormatPropertyValue(value));
            AddParameter(cmd, "@now", now);
            AddParameter(cmd, "@id", id);
            AddParameter(cmd, "@typeName", typeName);
            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            return rows > 0;
        }

        async Task<bool> RemovePropertyCoreAsync(string tableName, string id, string typeName, string jsonPath, CancellationToken ct)
        {
            await this.EnsureTableAsync(tableName, ct).ConfigureAwait(false);
            var now = DateTimeOffset.UtcNow;
            await using var cmd = this.CreateCommand();
            cmd.CommandText = this.provider.BuildRemovePropertySql(tableName);
            AddParameter(cmd, "@path", "$." + jsonPath);
            AddParameter(cmd, "@now", now);
            AddParameter(cmd, "@id", id);
            AddParameter(cmd, "@typeName", typeName);
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

        public async Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);
            var tableName = this.ResolveTableName<T>();

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
            var json = SerializeDocument(document, typeInfo, this.jsonOptions);
            await this.InsertCoreAsync(tableName, id, this.ResolveTypeName<T>(), json, cancellationToken).ConfigureAwait(false);
        }

        public async Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);
            var tableName = this.ResolveTableName<T>();
            var typeName = this.ResolveTypeName<T>();
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);

            return await BatchInsertCoreAsync(
                tableName, typeName, documents, accessor, typeInfo,
                this.jsonOptions, this.logging, this.provider,
                this.CreateCommand,
                this.GenerateIdAsync,
                cancellationToken
            ).ConfigureAwait(false);
        }

        public async Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);

            if (accessor.IsDefaultId(document))
                throw new InvalidOperationException(
                    $"Update requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Update.");

            var id = accessor.GetIdAsString(document);
            var json = SerializeDocument(document, typeInfo, this.jsonOptions);
            await this.UpdateCoreAsync(this.ResolveTableName<T>(), id, this.ResolveTypeName<T>(), json, cancellationToken).ConfigureAwait(false);
        }

        public async Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var accessor = this.idCache.GetOrCreate(typeInfo);

            if (accessor.IsDefaultId(patch))
                throw new InvalidOperationException(
                    $"Upsert requires a non-default Id on the document. " +
                    $"Set the Id property on '{typeof(T).Name}' before calling Upsert.");

            var id = accessor.GetIdAsString(patch);
            var json = SerializeDocument(patch, typeInfo, this.jsonOptions);
            await this.UpsertMergeCoreAsync(this.ResolveTableName<T>(), id, this.ResolveTypeName<T>(), json, cancellationToken).ConfigureAwait(false);
        }

        public async Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
            var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
            var tableName = this.ResolveTableName<T>();
            return await this.SetPropertyCoreAsync(tableName, resolvedId, this.ResolveTypeName<T>(), jsonPath, value, cancellationToken).ConfigureAwait(false);
        }

        public async Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
            var jsonPath = ResolvePropertyPath(property, this.jsonOptions, typeInfo);
            var tableName = this.ResolveTableName<T>();
            return await this.RemovePropertyCoreAsync(tableName, resolvedId, this.ResolveTypeName<T>(), jsonPath, cancellationToken).ConfigureAwait(false);
        }

        public async Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        {
            var typeInfo = FindTypeInfo(jsonTypeInfo);
            var resolvedId = this.idCache.GetOrCreate(typeInfo).ResolveId(id);
            var tableName = this.ResolveTableName<T>();
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            await using var cmd = this.CreateCommand();
            cmd.CommandText = $"SELECT Data FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName;";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());

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
            cmd.CommandText = $"SELECT Data FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName;";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());

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
            cmd.CommandText = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName AND ({whereClause});";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
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
            cmd.CommandText = $"SELECT Data FROM {Qt(tableName)} WHERE TypeName = @typeName AND ({whereClause});";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
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
            if (!string.IsNullOrWhiteSpace(whereClause))
                sql += $" AND ({whereClause})";
            cmd.CommandText = sql + ";";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            BindParameters(cmd, parameters);

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
            cmd.CommandText = $"DELETE FROM {Qt(tableName)} WHERE Id = @id AND TypeName = @typeName;";
            AddParameter(cmd, "@id", resolvedId);
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.Log(cmd.CommandText);
            var rows = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return rows > 0;
        }

        public async Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class
        {
            var tableName = this.ResolveTableName<T>();
            await this.EnsureTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            await using var cmd = this.CreateCommand();
            cmd.CommandText = $"DELETE FROM {Qt(tableName)} WHERE TypeName = @typeName;";
            AddParameter(cmd, "@typeName", this.ResolveTypeName<T>());
            this.Log(cmd.CommandText);
            return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public Task RunInTransaction(Func<IDocumentStore, Task> operation, CancellationToken cancellationToken = default)
        {
            throw new InvalidOperationException("Nested transactions are not supported.");
        }

        public Task Backup(string destinationPath, CancellationToken cancellationToken = default)
        {
            throw new InvalidOperationException("Backup is not supported inside a transaction.");
        }
    }
}
