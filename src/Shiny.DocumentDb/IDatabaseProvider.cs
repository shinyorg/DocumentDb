using System.Data.Common;
using System.Text;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

public interface IDatabaseProvider
{
    // Connection
    DbConnection CreateConnection();
    Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct);

    /// <summary>
    /// When true, the document store keeps a single long-lived connection and serializes every
    /// operation through a semaphore. Use for engines that lock the entire database on writes or
    /// don't tolerate multiple concurrent connections (e.g. file-based SQLite). When false, the
    /// store opens a fresh connection per operation and relies on the ADO.NET driver's built-in
    /// connection pool to multiplex concurrent callers — the default for server SQL providers.
    /// </summary>
    bool RequiresSingleConnection => false;

    // Schema DDL
    string BuildCreateTableSql(string tableName);
    string BuildCreateTypenameIndexSql(string tableName);

    // Multi-tenancy DDL (idempotent — safe to call on existing tables)
    string BuildAddTenantColumnSql(string tableName)
        => $"ALTER TABLE {QuoteTable(tableName)} ADD COLUMN TenantId TEXT;";

    string BuildCreateTenantIndexSql(string tableName)
        => $"CREATE INDEX IF NOT EXISTS IX_{tableName}_TenantId ON {QuoteTable(tableName)} (TenantId, TypeName);";

    // CRUD SQL builders
    string BuildInsertSql(string tableName);

    // Batch insert – multi-row VALUES for single round-trip
    string BuildBatchInsertSql(string tableName, int batchSize)
    {
        var qt = QuoteTable(tableName);
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {qt} (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, @data_{i}, @now, @now)");
        }
        sb.Append(';');
        return sb.ToString();
    }
    string BuildUpdateSql(string tableName);
    string BuildUpsertMergeSql(string tableName);
    string BuildSetPropertySql(string tableName);
    string BuildRemovePropertySql(string tableName);
    string BuildMaxIdSql(string tableName);

    // Index management
    string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName);

    /// <summary>
    /// Builds DDL for a composite (multi-column) JSON expression index over the given paths.
    /// Default implementation throws — providers that support composite indexes override this.
    /// Single-path callers continue to use <see cref="BuildCreateJsonIndexSql(string, string, string, string)"/>.
    /// </summary>
    string BuildCreateJsonIndexSql(string indexName, string tableName, IReadOnlyList<string> jsonPaths, string typeName)
        => jsonPaths.Count == 1
            ? BuildCreateJsonIndexSql(indexName, tableName, jsonPaths[0], typeName)
            : throw new NotSupportedException("This provider does not support composite (multi-column) JSON indexes.");

    string BuildDropIndexSql(string indexName, string tableName);
    string BuildListJsonIndexesSql(string tableName, string prefix);

    // RFC 7396 JSON Merge Patch support.
    // Providers that lack a native deep-merge function (PostgreSQL, SQL Server) return false;
    // DocumentStore then performs a read-merge-write fallback inside a row-locked transaction.
    bool SupportsJsonMergePatch => true;

    // Returns a row-locking SELECT for the fallback merge path. Only consulted when
    // SupportsJsonMergePatch is false.
    string BuildSelectDataForUpdateSql(string tableName)
        => $"SELECT Data FROM {QuoteTable(tableName)} WHERE Id = @id AND TypeName = @typeName";

    // JSON SQL dialect fragments (used by expression visitors)
    string JsonExtract(string column, string jsonPath);
    string JsonExtractTyped(string column, string jsonPath, Type clrType) => JsonExtract(column, jsonPath);
    string JsonExtractElement(string jsonPath);
    string JsonExtractElementTyped(string jsonPath, Type clrType) => JsonExtractElement(jsonPath);
    string JsonExtractElementNumeric(string jsonPath);
    string CastIntegerAggregate(string expression);
    string JsonExtractNumeric(string column, string jsonPath);
    string JsonArrayLength(string column, string jsonPath);
    string JsonEachFrom(string column, string jsonPath);
    string JsonObject(IEnumerable<string> keyValuePairs);
    string JsonTrue();
    string JsonFalse();
    string JsonNullCheck(string column, string jsonPath, bool isNull);
    string JsonEachPrimitiveValue { get; }
    string JsonEachPrimitiveNumericValue { get; }

    // SQL dialect helpers
    string QuoteTable(string tableName);
    string ConcatStrings(params string[] parts);
    string BuildJsonSetExpression();
    object FormatPropertyValue(object? value);

    // Pagination
    string BuildPaginationClause(int offset, int take);

    // Error classification
    bool IsDuplicateKeyException(Exception ex);

    // Native change feed (optional — PostgreSQL and SQL Server implement these)
    bool SupportsChangeFeed => false;

    /// <summary>
    /// Begins a native change-feed subscription against <paramref name="tableName"/>, delivering
    /// changes for the given <paramref name="typeName"/>. The provider owns its own connection(s)
    /// and any required provisioning (triggers, change tracking). Returns a handle that stops the
    /// subscription when disposed.
    /// </summary>
    Task<IAsyncDisposable> SubscribeChangesAsync(
        string tableName,
        string typeName,
        Func<RawDocumentChange, CancellationToken, Task> onChange,
        CancellationToken cancellationToken)
        => throw new NotSupportedException("This provider does not support native change feeds.");

    // Temporal / system-versioned history (optional — relational providers implement these).
    // A sidecar "{tableName}_history" table records every version of a document. The DML below is
    // portable ANSI SQL with default implementations; a provider only needs to set
    // SupportsTemporal => true and override BuildCreateHistoryTableSql with its column types.
    // PostgreSQL/DuckDB additionally override BuildHistoryInsertSql to cast the JSON payload, and
    // MySQL overrides BuildHistoryPruneByCountSql (it forbids self-referencing DELETE subqueries).
    bool SupportsTemporal => false;

    /// <summary>The sidecar history table name for a given documents table.</summary>
    string HistoryTableName(string tableName) => tableName + "_history";

    /// <summary>DDL that creates the per-table history sidecar (idempotent). Returns null when unsupported.</summary>
    string? BuildCreateHistoryTableSql(string tableName) => null;

    /// <summary>
    /// Secondary indexes on the history sidecar that back the fleet-wide queries (AsOfAll /
    /// ChangesByActor / ChangesBetween). Each is executed as its own statement, idempotently — the
    /// caller swallows "already exists". The default builds a (TypeName, ValidFrom, ValidTo) index
    /// for time-range / point-in-time scans and a (TypeName, Actor) index for by-actor lookups.
    /// </summary>
    IReadOnlyList<string> BuildCreateHistoryIndexesSql(string tableName)
    {
        var h = QuoteTable(HistoryTableName(tableName));
        return new[]
        {
            $"CREATE INDEX idx_{tableName}_history_time ON {h} (TypeName, ValidFrom, ValidTo)",
            $"CREATE INDEX idx_{tableName}_history_actor ON {h} (TypeName, Actor)"
        };
    }

    /// <summary>
    /// Closes the currently-open version of a document by stamping its <c>ValidTo</c>.
    /// Bound parameters: <c>@id</c>, <c>@typeName</c>, <c>@validTo</c>.
    /// </summary>
    string BuildHistoryCloseSql(string tableName)
        => $"UPDATE {QuoteTable(HistoryTableName(tableName))} SET ValidTo = @validTo " +
           "WHERE Id = @id AND TypeName = @typeName AND ValidTo IS NULL";

    /// <summary>
    /// Appends a new open version, computing the next version number atomically.
    /// Bound parameters: <c>@id</c>, <c>@typeName</c>, <c>@validFrom</c>, <c>@operation</c>, <c>@actor</c>, <c>@data</c>.
    /// </summary>
    string BuildHistoryInsertSql(string tableName)
    {
        var h = QuoteTable(HistoryTableName(tableName));
        return $"INSERT INTO {h} (Id, TypeName, Version, ValidFrom, ValidTo, Operation, Actor, Data) " +
               "SELECT @id, @typeName, COALESCE(MAX(Version), 0) + 1, @validFrom, NULL, @operation, @actor, @data " +
               $"FROM {h} WHERE Id = @id AND TypeName = @typeName";
    }

    // All version-row reads yield the same 7 columns in this order:
    //   0 Id, 1 Version, 2 ValidFrom, 3 ValidTo, 4 Operation, 5 Actor, 6 Data
    const string HistoryVersionColumns = "Id, Version, ValidFrom, ValidTo, Operation, Actor, Data";

    /// <summary>
    /// Selects every version of one document ordered by version ascending.
    /// Yields the standard 7 version columns. Bound parameters: <c>@id</c>, <c>@typeName</c>.
    /// </summary>
    string BuildHistorySelectSql(string tableName)
        => $"SELECT {HistoryVersionColumns} FROM {QuoteTable(HistoryTableName(tableName))} " +
           "WHERE Id = @id AND TypeName = @typeName ORDER BY Version ASC";

    /// <summary>
    /// Selects the live (non-tombstone) Data of every document of the type as of <c>@asOf</c> — a
    /// fleet-wide point-in-time snapshot. Bound parameters: <c>@typeName</c>, <c>@asOf</c>.
    /// </summary>
    string BuildHistoryAsOfAllSql(string tableName)
        => $"SELECT Data FROM {QuoteTable(HistoryTableName(tableName))} " +
           "WHERE TypeName = @typeName AND ValidFrom <= @asOf AND (ValidTo IS NULL OR ValidTo > @asOf) AND Data IS NOT NULL";

    /// <summary>
    /// Selects every version authored by <c>@actor</c> across all documents of the type, oldest
    /// first. Yields the standard 7 version columns. Bound parameters: <c>@typeName</c>, <c>@actor</c>.
    /// </summary>
    string BuildHistoryByActorSql(string tableName)
        => $"SELECT {HistoryVersionColumns} FROM {QuoteTable(HistoryTableName(tableName))} " +
           "WHERE TypeName = @typeName AND Actor = @actor ORDER BY ValidFrom ASC, Id ASC, Version ASC";

    /// <summary>
    /// Selects every version whose <c>ValidFrom</c> falls in <c>[@from, @to)</c> across all documents
    /// of the type, oldest first — an audit log. Yields the standard 7 version columns.
    /// Bound parameters: <c>@typeName</c>, <c>@from</c>, <c>@to</c>.
    /// </summary>
    string BuildHistoryBetweenSql(string tableName)
        => $"SELECT {HistoryVersionColumns} FROM {QuoteTable(HistoryTableName(tableName))} " +
           "WHERE TypeName = @typeName AND ValidFrom >= @from AND ValidFrom < @to ORDER BY ValidFrom ASC, Id ASC, Version ASC";

    /// <summary>
    /// Selects the version(s) current as of <c>@asOf</c> (Data, Operation), newest first — the caller
    /// reads only the first row. Bound parameters: <c>@id</c>, <c>@typeName</c>, <c>@asOf</c>.
    /// </summary>
    string BuildHistoryAsOfSql(string tableName)
        => $"SELECT Data, Operation FROM {QuoteTable(HistoryTableName(tableName))} " +
           "WHERE Id = @id AND TypeName = @typeName AND ValidFrom <= @asOf AND (ValidTo IS NULL OR ValidTo > @asOf) " +
           "ORDER BY Version DESC";

    /// <summary>Selects the Data of one specific version. Bound parameters: <c>@id</c>, <c>@typeName</c>, <c>@version</c>.</summary>
    string BuildHistoryVersionSql(string tableName)
        => $"SELECT Data FROM {QuoteTable(HistoryTableName(tableName))} " +
           "WHERE Id = @id AND TypeName = @typeName AND Version = @version";

    /// <summary>Prunes closed versions whose <c>ValidTo</c> precedes <c>@cutoff</c>. Bound parameters: <c>@id</c>, <c>@typeName</c>, <c>@cutoff</c>.</summary>
    string BuildHistoryPruneByAgeSql(string tableName)
        => $"DELETE FROM {QuoteTable(HistoryTableName(tableName))} " +
           "WHERE Id = @id AND TypeName = @typeName AND ValidTo IS NOT NULL AND ValidTo < @cutoff";

    /// <summary>
    /// Prunes all but the newest <c>@keep</c> versions. Versions are monotonic per document, so the
    /// cutoff is <c>MAX(Version) - @keep</c>. Bound parameters: <c>@id</c>, <c>@typeName</c>, <c>@keep</c>.
    /// </summary>
    string BuildHistoryPruneByCountSql(string tableName)
    {
        var h = QuoteTable(HistoryTableName(tableName));
        return $"DELETE FROM {h} WHERE Id = @id AND TypeName = @typeName AND Version <= " +
               $"(SELECT MAX(Version) FROM {h} WHERE Id = @id AND TypeName = @typeName) - @keep";
    }

    // Spatial (optional — only SQLite implements these)
    bool SupportsSpatial => false;
    string? BuildCreateSpatialTablesSql(string tableName) => null;
    string? BuildSpatialUpsertSql(string tableName) => null;
    string? BuildSpatialDeleteSql(string tableName) => null;
    string? BuildSpatialClearSql(string tableName) => null;
    string? BuildSpatialBoundingBoxQuerySql(string tableName, string? additionalWhere) => null;

    // Vector (optional)
    bool SupportsVector => false;

    /// <summary>
    /// Called on every freshly opened connection when at least one vector mapping is registered.
    /// Implementations may load extensions (SQLite vec0, DuckDB vss).
    /// </summary>
    Task LoadVectorExtensionAsync(DbConnection connection, CancellationToken ct) => Task.CompletedTask;

    /// <summary>Returns DDL that creates the per-type sidecar vector storage + index.</summary>
    string? BuildCreateVectorTablesSql(string tableName, string typeName, VectorMapping mapping) => null;

    /// <summary>Upsert SQL — bound parameters: <c>@vecDocId</c>, <c>@vecTypeName</c>, <c>@embedding</c>.</summary>
    string? BuildVectorUpsertSql(string tableName, string typeName, VectorMapping mapping) => null;

    /// <summary>Delete SQL — bound parameters: <c>@vecDocId</c>, <c>@vecTypeName</c>.</summary>
    string? BuildVectorDeleteSql(string tableName, string typeName) => null;

    /// <summary>Clear SQL — bound parameters: <c>@vecTypeName</c>.</summary>
    string? BuildVectorClearSql(string tableName, string typeName) => null;

    /// <summary>
    /// Converts the vector value into the form expected by this provider's parameter binder.
    /// Default returns the raw <c>float[]</c>; pgvector returns a string literal, vec0 packs
    /// to a byte[] of float32, etc.
    /// </summary>
    object FormatVectorParameter(ReadOnlyMemory<float> vector, VectorMapping mapping) => vector.ToArray();

    /// <summary>
    /// Builds the ANN search SQL. Returns the SQL plus the parameter dictionary used to
    /// bind the query vector — provider-specific because each backend's literal syntax differs.
    /// The returned SELECT must yield two columns: the document <c>Data</c> JSON, and the
    /// numeric score.
    /// </summary>
    (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildVectorSearchSql(
        string tableName,
        string typeName,
        VectorMapping mapping,
        ReadOnlyMemory<float> query,
        int k,
        string? additionalWhere)
        => throw new NotSupportedException("Vector queries are not supported by this provider.");
}
