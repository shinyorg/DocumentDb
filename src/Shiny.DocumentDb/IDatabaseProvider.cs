using System.Data.Common;
using System.Text;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

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

    /// <summary>
    /// True when the provider implements <see cref="BuildBatchUpsertSql"/> — a single multi-row
    /// INSERT … ON CONFLICT … DO UPDATE that deep-merges (RFC 7396) every row in one round-trip.
    /// Only SQLite and DuckDB qualify (native <c>json_patch</c> / <c>json_merge_patch</c> plus
    /// <c>excluded</c> reference support). Providers that return false take the per-document upsert
    /// loop inside one transaction instead.
    /// </summary>
    bool SupportsBatchUpsert => false;

    /// <summary>
    /// Multi-row upsert (deep merge) for a single round-trip — only consulted when
    /// <see cref="SupportsBatchUpsert"/> is true. Shared parameters: <c>@typeName</c>, <c>@now</c>;
    /// per-row: <c>@id_i</c>, <c>@data_i</c>. The ON CONFLICT update path must reference the conflicting
    /// row's payload (SQLite/DuckDB <c>excluded.Data</c>) so one clause serves every row.
    /// </summary>
    string BuildBatchUpsertSql(string tableName, int batchSize)
        => throw new NotSupportedException("This provider does not support batch upsert.");

    /// <summary>
    /// Delete-by-id-list for a single round-trip: <c>DELETE … WHERE TypeName=@typeName AND Id IN (…)</c>.
    /// Shared parameter: <c>@typeName</c>; per-row: <c>@id_i</c>. The default ANSI form is correct for
    /// every relational provider (the <c>@id_i</c> convention matches <see cref="BuildBatchInsertSql"/>).
    /// </summary>
    string BuildBatchDeleteByIdsSql(string tableName, int count)
    {
        var sb = new StringBuilder();
        sb.Append($"DELETE FROM {QuoteTable(tableName)} WHERE TypeName = @typeName AND Id IN (");
        for (var i = 0; i < count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"@id_{i}");
        }
        sb.Append(");");
        return sb.ToString();
    }

    // ── Bulk import (IDocumentBackup) collision modes ──────────────────────
    // Insert mode reuses BuildBatchInsertSql (universal). The two below back Replace / SkipExisting.
    // Merge mode reuses BuildBatchUpsertSql (gated by SupportsBatchUpsert). The default ANSI ON CONFLICT
    // form is correct for SQLite / PostgreSQL / DuckDB; MySQL (ON DUPLICATE KEY), SQL Server and Oracle
    // (MERGE) override these. Shared parameters: @typeName, @now; per-row: @id_i, @data_i — matching
    // BuildBatchInsertSql so a provider that casts @data_i there must cast here too.

    /// <summary>True when this provider implements <see cref="BuildBatchReplaceSql"/> and
    /// <see cref="BuildBatchSkipExistingSql"/> for a single multi-row round-trip. False routes those bulk
    /// modes through a per-row fallback inside one transaction.</summary>
    bool SupportsBulkReplace => true;

    /// <summary>Multi-row insert that overwrites the body wholesale on an Id+TypeName conflict.</summary>
    string BuildBatchReplaceSql(string tableName, int batchSize)
    {
        var qt = QuoteTable(tableName);
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {qt} (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, @data_{i}, @now, @now)");
        }
        sb.Append(" ON CONFLICT(Id, TypeName) DO UPDATE SET Data = excluded.Data, UpdatedAt = @now;");
        return sb.ToString();
    }

    /// <summary>Multi-row insert that silently skips rows whose Id+TypeName already exists.</summary>
    string BuildBatchSkipExistingSql(string tableName, int batchSize)
    {
        var qt = QuoteTable(tableName);
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {qt} (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, @data_{i}, @now, @now)");
        }
        sb.Append(" ON CONFLICT(Id, TypeName) DO NOTHING;");
        return sb.ToString();
    }

    /// <summary>
    /// True when the provider can stream an <see cref="BulkWriteMode.Insert"/> chunk via a native bulk-copy
    /// API (PostgreSQL binary <c>COPY</c>, <c>SqlBulkCopy</c>, the DuckDB appender) instead of a multi-row
    /// INSERT — the 10-100× path for mass inserts. Only consulted for Insert mode; Replace/Merge/SkipExisting
    /// continue to use the multi-row VALUES builders.
    /// </summary>
    bool SupportsBulkCopy => false;

    /// <summary>
    /// Streams an Insert chunk into <paramref name="tableName"/> using the provider's native bulk-copy API.
    /// Columns: <c>Id</c> = <see cref="RawBulkRow.Id"/>, <c>TypeName</c> = <paramref name="typeName"/>,
    /// <c>Data</c> = <see cref="RawBulkRow.Data"/> (raw JSON), <c>CreatedAt</c>/<c>UpdatedAt</c> =
    /// <paramref name="timestamp"/>. Runs within <paramref name="transaction"/> when supplied. Returns the
    /// number of rows written. Only called when <see cref="SupportsBulkCopy"/> is true.
    /// </summary>
    Task<long> BulkCopyInsertAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string tableName,
        string typeName,
        IReadOnlyList<RawBulkRow> rows,
        DateTimeOffset timestamp,
        CancellationToken cancellationToken)
        => throw new NotSupportedException("This provider does not support native bulk copy.");

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

    /// <summary>
    /// Lists the user tables in the current database as a single column of names — used by
    /// <see cref="IDocumentMaintenance.ClearAll"/> to wipe every document table and sidecar. The default
    /// queries the ANSI <c>information_schema.tables</c> and excludes the system schemas
    /// (<c>pg_catalog</c>, <c>information_schema</c>) — critical on PostgreSQL, where those are listed as
    /// <c>BASE TABLE</c> and <c>ClearAll</c> would otherwise issue <c>DELETE</c> against system catalogs
    /// (harmless on SQL Server / DuckDB, which don't surface those as base tables). SQLite and Oracle
    /// override with their catalog views; MySQL overrides to scope to the current database (its
    /// <c>information_schema</c> is server-wide).
    /// </summary>
    string BuildListTablesSql()
        => "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' " +
           "AND table_schema NOT IN ('pg_catalog', 'information_schema');";

    // RFC 7396 JSON Merge Patch support.
    // Providers that lack a native deep-merge function (PostgreSQL, SQL Server) return false;
    // DocumentStore then performs a read-merge-write fallback inside a row-locked transaction.
    bool SupportsJsonMergePatch => true;

    // Returns a row-locking SELECT for the fallback merge path. Only consulted when
    // SupportsJsonMergePatch is false.
    string BuildSelectDataForUpdateSql(string tableName)
        => $"SELECT Data FROM {QuoteTable(tableName)} WHERE Id = @id AND TypeName = @typeName";

    // Normalizes a CLR value just before it is bound as a query parameter, letting a provider adapt
    // values its ADO.NET driver would otherwise bind with the wrong storage type. Default is identity;
    // SQLite overrides it to bind decimals as REAL (Microsoft.Data.Sqlite binds decimal as TEXT, and
    // SQLite type affinity ranks TEXT above REAL, so numeric comparisons would never match).
    object? NormalizeParameterValue(object? value) => value;

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

    // ── Scalar function dialect (used by SqlPredicateEmitter / Cosmos) ──
    // Default implementations are ANSI-portable (and correct for SQLite/DuckDB); providers override only
    // the entries whose spelling differs. Arguments arrive as already-emitted SQL fragments.

    /// <summary>Casts an expression to an integer type for bitwise operations.</summary>
    string CastInteger(string expr) => $"CAST({expr} AS INTEGER)";

    /// <summary>Bitwise AND — backs flag-enum tests. Oracle overrides to <c>BITAND</c> (no <c>&amp;</c> operator).</summary>
    string BitAnd(string left, string right) => $"({left} & {right})";

    /// <summary>True when a native/registered <c>soundex</c> function is callable on this provider.</summary>
    bool SupportsSoundex => false;

    /// <summary>True when the provider can register CLR user-defined functions per connection (SQLite, DuckDB).</summary>
    bool SupportsUserFunctions => false;

    string TranslateScalar(ScalarFn fn, IReadOnlyList<string> args, Type resultType)
        => ScalarSqlDefaults.Translate(this, fn, args, resultType);

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
    /// Bound parameters: <c>@typeName</c>, <c>@fromTs</c>, <c>@toTs</c> (named to avoid the Oracle reserved
    /// words <c>FROM</c>/<c>TO</c>, which are rejected as bind-variable names with ORA-01745).
    /// </summary>
    string BuildHistoryBetweenSql(string tableName)
        => $"SELECT {HistoryVersionColumns} FROM {QuoteTable(HistoryTableName(tableName))} " +
           "WHERE TypeName = @typeName AND ValidFrom >= @fromTs AND ValidFrom < @toTs ORDER BY ValidFrom ASC, Id ASC, Version ASC";

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

    /// <summary>
    /// Renders a boolean SQL fragment for a <c>DocumentFunctions</c> spatial predicate used inside a
    /// <c>Where</c> — the stored geometry at <paramref name="jsonPath"/> vs a query geometry (bound as the
    /// supplied parameters). <paramref name="predicate"/> is the op name ("Intersects", "Contains", …,
    /// "WithinDistance"). Returns <c>null</c> when the provider can't translate spatial predicates (the
    /// query layer then throws a clear "use store.Geo*" message). SQLite emits an R*Tree-pruned UDF refine.
    /// </summary>
    string? BuildSpatialFilterSql(
        string tableName, string jsonPath, string predicate,
        string geoJsonParam, string minLatParam, string maxLatParam, string minLngParam, string maxLngParam,
        string? metersParam) => null;

    /// <summary>SQL for a scalar distance (meters or a monotonic proxy) from the stored geometry at
    /// <paramref name="jsonPath"/> to the query geometry — backs <c>DocumentFunctions.Distance</c> in
    /// <c>OrderBy</c>. Null when unsupported.</summary>
    string? BuildSpatialDistanceSql(string jsonPath, string geoJsonParam) => null;

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

    // Full-text search (optional). Unlike vectors, the per-provider full-text index is maintained by
    // the database engine itself — SQLite via triggers, PostgreSQL/MySQL/SQL Server via generated or
    // computed columns, Oracle via an on-commit CONTEXT index — so there are no write-path sync hooks.
    // Everything the engine needs is created once by BuildCreateFullTextSql at table init.
    bool SupportsFullText => false;

    /// <summary>
    /// Returns the DDL statements (executed individually, idempotently) that create the full-text index
    /// for one mapped type in <paramref name="tableName"/>: the FTS virtual table + triggers (SQLite),
    /// the generated/computed text column + index (PostgreSQL/MySQL/SQL Server), or the domain index
    /// (Oracle). Each statement is run on its own and "already exists" failures are swallowed by the
    /// caller. Returns an empty list when unsupported.
    /// </summary>
    IReadOnlyList<string> BuildCreateFullTextSql(string tableName, string typeName, FullTextMapping mapping)
        => Array.Empty<string>();

    /// <summary>
    /// True when <see cref="BuildCreateFullTextSql"/> must be re-run after writes because the index is a
    /// snapshot rather than incrementally maintained (DuckDB's <c>fts</c> extension). The store rebuilds
    /// the index immediately before each full-text query for these providers.
    /// </summary>
    bool FullTextIndexRequiresRebuild => false;

    /// <summary>
    /// Builds the ranked full-text query. Returns the SQL plus the parameter dictionary binding the
    /// search terms. The SELECT must yield two columns: the document <c>Data</c> JSON and a numeric
    /// relevance score where <b>higher is more relevant</b> (negate BM25, etc.), ordered by score
    /// descending. <c>@typeName</c> is bound by the caller; <paramref name="additionalWhere"/> is an
    /// already-emitted predicate over the document alias to AND in as a pre-filter (may be null).
    /// </summary>
    (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildFullTextSearchSql(
        string tableName,
        string typeName,
        FullTextMapping mapping,
        string searchText,
        int maxResults,
        string? additionalWhere)
        => throw new NotSupportedException("Full-text queries are not supported by this provider.");

    // Computed columns (optional). When the provider can materialize a derived value as a native
    // generated/computed column, the store creates it once at table init and the query layer references
    // the column directly (instead of inlining the definition expression). Providers that report false
    // fall back to the always-available alias mode, where the definition is inlined into each query.
    bool SupportsComputedColumns => false;

    /// <summary>
    /// Returns the DDL statements (executed individually, idempotently) that materialize a computed
    /// property as a native generated/computed column on <paramref name="tableName"/>, plus its index when
    /// <see cref="ComputedMapping.Indexed"/> is set. <paramref name="expressionSql"/> is the definition
    /// already lowered to this provider's JSON dialect with literals inlined (a generated-column definition
    /// cannot bind parameters). The index, where created, is scoped to <paramref name="typeName"/> because
    /// the document table is shared across types. Returns an empty list when unsupported.
    /// </summary>
    IReadOnlyList<string> BuildCreateComputedColumnSql(string tableName, string typeName, ComputedMapping mapping, string expressionSql)
        => Array.Empty<string>();
}
