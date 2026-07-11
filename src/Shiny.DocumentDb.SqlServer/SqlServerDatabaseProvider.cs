using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.FullText;

namespace Shiny.DocumentDb.SqlServer;

public class SqlServerDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;
    readonly SqlServerChangeFeedOptions changeFeed;

    public SqlServerDatabaseProvider(string connectionString, SqlServerChangeFeedOptions? changeFeedOptions = null)
    {
        this.connectionString = connectionString;
        this.changeFeed = changeFeedOptions ?? new SqlServerChangeFeedOptions();
    }

    public DbConnection CreateConnection() => new SqlConnection(this.connectionString);

    public Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct) => Task.CompletedTask;

    public string BuildCreateTableSql(string tableName) => $"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{tableName}')
        CREATE TABLE [{tableName}] (
            Id NVARCHAR(450) NOT NULL,
            TypeName NVARCHAR(450) NOT NULL,
            Data JSON NOT NULL,
            CreatedAt DATETIME2 NOT NULL,
            UpdatedAt DATETIME2 NOT NULL,
            CONSTRAINT PK_{tableName} PRIMARY KEY (Id, TypeName)
        );
        """;

    public string BuildCreateTypenameIndexSql(string tableName)
        => $"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_{tableName}_typename') CREATE INDEX idx_{tableName}_typename ON [{tableName}] (TypeName);";

    // ── Spatial ──
    // SQL Server can't parse GeoJSON, so it stores a native (planar) `geometry` column ingested from WKT and
    // uses .ST* method syntax. Planar geometry matches the C# relate engine and avoids the geography left-hand
    // ring-orientation rule. WithinDistance is a planar-degree approximation.
    public bool SupportsSpatial => true;
    public bool RequiresSpatialWkt => !this.PortableSpatial;

    /// <summary>Forces the dependency-free envelope tier (no geometry column, no native ST_* for
    /// DocumentFunctions-in-Where). Default false → native geometry column + method-syntax predicates.</summary>
    public bool PortableSpatial { get; init; }

    public string? BuildSpatialFilterSql(
        string tableName, string jsonPath, string predicate,
        string geoJsonParam, string wktParam, string minLatParam, string maxLatParam, string minLngParam, string maxLngParam,
        string? metersParam)
    {
        if (this.PortableSpatial)
            return null;

        var q = $"geometry::STGeomFromText({wktParam}, 4326).MakeValid()";
        var refine = predicate switch
        {
            "Intersects" => $"geom.STIntersects({q}) = 1",
            "Disjoint" => $"geom.STDisjoint({q}) = 1",
            "Contains" => $"geom.STContains({q}) = 1",
            "Within" => $"geom.STWithin({q}) = 1",
            "Covers" => $"geom.STContains({q}) = 1",     // SQL Server has no STCovers — boundary-inclusive approx
            "CoveredBy" => $"geom.STWithin({q}) = 1",
            "Touches" => $"geom.STTouches({q}) = 1",
            "Crosses" => $"geom.STCrosses({q}) = 1",
            "Overlaps" => $"geom.STOverlaps({q}) = 1",
            "Equals" => $"geom.STEquals({q}) = 1",
            // WithinDistance is intentionally NOT pushed down: the sidecar column is the planar `geometry`
            // type, so geom.STDistance measures degrees, not meters — a native pushdown would be silently
            // wrong away from the equator (and converting to `geography` mid-query is orientation-fragile).
            // Returning null makes the query engine throw a clear error directing callers to
            // store.GeoWithinDistance(...), which refines with an exact Haversine distance in managed code.
            _ => null
        };
        if (refine == null)
            return null;

        // The geom column lives in the sidecar; the whole predicate runs in the IN-subquery over it.
        var bbox = predicate == "Disjoint"
            ? ""  // anti-selective — can't bbox-prune
            : $"AND maxLat >= {minLatParam} AND minLat <= {maxLatParam} AND maxLng >= {minLngParam} AND minLng <= {maxLngParam} ";
        return $"Id IN (SELECT docId FROM [{tableName}_spatial] WHERE typeName = @typeName {bbox}AND {refine})";
    }

    // SQL Server can't build a geometry from GeoJSON, so distance is computed against the sidecar's native
    // (spatially-indexed) geom column via a correlated subquery keyed off the outer row's Id/TypeName, with the
    // query geometry passed as WKT. Planar STDistance (degrees) is monotonic with true distance for ORDER BY.
    // Portable mode has no geom column → null → OrderBy-by-distance throws NotSupportedException.
    public string? BuildSpatialDistanceSql(string? tableName, string jsonPath, string geoJsonParam, string wktParam)
        => this.PortableSpatial || tableName == null ? null
            : $"(SELECT s.geom.STDistance(geometry::STGeomFromText({wktParam}, 4326).MakeValid()) " +
              $"FROM [{tableName}_spatial] s WHERE s.docId = Id AND s.typeName = TypeName)";

    public string? BuildCreateSpatialTablesSql(string tableName)
    {
        var geomCol = this.PortableSpatial ? "" : "geom geometry NULL,\n            ";
        // A SQL Server spatial index requires a clustered PK <= 895 bytes; (docId,typeName) is 1800 bytes, so
        // use a small identity rowid PK + a unique constraint on (docId,typeName) for upsert/lookup.
        return $"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{tableName}_spatial')
        CREATE TABLE [{tableName}_spatial] (
            rowid INT IDENTITY(1,1) NOT NULL,
            docId NVARCHAR(450) NOT NULL,
            typeName NVARCHAR(450) NOT NULL,
            minLat FLOAT NOT NULL, maxLat FLOAT NOT NULL,
            minLng FLOAT NOT NULL, maxLng FLOAT NOT NULL,
            {geomCol}CONSTRAINT PK_{tableName}_spatial PRIMARY KEY (rowid),
            CONSTRAINT UQ_{tableName}_spatial UNIQUE (docId, typeName)
        );
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_{tableName}_spatial')
        CREATE INDEX idx_{tableName}_spatial ON [{tableName}_spatial] (typeName, minLat, maxLat, minLng, maxLng);
        {(this.PortableSpatial ? "" : $"""
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'sidx_{tableName}_spatial')
        CREATE SPATIAL INDEX sidx_{tableName}_spatial ON [{tableName}_spatial](geom)
            USING GEOMETRY_AUTO_GRID WITH (BOUNDING_BOX = (-180, -90, 180, 90));
        """)}
        """;
    }

    public string? BuildSpatialUpsertSql(string tableName)
    {
        var geomSel = this.PortableSpatial ? "" : ", geometry::STGeomFromText(@spatialWkt, 4326).MakeValid() AS geom";
        var geomUpd = this.PortableSpatial ? "" : ", geom = s.geom";
        var geomIns = this.PortableSpatial ? "" : ", geom";
        var geomVal = this.PortableSpatial ? "" : ", s.geom";
        return $"""
        MERGE [{tableName}_spatial] AS t
        USING (SELECT @spatialDocId AS docId, @spatialTypeName AS typeName,
                      @spatialMinLat AS minLat, @spatialMaxLat AS maxLat,
                      @spatialMinLng AS minLng, @spatialMaxLng AS maxLng{geomSel}) AS s
        ON t.docId = s.docId AND t.typeName = s.typeName
        WHEN MATCHED THEN UPDATE SET minLat = s.minLat, maxLat = s.maxLat, minLng = s.minLng, maxLng = s.maxLng{geomUpd}
        WHEN NOT MATCHED THEN INSERT (docId, typeName, minLat, maxLat, minLng, maxLng{geomIns})
            VALUES (s.docId, s.typeName, s.minLat, s.maxLat, s.minLng, s.maxLng{geomVal});
        """;
    }

    public string? BuildSpatialDeleteSql(string tableName)
        => $"DELETE FROM [{tableName}_spatial] WHERE docId = @spatialDocId AND typeName = @spatialTypeName;";

    public string? BuildSpatialClearSql(string tableName)
        => $"DELETE FROM [{tableName}_spatial] WHERE typeName = @typeName;";

    public string? BuildSpatialBoundingBoxQuerySql(string tableName, string? additionalWhere) => $"""
        SELECT CAST(d.Data AS NVARCHAR(MAX)) FROM [{tableName}] d
        INNER JOIN [{tableName}_spatial] r ON r.docId = d.Id AND r.typeName = d.TypeName
        WHERE d.TypeName = @typeName
          AND r.maxLat >= @minLat AND r.minLat <= @maxLat
          AND r.maxLng >= @minLng AND r.minLng <= @maxLng
          {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
        """;

    // ── Temporal (system-time history sidecar) ──────────────────────────
    // Portable DML defaults apply (SQL Server permits self-referencing DELETE subqueries).

    public bool SupportsTemporal => true;

    public string BuildCreateHistoryTableSql(string tableName) => $"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{tableName}_history')
        CREATE TABLE [{tableName}_history] (
            Id NVARCHAR(450) NOT NULL,
            TypeName NVARCHAR(450) NOT NULL,
            Version BIGINT NOT NULL,
            ValidFrom DATETIME2 NOT NULL,
            ValidTo DATETIME2 NULL,
            Operation NVARCHAR(20) NOT NULL,
            Actor NVARCHAR(450) NULL,
            Data JSON NULL,
            CONSTRAINT PK_{tableName}_history PRIMARY KEY (Id, TypeName, Version)
        );
        """;

    public string BuildAddTenantColumnSql(string tableName)
        => $"IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{tableName}') AND name = 'TenantId') ALTER TABLE [{tableName}] ADD TenantId NVARCHAR(450) NULL;";

    public string BuildCreateTenantIndexSql(string tableName)
        => $"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_{tableName}_TenantId') CREATE INDEX IX_{tableName}_TenantId ON [{tableName}] (TenantId, TypeName);";

    public string BuildInsertSql(string tableName) => $"""
        INSERT INTO [{tableName}] (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, @data, @now, @now);
        """;

    public string BuildUpdateSql(string tableName) => $"""
        UPDATE [{tableName}]
        SET Data = @data, UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public bool SupportsJsonMergePatch => false;

    public string BuildSelectDataForUpdateSql(string tableName)
        => $"SELECT Data FROM [{tableName}] WITH (UPDLOCK, HOLDLOCK) WHERE Id = @id AND TypeName = @typeName";

    public string BuildUpsertMergeSql(string tableName)
        => throw new NotSupportedException(
            "SQL Server has no native RFC 7396 JSON Merge Patch. " +
            "DocumentStore uses the read-merge-write fallback when SupportsJsonMergePatch is false.");

    // ── Bulk import (IDocumentBackup) collision modes ──────────────────────
    // Insert mode reuses the default BuildBatchInsertSql (@data_i bound directly into the JSON column).
    // SQL Server has no ON CONFLICT — Replace / SkipExisting use a single MERGE with a VALUES row
    // constructor as the source, aliasing its columns Id, TypeName, Data.

    public string BuildBatchReplaceSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder($"MERGE INTO [{tableName}] AS tgt USING (VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, @data_{i})");
        }
        sb.Append(") AS src (Id, TypeName, Data) ON tgt.Id = src.Id AND tgt.TypeName = src.TypeName ");
        sb.Append("WHEN MATCHED THEN UPDATE SET Data = src.Data, UpdatedAt = @now ");
        sb.Append("WHEN NOT MATCHED THEN INSERT (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES (src.Id, src.TypeName, src.Data, @now, @now);");
        return sb.ToString();
    }

    public string BuildBatchSkipExistingSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder($"MERGE INTO [{tableName}] AS tgt USING (VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, @data_{i})");
        }
        sb.Append(") AS src (Id, TypeName, Data) ON tgt.Id = src.Id AND tgt.TypeName = src.TypeName ");
        sb.Append("WHEN NOT MATCHED THEN INSERT (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES (src.Id, src.TypeName, src.Data, @now, @now);");
        return sb.ToString();
    }

    // ── Native bulk copy (SqlBulkCopy) ─────────────────────────────────────
    // Only invoked for BulkWriteMode.Insert. Streams rows straight into the documents table via
    // SqlBulkCopy. Columns CreatedAt/UpdatedAt are DATETIME2 (not DATETIMEOFFSET), so the DataTable
    // carries DateTime — using timestamp.UtcDateTime to match the @now binding (DateTimeOffset.UtcNow)
    // taken by the per-row INSERT path.

    public bool SupportsBulkCopy => true;

    public async Task<long> BulkCopyInsertAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string tableName,
        string typeName,
        IReadOnlyList<RawBulkRow> rows,
        DateTimeOffset timestamp,
        CancellationToken cancellationToken)
    {
        if (connection is not SqlConnection sqlConn)
            throw new InvalidOperationException(
                $"SqlServerDatabaseProvider.BulkCopyInsertAsync requires a {nameof(SqlConnection)} but received {connection.GetType().FullName}.");

        var sqlTx = transaction as SqlTransaction;
        if (transaction != null && sqlTx == null)
            throw new InvalidOperationException(
                $"SqlServerDatabaseProvider.BulkCopyInsertAsync requires a {nameof(SqlTransaction)} but received {transaction.GetType().FullName}.");

        var ts = timestamp.UtcDateTime;

        using var table = new DataTable();
        table.Columns.Add("Id", typeof(string));
        table.Columns.Add("TypeName", typeof(string));
        table.Columns.Add("Data", typeof(string));
        table.Columns.Add("CreatedAt", typeof(DateTime));
        table.Columns.Add("UpdatedAt", typeof(DateTime));

        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            table.Rows.Add(row.Id, typeName, row.Data, ts, ts);
        }

        using var bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.Default, sqlTx)
        {
            DestinationTableName = QuoteTable(tableName),
            BatchSize = rows.Count
        };

        // Map by name so column ordering / extra columns (e.g. TenantId, FtKey) don't break the copy.
        bulkCopy.ColumnMappings.Add("Id", "Id");
        bulkCopy.ColumnMappings.Add("TypeName", "TypeName");
        bulkCopy.ColumnMappings.Add("Data", "Data");
        bulkCopy.ColumnMappings.Add("CreatedAt", "CreatedAt");
        bulkCopy.ColumnMappings.Add("UpdatedAt", "UpdatedAt");

        await bulkCopy.WriteToServerAsync(table, cancellationToken).ConfigureAwait(false);
        return rows.Count;
    }

    public string BuildSetPropertySql(string tableName) => $"""
        UPDATE [{tableName}]
        SET Data.modify(@path, @value), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildRemovePropertySql(string tableName) => $"""
        UPDATE [{tableName}]
        SET Data.modify(@path, NULL), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildMaxIdSql(string tableName)
        => $"SELECT MAX(CAST(Id AS BIGINT)) FROM [{tableName}] WHERE TypeName = @typeName;";

    // SQL Server cannot index a JSON_VALUE expression directly — it requires a (persisted)
    // computed column per indexed path. Single-column indexes use the legacy convention
    // "cc_{indexName}"; composite indexes use "cc_{indexName}_0", "cc_{indexName}_1", …
    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName) => $"""
        IF NOT EXISTS (SELECT * FROM sys.computed_columns WHERE name = 'cc_{indexName}' AND object_id = OBJECT_ID('{tableName}'))
            ALTER TABLE [{tableName}] ADD [cc_{indexName}] AS CAST(JSON_VALUE(Data, '$.{jsonPath}') AS NVARCHAR(450));
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{indexName}' AND object_id = OBJECT_ID('{tableName}'))
            CREATE INDEX {indexName} ON [{tableName}] ([cc_{indexName}]) WHERE TypeName = '{typeName}';
        """;

    public string BuildCreateJsonIndexSql(string indexName, string tableName, IReadOnlyList<string> jsonPaths, string typeName)
    {
        if (jsonPaths.Count == 1)
            return this.BuildCreateJsonIndexSql(indexName, tableName, jsonPaths[0], typeName);

        var sb = new StringBuilder();
        for (var i = 0; i < jsonPaths.Count; i++)
        {
            var col = $"cc_{indexName}_{i}";
            sb.AppendLine($"IF NOT EXISTS (SELECT * FROM sys.computed_columns WHERE name = '{col}' AND object_id = OBJECT_ID('{tableName}'))");
            sb.AppendLine($"    ALTER TABLE [{tableName}] ADD [{col}] AS CAST(JSON_VALUE(Data, '$.{jsonPaths[i]}') AS NVARCHAR(450));");
        }
        var cols = string.Join(", ", Enumerable.Range(0, jsonPaths.Count).Select(i => $"[cc_{indexName}_{i}]"));
        sb.AppendLine($"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{indexName}' AND object_id = OBJECT_ID('{tableName}'))");
        sb.AppendLine($"    CREATE INDEX {indexName} ON [{tableName}] ({cols}) WHERE TypeName = '{typeName}';");
        return sb.ToString();
    }

    // Drop must work for both single- and multi-column indexes without knowing which it was.
    // Strategy: capture the index's computed columns (filtered to our "cc_" convention) via
    // sys.index_columns, drop the index, then drop those columns. Falls back to the legacy
    // single-column name when no index row exists (idempotent drop).
    public string BuildDropIndexSql(string indexName, string tableName) => $"""
        DECLARE @tableId INT = OBJECT_ID('{tableName}');
        DECLARE @indexId INT = (SELECT index_id FROM sys.indexes WHERE name = '{indexName}' AND object_id = @tableId);
        DECLARE @cols TABLE (name NVARCHAR(128));
        IF @indexId IS NOT NULL
        BEGIN
            INSERT INTO @cols (name)
            SELECT c.name
            FROM sys.index_columns ic
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE ic.object_id = @tableId AND ic.index_id = @indexId
              AND c.is_computed = 1
              AND c.name LIKE 'cc[_]%';
            DROP INDEX [{indexName}] ON [{tableName}];
        END
        ELSE IF EXISTS (SELECT * FROM sys.computed_columns WHERE name = 'cc_{indexName}' AND object_id = @tableId)
        BEGIN
            INSERT INTO @cols (name) VALUES ('cc_{indexName}');
        END
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql = @sql + N'ALTER TABLE [{tableName}] DROP COLUMN [' + name + N'];' FROM @cols;
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
        """;

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('{tableName}') AND name LIKE @prefix;";

    public string JsonExtract(string column, string jsonPath)
        => $"JSON_VALUE({column}, '$.{jsonPath}')";

    // JSON_VALUE returns nvarchar; cast numeric/bool so arithmetic and comparisons are typed (and so a
    // PERSISTED computed column over the expression is deterministic and therefore indexable).
    public string JsonExtractTyped(string column, string jsonPath, Type clrType)
    {
        var extract = JsonExtract(column, jsonPath);
        var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (t.IsEnum) t = Enum.GetUnderlyingType(t);
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte))
            return $"CAST({extract} AS BIGINT)";
        if (t == typeof(double) || t == typeof(float))
            return $"CAST({extract} AS FLOAT)";
        if (t == typeof(decimal))
            return $"CAST({extract} AS DECIMAL(38,10))";
        if (t == typeof(bool))
            return $"CAST({extract} AS BIT)";
        return extract;
    }

    public string JsonExtractElement(string jsonPath)
        => $"JSON_VALUE(value, '$.{jsonPath}')";

    public string JsonExtractElementNumeric(string jsonPath)
        => $"CAST(JSON_VALUE(value, '$.{jsonPath}') AS FLOAT)";
    public string CastIntegerAggregate(string expression)
        => $"CAST({expression} AS BIGINT)";
    public string JsonExtractNumeric(string column, string jsonPath)
        => $"CAST(JSON_VALUE({column}, '$.{jsonPath}') AS FLOAT)";

    public string JsonArrayLength(string column, string jsonPath)
        => $"(SELECT COUNT(*) FROM OPENJSON({column}, '$.{jsonPath}'))";

    public string JsonEachFrom(string column, string jsonPath)
        => $"OPENJSON({column}, '$.{jsonPath}')";

    public string JsonObject(IEnumerable<string> keyValuePairs)
    {
        var pairs = keyValuePairs.ToList();
        var parts = new List<string>();
        for (var i = 0; i < pairs.Count; i += 2)
        {
            var key = pairs[i].Trim('\'');
            var value = pairs[i + 1];
            parts.Add($"'{key}':{value}");
        }
        return $"JSON_OBJECT({string.Join(", ", parts)})";
    }

    public string JsonTrue() => "CAST(1 AS BIT)";

    public string JsonFalse() => "CAST(0 AS BIT)";

    public string JsonNullCheck(string column, string jsonPath, bool isNull)
    {
        var extract = JsonExtract(column, jsonPath);
        return isNull ? $"{extract} IS NULL" : $"{extract} IS NOT NULL";
    }

    public string JsonEachPrimitiveValue => "value";
    public string JsonEachPrimitiveNumericValue => "CAST(value AS FLOAT)";
    public string QuoteTable(string tableName) => $"[{tableName}]";

    public string ConcatStrings(params string[] parts) => $"CONCAT({string.Join(", ", parts)})";

    public bool SupportsSoundex => true;
    public string CastInteger(string expr) => $"CAST({expr} AS BIGINT)";

    public string TranslateScalar(ScalarFn fn, IReadOnlyList<string> args, Type resultType) => fn switch
    {
        ScalarFn.Length => $"LEN({args[0]})",
        ScalarFn.IndexOf => $"(CHARINDEX({args[1]}, {args[0]}) - 1)",
        ScalarFn.Substring => args.Count == 3
            ? $"SUBSTRING({args[0]}, {CastInteger(args[1])} + 1, {CastInteger(args[2])})"
            : $"SUBSTRING({args[0]}, {CastInteger(args[1])} + 1, LEN({args[0]}))",
        ScalarFn.Ceiling => $"CEILING({args[0]})",
        ScalarFn.Round => args.Count == 2 ? $"ROUND({args[0]}, {args[1]})" : $"ROUND({args[0]}, 0)",
        // SQL Server has no EXTRACT; DATEPART over DATETIME2 (which parses ISO-8601 'T' strings).
        ScalarFn.Year => $"DATEPART(YEAR, CAST({args[0]} AS DATETIME2))",
        ScalarFn.Month => $"DATEPART(MONTH, CAST({args[0]} AS DATETIME2))",
        ScalarFn.Day => $"DATEPART(DAY, CAST({args[0]} AS DATETIME2))",
        ScalarFn.Hour => $"DATEPART(HOUR, CAST({args[0]} AS DATETIME2))",
        ScalarFn.Minute => $"DATEPART(MINUTE, CAST({args[0]} AS DATETIME2))",
        ScalarFn.Second => $"DATEPART(SECOND, CAST({args[0]} AS DATETIME2))",
        _ => Internal.Query.ScalarSqlDefaults.Translate(this, fn, args, resultType)
    };

    public string BuildJsonSetExpression() => "JSON_MODIFY(Data, @path, @value)";

    public object FormatPropertyValue(object? value) => value ?? DBNull.Value;

    public string BuildPaginationClause(int offset, int take)
        => $"OFFSET {offset} ROWS FETCH NEXT {take} ROWS ONLY";

    public string BuildLimitClause(int take)
        => $"OFFSET 0 ROWS FETCH NEXT {take} ROWS ONLY";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is SqlException sqlEx && (sqlEx.Number == 2627 || sqlEx.Number == 2601);

    // ── Native change feed: Change Tracking (+ optional Query Notifications) ──

    public bool SupportsChangeFeed => true;

    public async Task<IAsyncDisposable> SubscribeChangesAsync(
        string tableName,
        string typeName,
        Func<RawDocumentChange, CancellationToken, Task> onChange,
        CancellationToken cancellationToken)
    {
        // Provision change tracking up front so permission/configuration errors surface here.
        await this.EnsureChangeTrackingAsync(tableName, cancellationToken).ConfigureAwait(false);
        var baseline = await this.GetCurrentVersionAsync(cancellationToken).ConfigureAwait(false);
        return new ChangeFeedSubscription(cancellationToken,
            token => this.RunPollLoopAsync(tableName, typeName, baseline, onChange, token));
    }

    async Task EnsureChangeTrackingAsync(string tableName, CancellationToken ct)
    {
        await using var conn = new SqlConnection(this.connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        await using (var dbCmd = conn.CreateCommand())
        {
            dbCmd.CommandText = """
                IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
                    ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
                """;
            await dbCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using var tableCmd = conn.CreateCommand();
        tableCmd.CommandText = $"""
            IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('[{tableName}]'))
                ALTER TABLE [{tableName}] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
            """;
        await tableCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    async Task<long> GetCurrentVersionAsync(CancellationToken ct)
    {
        await using var conn = new SqlConnection(this.connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT ISNULL(CHANGE_TRACKING_CURRENT_VERSION(), 0);";
        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is null or DBNull ? 0L : Convert.ToInt64(result);
    }

    async Task RunPollLoopAsync(
        string tableName,
        string typeName,
        long baseline,
        Func<RawDocumentChange, CancellationToken, Task> onChange,
        CancellationToken token)
    {
        var last = baseline;
        var useNotify = this.changeFeed.UseQueryNotifications;
        using var signal = useNotify ? new SemaphoreSlim(0, 1) : null;
        var started = false;

        try
        {
            if (useNotify)
            {
                try { SqlDependency.Start(this.connectionString); started = true; }
                catch { useNotify = false; }
            }

            while (!token.IsCancellationRequested)
            {
                var current = await this.GetCurrentVersionAsync(token).ConfigureAwait(false);
                if (current > last)
                {
                    await this.ReadChangesAsync(tableName, typeName, last, onChange, token).ConfigureAwait(false);
                    last = current;
                }

                if (useNotify && signal != null)
                {
                    try { await this.ArmDependencyAsync(tableName, typeName, signal, token).ConfigureAwait(false); }
                    catch { /* fall back to interval wait this round */ }
                    try { await signal.WaitAsync(this.changeFeed.PollInterval, token).ConfigureAwait(false); }
                    catch (OperationCanceledException) { }
                }
                else
                {
                    await Task.Delay(this.changeFeed.PollInterval, token).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            if (started)
            {
                try { SqlDependency.Stop(this.connectionString); } catch { }
            }
        }
    }

    async Task ReadChangesAsync(
        string tableName,
        string typeName,
        long sinceVersion,
        Func<RawDocumentChange, CancellationToken, Task> onChange,
        CancellationToken token)
    {
        await using var conn = new SqlConnection(this.connectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT ct.Id, ct.SYS_CHANGE_OPERATION, CAST(d.Data AS NVARCHAR(MAX))
            FROM CHANGETABLE(CHANGES [{tableName}], @last) AS ct
            LEFT JOIN [{tableName}] AS d ON d.Id = ct.Id AND d.TypeName = ct.TypeName
            WHERE ct.TypeName = @typeName
            ORDER BY ct.SYS_CHANGE_VERSION;
            """;
        cmd.Parameters.AddWithValue("@last", sinceVersion);
        cmd.Parameters.AddWithValue("@typeName", typeName);

        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            var id = reader.GetString(0);
            var op = reader.GetString(1).Trim();
            var changeType = op switch
            {
                "I" => DocumentChangeType.Inserted,
                "U" => DocumentChangeType.Updated,
                "D" => DocumentChangeType.Removed,
                _ => (DocumentChangeType?)null
            };
            if (changeType is null)
                continue;

            var json = changeType == DocumentChangeType.Removed || reader.IsDBNull(2)
                ? null
                : reader.GetString(2);
            await onChange(new RawDocumentChange(changeType.Value, id, json), token).ConfigureAwait(false);
        }
    }

    async Task ArmDependencyAsync(string tableName, string typeName, SemaphoreSlim signal, CancellationToken token)
    {
        await using var conn = new SqlConnection(this.connectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        // Query Notifications require a schema-qualified name and explicit columns.
        cmd.CommandText = $"SELECT [Id], [TypeName] FROM dbo.[{tableName}] WHERE [TypeName] = @typeName;";
        cmd.Parameters.AddWithValue("@typeName", typeName);

        var dependency = new SqlDependency(cmd);
        dependency.OnChange += (_, _) =>
        {
            try { signal.Release(); } catch (SemaphoreFullException) { }
        };

        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false)) { } // drain to register the notification
    }

    // ── Vector (SQL Server 2025 native VECTOR) ──────────────────────────

    public bool SupportsVector => true;

    static string SanitizeForTableSuffix(string typeName)
    {
        var sb = new StringBuilder(typeName.Length);
        foreach (var c in typeName)
            sb.Append(char.IsLetterOrDigit(c) || c == '_' ? c : '_');
        return sb.ToString();
    }

    static string VecTable(string tableName, string typeName)
        => $"[{tableName}_vec_{SanitizeForTableSuffix(typeName)}]";

    static string MetricLiteral(VectorDistance metric) => metric switch
    {
        VectorDistance.Cosine => "'cosine'",
        VectorDistance.Euclidean => "'euclidean'",
        VectorDistance.DotProduct => "'dot'",
        VectorDistance.Hamming => throw new NotSupportedException("SQL Server VECTOR does not support Hamming distance."),
        _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, null)
    };

    public string BuildCreateVectorTablesSql(string tableName, string typeName, VectorMapping mapping)
    {
        var vec = VecTable(tableName, typeName);
        var bare = $"{tableName}_vec_{SanitizeForTableSuffix(typeName)}";
        var sb = new StringBuilder();
        sb.Append($"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{bare}')
            CREATE TABLE {vec} (
                docId NVARCHAR(450) NOT NULL,
                typeName NVARCHAR(450) NOT NULL,
                embedding VECTOR({mapping.Dimensions}) NOT NULL,
                CONSTRAINT PK_{bare} PRIMARY KEY (docId)
            );

            """);

        // DiskANN vector index (SQL Server 2025 preview syntax).
        // Some environments don't yet expose CREATE VECTOR INDEX — wrap so absence is tolerated.
        if (mapping.IndexKind == VectorIndexKind.DiskAnn)
        {
            sb.Append($"""
                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_{bare}' AND object_id = OBJECT_ID('{bare}'))
                BEGIN
                    BEGIN TRY
                        EXEC('CREATE VECTOR INDEX idx_{bare} ON {vec}(embedding) WITH (METRIC = {MetricLiteral(mapping.Metric)});');
                    END TRY
                    BEGIN CATCH
                        -- Vector index unsupported on this server — fall back to sequential scan.
                    END CATCH
                END;
                """);
        }

        return sb.ToString();
    }

    public string BuildVectorUpsertSql(string tableName, string typeName, VectorMapping mapping)
    {
        var vec = VecTable(tableName, typeName);
        return $"""
            MERGE {vec} AS tgt
            USING (SELECT @vecDocId AS docId) AS src ON tgt.docId = src.docId
            WHEN MATCHED THEN UPDATE SET embedding = CAST(@embedding AS VECTOR({mapping.Dimensions}))
            WHEN NOT MATCHED THEN INSERT (docId, typeName, embedding)
                VALUES (@vecDocId, @vecTypeName, CAST(@embedding AS VECTOR({mapping.Dimensions})));
            """;
    }

    public string BuildVectorDeleteSql(string tableName, string typeName)
        => $"DELETE FROM {VecTable(tableName, typeName)} WHERE docId = @vecDocId;";

    public string BuildVectorClearSql(string tableName, string typeName)
        => $"DELETE FROM {VecTable(tableName, typeName)} WHERE typeName = @vecTypeName;";

    public (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildVectorSearchSql(
        string tableName, string typeName, VectorMapping mapping,
        ReadOnlyMemory<float> query, int k, string? additionalWhere)
    {
        var vec = VecTable(tableName, typeName);
        var metric = MetricLiteral(mapping.Metric);

        var sql = $"""
            SELECT TOP ({k}) CAST(d.Data AS NVARCHAR(MAX)),
                   VECTOR_DISTANCE({metric}, v.embedding, CAST(@embedding AS VECTOR({mapping.Dimensions}))) AS score
            FROM {vec} v
            INNER JOIN [{tableName}] d ON d.Id = v.docId AND d.TypeName = @typeName
            {(additionalWhere != null ? $"WHERE {additionalWhere}" : "")}
            ORDER BY score;
            """;

        return (sql, new Dictionary<string, object> { ["@embedding"] = FormatVectorParameter(query, mapping) });
    }

    public object FormatVectorParameter(ReadOnlyMemory<float> vector, VectorMapping mapping)
    {
        // SQL Server VECTOR literal: JSON array string cast to VECTOR.
        var sb = new StringBuilder(vector.Length * 12);
        sb.Append('[');
        var span = vector.Span;
        for (var i = 0; i < span.Length; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(span[i].ToString("R", CultureInfo.InvariantCulture));
        }
        sb.Append(']');
        return sb.ToString();
    }

    // ── Full-text search (Full-Text Index + FREETEXTTABLE) ───────────────
    // Requires the SQL Server Full-Text Search feature. A full-text index needs a single-column unique
    // key, but the documents table is keyed on (Id, TypeName); we add a surrogate IDENTITY key + unique
    // index, a PERSISTED computed text column, and a full-text index over it. One FT index per table —
    // a single full-text-mapped type per table is the supported shape.

    public bool SupportsComputedColumns => true;

    public IReadOnlyList<string> BuildCreateComputedColumnSql(string tableName, string typeName, ComputedMapping mapping, string expressionSql)
    {
        var col = mapping.ColumnName;
        // SQL Server computed columns infer their type from the (already-cast) expression; PERSISTED makes
        // the deterministic JSON expression storable and indexable.
        var statements = new List<string>
        {
            $"IF NOT EXISTS (SELECT * FROM sys.computed_columns WHERE name = '{col}' AND object_id = OBJECT_ID('{tableName}')) " +
            $"ALTER TABLE [{tableName}] ADD [{col}] AS ({expressionSql}) PERSISTED;"
        };
        if (mapping.Indexed)
            statements.Add(
                $"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_{col}' AND object_id = OBJECT_ID('{tableName}')) " +
                $"CREATE INDEX idx_{col} ON [{tableName}] ([{col}]) WHERE TypeName = '{typeName}';");
        return statements;
    }

    public bool SupportsFullText => true;

    static string FtsColumn(string typeName) => "ftcc_" + SanitizeForTableSuffix(typeName);

    public IReadOnlyList<string> BuildCreateFullTextSql(string tableName, string typeName, FullTextMapping mapping)
    {
        var col = FtsColumn(typeName);
        var keyIndex = $"UX_{tableName}_ftkey";
        var catalog = $"ftcat_{tableName}";

        var concat = new StringBuilder("CONCAT(");
        for (var i = 0; i < mapping.JsonPaths.Count; i++)
        {
            if (i > 0) concat.Append(", ' ', ");
            concat.Append($"JSON_VALUE(Data, '$.{mapping.JsonPaths[i]}')");
        }
        concat.Append(')');

        return new[]
        {
            // Surrogate single-column key required by full-text indexing.
            $"""
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE name = 'FtKey' AND object_id = OBJECT_ID('{tableName}'))
                ALTER TABLE [{tableName}] ADD FtKey INT IDENTITY(1,1) NOT NULL;
            """,
            $"""
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{keyIndex}' AND object_id = OBJECT_ID('{tableName}'))
                CREATE UNIQUE INDEX {keyIndex} ON [{tableName}] (FtKey);
            """,
            // Persisted computed column holding the searchable text.
            $"""
            IF NOT EXISTS (SELECT * FROM sys.computed_columns WHERE name = '{col}' AND object_id = OBJECT_ID('{tableName}'))
                ALTER TABLE [{tableName}] ADD [{col}] AS CAST({concat} AS NVARCHAR(4000)) PERSISTED;
            """,
            $"IF NOT EXISTS (SELECT * FROM sys.fulltext_catalogs WHERE name = '{catalog}') CREATE FULLTEXT CATALOG {catalog};",
            $"""
            IF NOT EXISTS (SELECT * FROM sys.fulltext_indexes WHERE object_id = OBJECT_ID('{tableName}'))
                CREATE FULLTEXT INDEX ON [{tableName}] ([{col}]) KEY INDEX {keyIndex} ON {catalog} WITH CHANGE_TRACKING AUTO;
            """
        };
    }

    public (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildFullTextSearchSql(
        string tableName, string typeName, FullTextMapping mapping,
        string searchText, int maxResults, string? additionalWhere)
    {
        var col = FtsColumn(typeName);
        // FREETEXTTABLE is natural-language (OR + ranked); its 4th argument caps to top-N by rank.
        var sql = $"""
            SELECT CAST(d.Data AS NVARCHAR(MAX)), kt.[RANK] AS score
            FROM [{tableName}] d
            INNER JOIN FREETEXTTABLE([{tableName}], [{col}], @ftsQuery, {maxResults}) kt ON kt.[KEY] = d.FtKey
            WHERE d.TypeName = @typeName
              {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
            ORDER BY kt.[RANK] DESC;
            """;

        return (sql, new Dictionary<string, object> { ["@ftsQuery"] = searchText });
    }

    // ── Composable Lucene queries (DocumentFunctions.LuceneMatch / LuceneScore) ───────────────────────
    // CONTAINS expresses terms, phrases, AND/OR/AND NOT, grouping, prefix ("foo*") and NEAR() proximity —
    // but not fuzzy, per-term boost or field scoping. Score uses a correlated CONTAINSTABLE keyed off the
    // surrogate FtKey column the index requires.

    public FtCapabilities FullTextQueryCapabilities => FtCapabilities.Prefix | FtCapabilities.Proximity;

    public string? BuildFullTextMatchSql(string tableName, string typeName, FullTextMapping mapping, FtQuery query, Func<string, string> bindParam)
    {
        var col = FtsColumn(typeName);
        var p = bindParam(new ContainsTranslator().Write(query.Root));
        return $"CONTAINS([{col}], {p})";
    }

    public string? BuildFullTextScoreSql(string tableName, string typeName, FullTextMapping mapping, FtQuery query, Func<string, string> bindParam)
    {
        var col = FtsColumn(typeName);
        var p = bindParam(new ContainsTranslator().Write(query.Root));
        // Correlate CONTAINSTABLE back to the outer row via the surrogate FtKey; non-matching rows yield NULL.
        return $"(SELECT ct.[RANK] FROM CONTAINSTABLE([{tableName}], [{col}], {p}) ct WHERE ct.[KEY] = FtKey)";
    }

    // Renders the AST to a SQL Server CONTAINS search condition. Terms are double-quoted so user text can
    // never inject CONTAINS operators.
    sealed class ContainsTranslator : FtSqlTranslator
    {
        protected override string Term(string term) => Quote(term);
        protected override string Prefix(string term) => "\"" + Alnum(term) + "*\"";
        protected override string Phrase(IReadOnlyList<string> terms) => "\"" + string.Join(' ', terms.Select(t => t.Replace("\"", " "))) + "\"";
        protected override string Proximity(IReadOnlyList<string> terms, int slop) => $"NEAR(({string.Join(", ", terms.Select(Quote))}), {slop})";
        protected override string And(IReadOnlyList<string> parts) => string.Join(" AND ", parts);
        protected override string Or(IReadOnlyList<string> parts) => string.Join(" OR ", parts);
        protected override string AndNot(string basePart, string negated) => $"{basePart} AND NOT {negated}";

        static string Quote(string term) => "\"" + term.Replace("\"", " ") + "\"";
        static string Alnum(string term) => new string(term.Where(char.IsLetterOrDigit).ToArray());
    }
}
