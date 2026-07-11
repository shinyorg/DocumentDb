using System.Data.Common;
using System.Globalization;
using System.Text;
using DuckDB.NET.Data;
using Shiny.DocumentDb.DuckDb.Internal;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.DuckDb;

public class DuckDbDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;

    public DuckDbDatabaseProvider(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public DbConnection CreateConnection() => new DuckDbAtConnection(this.connectionString);

    // DuckDB is embedded — opening a fresh connection per op creates a separate database
    // (in-memory) or contends on the file lock. Keep one long-lived connection and serialize.
    public bool RequiresSingleConnection => true;

    public async Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "INSTALL json; LOAD json;";
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        // Spatial extension is loaded lazily in BuildCreateSpatialTablesSql — only when a geometry is mapped.
    }

    public string BuildCreateTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS "{tableName}" (
            Id VARCHAR NOT NULL,
            TypeName VARCHAR NOT NULL,
            Data JSON NOT NULL,
            CreatedAt TIMESTAMPTZ NOT NULL,
            UpdatedAt TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (Id, TypeName)
        );
        """;

    public string BuildCreateTypenameIndexSql(string tableName)
        => $"CREATE INDEX IF NOT EXISTS idx_{tableName}_typename ON \"{tableName}\" (TypeName);";

    // ── Spatial (generic envelope sidecar; bbox prune + C# refine) ──
    public bool SupportsSpatial => true;

    public string? BuildSpatialDistanceSql(string? tableName, string jsonPath, string geoJsonParam, string wktParam)
        => this.PortableSpatial ? null
            : $"ST_Distance(ST_GeomFromGeoJSON(json_extract(Data, '$.{jsonPath}')::VARCHAR), ST_GeomFromGeoJSON({geoJsonParam}))";

    /// <summary>Forces the dependency-free envelope tier (no native ST_* pushdown for DocumentFunctions-in-Where,
    /// no <c>spatial</c> extension load). Default false → native ST_* over an R-Tree-indexed column.</summary>
    public bool PortableSpatial { get; init; }

    public bool RequiresSpatialGeoJson => !this.PortableSpatial;

    // Predicates run over the sidecar's R-Tree-indexed geom column; the B-tree envelope index also remains,
    // and DuckDB's planner uses the R-Tree for the bbox filter.
    public string? BuildSpatialFilterSql(
        string tableName, string jsonPath, string predicate,
        string geoJsonParam, string wktParam, string minLatParam, string maxLatParam, string minLngParam, string maxLngParam,
        string? metersParam)
    {
        if (this.PortableSpatial)
            return null;

        var query = $"ST_GeomFromGeoJSON({geoJsonParam})";
        var pred = predicate switch
        {
            "Intersects" => $"ST_Intersects(geom, {query})",
            "Disjoint" => $"NOT ST_Intersects(geom, {query})",
            "Contains" => $"ST_Contains(geom, {query})",
            "Within" => $"ST_Within(geom, {query})",
            "Covers" => $"ST_Covers(geom, {query})",
            "CoveredBy" => $"ST_CoveredBy(geom, {query})",
            "Touches" => $"ST_Touches(geom, {query})",
            "Crosses" => $"ST_Crosses(geom, {query})",
            "Overlaps" => $"ST_Overlaps(geom, {query})",
            "Equals" => $"ST_Equals(geom, {query})",
            // WithinDistance is intentionally NOT pushed down: DuckDB's spatial extension has no geodesic
            // distance for polygons (ST_DWithin is planar degrees, and ST_*_Spheroid is point-only), so a
            // native pushdown would be silently wrong away from the equator. Returning null makes the query
            // engine throw a clear error directing callers to store.GeoWithinDistance(...), which refines with
            // an exact Haversine distance in managed code.
            _ => null
        };
        if (pred == null)
            return null;

        return $"Id IN (SELECT docId FROM \"{tableName}_spatial\" WHERE typeName = @typeName AND {pred})";
    }

    // Loads the spatial extension here (only reached when a geometry is mapped) so native DocumentFunctions
    // pushdown works; fail-loud at init if it can't INSTALL/LOAD. PortableSpatial skips it (envelope tier).
    public string? BuildCreateSpatialTablesSql(string tableName)
    {
        var geomCol = this.PortableSpatial ? "" : "geom GEOMETRY,\n            ";
        var rtree = this.PortableSpatial ? "" :
            $"\nCREATE INDEX IF NOT EXISTS rtree_{tableName}_spatial ON \"{tableName}_spatial\" USING RTREE (geom);";
        return $"""
        {(this.PortableSpatial ? "" : "INSTALL spatial; LOAD spatial;")}
        CREATE TABLE IF NOT EXISTS "{tableName}_spatial" (
            docId VARCHAR NOT NULL,
            typeName VARCHAR NOT NULL,
            minLat DOUBLE NOT NULL, maxLat DOUBLE NOT NULL,
            minLng DOUBLE NOT NULL, maxLng DOUBLE NOT NULL,
            {geomCol}PRIMARY KEY (docId, typeName)
        );
        CREATE INDEX IF NOT EXISTS idx_{tableName}_spatial ON "{tableName}_spatial" (typeName, minLat, maxLat, minLng, maxLng);{rtree}
        """;
    }

    public string? BuildSpatialUpsertSql(string tableName)
    {
        var geomIns = this.PortableSpatial ? "" : ", geom";
        var geomVal = this.PortableSpatial ? "" : ", ST_GeomFromGeoJSON(@spatialGeoJson)";
        var geomUpd = this.PortableSpatial ? "" : ", geom = EXCLUDED.geom";
        return $"""
        INSERT INTO "{tableName}_spatial" (docId, typeName, minLat, maxLat, minLng, maxLng{geomIns})
        VALUES (@spatialDocId, @spatialTypeName, @spatialMinLat, @spatialMaxLat, @spatialMinLng, @spatialMaxLng{geomVal})
        ON CONFLICT (docId, typeName) DO UPDATE SET
            minLat = EXCLUDED.minLat, maxLat = EXCLUDED.maxLat, minLng = EXCLUDED.minLng, maxLng = EXCLUDED.maxLng{geomUpd};
        """;
    }

    public string? BuildSpatialDeleteSql(string tableName)
        => $"DELETE FROM \"{tableName}_spatial\" WHERE docId = @spatialDocId AND typeName = @spatialTypeName;";

    public string? BuildSpatialClearSql(string tableName)
        => $"DELETE FROM \"{tableName}_spatial\" WHERE typeName = @typeName;";

    public string? BuildSpatialBoundingBoxQuerySql(string tableName, string? additionalWhere) => $"""
        SELECT d.Data FROM "{tableName}" d
        INNER JOIN "{tableName}_spatial" r ON r.docId = d.Id AND r.typeName = d.TypeName
        WHERE d.TypeName = @typeName
          AND r.maxLat >= @minLat AND r.minLat <= @maxLat
          AND r.maxLng >= @minLng AND r.minLng <= @maxLng
          {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
        """;

    // ── Temporal (system-time history sidecar) ──────────────────────────
    // Portable DML defaults apply; the JSON payload needs an explicit cast on insert.

    public bool SupportsTemporal => true;

    public string BuildCreateHistoryTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS "{tableName}_history" (
            Id VARCHAR NOT NULL,
            TypeName VARCHAR NOT NULL,
            Version BIGINT NOT NULL,
            ValidFrom TIMESTAMPTZ NOT NULL,
            ValidTo TIMESTAMPTZ NULL,
            Operation VARCHAR NOT NULL,
            Actor VARCHAR NULL,
            Data JSON NULL,
            TenantId VARCHAR NULL,
            PRIMARY KEY (Id, TypeName, Version)
        );
        """;

    public string BuildHistoryInsertSql(string tableName)
        => $"INSERT INTO \"{tableName}_history\" (Id, TypeName, Version, ValidFrom, ValidTo, Operation, Actor, Data) " +
           "SELECT @id, @typeName, COALESCE(MAX(Version), 0) + 1, @validFrom, NULL, @operation, @actor, CAST(@data AS JSON) " +
           $"FROM \"{tableName}_history\" WHERE Id = @id AND TypeName = @typeName";

    public string BuildInsertSql(string tableName) => $"""
        INSERT INTO "{tableName}" (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, CAST(@data AS JSON), @now, @now);
        """;

    public string BuildUpdateSql(string tableName) => $"""
        UPDATE "{tableName}"
        SET Data = CAST(@data AS JSON), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    // DuckDB exposes json_merge_patch (RFC 7396) since v0.10 — use the SupportsJsonMergePatch fast path.
    public bool SupportsJsonMergePatch => true;

    public string BuildUpsertMergeSql(string tableName) => $"""
        INSERT INTO "{tableName}" (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, CAST(@data AS JSON), @now, @now)
        ON CONFLICT (Id, TypeName) DO UPDATE SET
            Data = json_merge_patch("{tableName}".Data, EXCLUDED.Data),
            UpdatedAt = EXCLUDED.UpdatedAt;
        """;

    public bool SupportsBatchUpsert => true;

    // Multi-row deep-merge upsert in one round-trip. EXCLUDED.Data carries each conflicting row's
    // patch, so a single ON CONFLICT clause merges every row via json_merge_patch.
    public string BuildBatchUpsertSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO \"{tableName}\" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, CAST(@data_{i} AS JSON), @now, @now)");
        }
        sb.Append($" ON CONFLICT (Id, TypeName) DO UPDATE SET Data = json_merge_patch(\"{tableName}\".Data, EXCLUDED.Data), UpdatedAt = EXCLUDED.UpdatedAt;");
        return sb.ToString();
    }

    // DuckDB has no json_set/json_remove. We construct an RFC 7396 merge patch from the path by
    // folding the reversed path parts with string concatenation: ["city","shippingAddress"] +
    // value '"NY"' folds into '{"city":"NY"}' then '{"shippingAddress":{"city":"NY"}}'. The
    // accumulator is a VARCHAR (raw JSON text) because json_object with a dynamic VARCHAR key
    // inside a lambda is unreliable; once folded we CAST the string to JSON and merge.
    // FormatPropertyValue pre-quotes string values (e.g. "NY" → "\"NY\""), so @value is valid JSON.
    public string BuildSetPropertySql(string tableName) => $$"""
        UPDATE "{{tableName}}"
        SET Data = json_merge_patch(Data, CAST(
            list_reduce(
                list_reverse(string_split(REPLACE(@path, '$.', ''), '.')),
                (acc, part) -> '{"' || part || '":' || acc || '}',
                @value
            ) AS JSON
        )), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    // RFC 7396 semantics: setting a field to JSON null removes it from the document.
    public string BuildRemovePropertySql(string tableName) => $$"""
        UPDATE "{{tableName}}"
        SET Data = json_merge_patch(Data, CAST(
            list_reduce(
                list_reverse(string_split(REPLACE(@path, '$.', ''), '.')),
                (acc, part) -> '{"' || part || '":' || acc || '}',
                'null'
            ) AS JSON
        )), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildMaxIdSql(string tableName)
        => $"SELECT MAX(CAST(Id AS BIGINT)) FROM \"{tableName}\" WHERE TypeName = @typeName;";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName)
        => $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{tableName}\" (json_extract_string(Data, '$.{jsonPath}'));";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, IReadOnlyList<string> jsonPaths, string typeName)
    {
        if (jsonPaths.Count == 1)
            return this.BuildCreateJsonIndexSql(indexName, tableName, jsonPaths[0], typeName);
        var exprs = string.Join(", ", jsonPaths.Select(p => $"json_extract_string(Data, '$.{p}')"));
        return $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{tableName}\" ({exprs});";
    }

    public string BuildDropIndexSql(string indexName, string tableName)
        => $"DROP INDEX IF EXISTS {indexName};";

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT index_name FROM duckdb_indexes() WHERE table_name = '{tableName}' AND index_name LIKE @prefix;";

    public string JsonExtract(string column, string jsonPath)
        => $"json_extract_string({column}, '$.{jsonPath}')";

    public string JsonExtractTyped(string column, string jsonPath, Type clrType)
    {
        var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (t.IsEnum) t = Enum.GetUnderlyingType(t); // enums are stored as their numeric value
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte))
            return $"CAST(json_extract_string({column}, '$.{jsonPath}') AS BIGINT)";
        if (t == typeof(double) || t == typeof(float))
            return $"CAST(json_extract_string({column}, '$.{jsonPath}') AS DOUBLE)";
        if (t == typeof(decimal))
            return $"CAST(json_extract_string({column}, '$.{jsonPath}') AS DECIMAL(38,10))";
        if (t == typeof(bool))
            return $"CAST(json_extract_string({column}, '$.{jsonPath}') AS BOOLEAN)";
        return $"json_extract_string({column}, '$.{jsonPath}')";
    }

    public string JsonExtractElement(string jsonPath)
        => $"json_extract_string(value, '$.{jsonPath}')";

    public string JsonExtractElementTyped(string jsonPath, Type clrType)
    {
        var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (t.IsEnum) t = Enum.GetUnderlyingType(t); // enums are stored as their numeric value
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte))
            return $"CAST(json_extract_string(value, '$.{jsonPath}') AS BIGINT)";
        if (t == typeof(double) || t == typeof(float))
            return $"CAST(json_extract_string(value, '$.{jsonPath}') AS DOUBLE)";
        if (t == typeof(decimal))
            return $"CAST(json_extract_string(value, '$.{jsonPath}') AS DECIMAL(38,10))";
        if (t == typeof(bool))
            return $"CAST(json_extract_string(value, '$.{jsonPath}') AS BOOLEAN)";
        return $"json_extract_string(value, '$.{jsonPath}')";
    }

    public string JsonExtractElementNumeric(string jsonPath)
        => $"CAST(json_extract_string(value, '$.{jsonPath}') AS DOUBLE)";

    public string CastIntegerAggregate(string expression)
        => $"CAST({expression} AS BIGINT)";

    public string JsonExtractNumeric(string column, string jsonPath)
        => $"CAST(json_extract_string({column}, '$.{jsonPath}') AS DOUBLE)";

    public string JsonArrayLength(string column, string jsonPath)
        => $"json_array_length({column}, '$.{jsonPath}')";

    public string JsonEachFrom(string column, string jsonPath)
        => $"(SELECT unnest(CAST(json_extract({column}, '$.{jsonPath}') AS JSON[])) AS value) je";

    public string JsonObject(IEnumerable<string> keyValuePairs)
        => $"json_object({string.Join(", ", keyValuePairs)})";

    public string JsonTrue() => "CAST('true' AS JSON)";

    public string JsonFalse() => "CAST('false' AS JSON)";

    public string JsonNullCheck(string column, string jsonPath, bool isNull)
    {
        var raw = $"json_extract({column}, '$.{jsonPath}')";
        // DuckDB returns SQL NULL when the path is missing, and JSON 'null' literal when the field is explicitly null.
        return isNull
            ? $"({raw} IS NULL OR json_type({raw}) = 'NULL')"
            : $"({raw} IS NOT NULL AND json_type({raw}) <> 'NULL')";
    }

    public string JsonEachPrimitiveValue => "json_extract_string(value, '$')";
    public string JsonEachPrimitiveNumericValue => "CAST(json_extract_string(value, '$') AS DOUBLE)";

    public string QuoteTable(string tableName) => $"\"{tableName.Replace("\"", "\"\"")}\"";

    public string ConcatStrings(params string[] parts) => string.Join(" || ", parts);

    public string BuildJsonSetExpression() => """
        json_merge_patch(Data, CAST(list_reduce(
            list_reverse(string_split(REPLACE(@path, '$.', ''), '.')),
            (acc, part) -> '{"' || part || '":' || acc || '}',
            @value
        ) AS JSON))
        """;

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"LIMIT {take} OFFSET {offset}";

    public bool IsDuplicateKeyException(Exception ex)
    {
        // DuckDB.NET throws DuckDBException; constraint violations carry "Constraint Error" or
        // "violates primary key constraint" in the message.
        var msg = ex.Message;
        return msg.Contains("Duplicate key", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("PRIMARY KEY", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("UNIQUE constraint", StringComparison.OrdinalIgnoreCase);
    }

    public string BuildBatchInsertSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO \"{tableName}\" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, CAST(@data_{i} AS JSON), @now, @now)");
        }
        sb.Append(';');
        return sb.ToString();
    }

    // Casts the bound JSON parameter to JSON — used by the interface-default backup Insert SQL so the
    // timestamp-preserving restore path gets the same cast as BuildBatchInsertSql above.
    public string JsonInsertValueExpr(string dataParam) => $"CAST({dataParam} AS JSON)";

    // ── Bulk import collision modes ──────────────────────────────────────
    // Same JSON cast on @data_i as BuildBatchInsertSql; DuckDB supports ANSI ON CONFLICT.

    public string BuildBatchReplaceSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO \"{tableName}\" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, CAST(@data_{i} AS JSON), @now, @now)");
        }
        sb.Append(" ON CONFLICT (Id, TypeName) DO UPDATE SET Data = EXCLUDED.Data, UpdatedAt = @now;");
        return sb.ToString();
    }

    public string BuildBatchSkipExistingSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO \"{tableName}\" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, CAST(@data_{i} AS JSON), @now, @now)");
        }
        sb.Append(" ON CONFLICT (Id, TypeName) DO NOTHING;");
        return sb.ToString();
    }

    // ── Native bulk copy (DuckDB appender) ───────────────────────────────
    // For BulkWriteMode.Insert the engine streams a type-homogeneous chunk through DuckDB's native
    // appender — far faster than a multi-row VALUES statement. The appender writes column-by-column in
    // declaration order: Id (VARCHAR), TypeName (VARCHAR), Data (JSON — accepts a raw JSON string),
    // CreatedAt/UpdatedAt (TIMESTAMPTZ — bound from DateTimeOffset).

    public bool SupportsBulkCopy => true;

    // The appender is positional and must supply a value for every column in declaration order, so it can't
    // tolerate the trailing nullable TenantId column that shared-table multi-tenancy adds. Report false so the
    // store falls back to the multi-row INSERT (which omits TenantId → NULL) when tenancy is enabled, matching
    // the PostgreSQL/SQL Server bulk paths rather than throwing a column-count mismatch.
    public bool SupportsBulkCopyWithTenant => false;

    public Task<long> BulkCopyInsertAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string tableName,
        string typeName,
        IReadOnlyList<RawBulkRow> rows,
        DateTimeOffset timestamp,
        CancellationToken cancellationToken)
    {
        // The store hands us the @param-rewriting wrapper; the appender lives on the native connection.
        var native = connection switch
        {
            DuckDbAtConnection wrapper => wrapper.Inner,
            DuckDBConnection direct => direct,
            _ => throw new InvalidOperationException(
                $"DuckDB bulk copy requires a DuckDBConnection but got '{connection.GetType().FullName}'.")
        };

        // The appender is synchronous and operates directly on the connection. DuckDB has one
        // transaction per connection, so the appender automatically participates in whatever
        // transaction the caller began on this connection; Close() flushes the rows into that
        // active transaction and the caller's Commit makes them durable. We don't touch `transaction`
        // beyond relying on it being active on `native`.
        cancellationToken.ThrowIfCancellationRequested();

        // TIMESTAMPTZ binds from DateTimeOffset (the appender has a dedicated overload); normalize to UTC.
        var ts = timestamp.ToUniversalTime();
        var appender = native.CreateAppender(tableName);
        try
        {
            foreach (var row in rows)
            {
                var r = appender.CreateRow();
                r.AppendValue(row.Id);        // Id       VARCHAR
                r.AppendValue(typeName);      // TypeName VARCHAR
                r.AppendValue(row.Data);      // Data     JSON  (raw JSON string accepted as VARCHAR-backed logical type)
                r.AppendValue(ts);            // CreatedAt TIMESTAMPTZ (DateTimeOffset)
                r.AppendValue(ts);            // UpdatedAt TIMESTAMPTZ (DateTimeOffset)
                r.EndRow();
            }
            appender.Close();
        }
        finally
        {
            appender.Dispose();
        }

        return Task.FromResult((long)rows.Count);
    }

    // ── Vector (DuckDB vss extension) ────────────────────────────────────

    public bool SupportsVector => true;

    public async Task LoadVectorExtensionAsync(DbConnection connection, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "INSTALL vss; LOAD vss;";
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        // HNSW persistence on file-backed DBs requires the experimental flag.
        await using var flagCmd = connection.CreateCommand();
        flagCmd.CommandText = "SET hnsw_enable_experimental_persistence = true;";
        try { await flagCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false); }
        catch { /* in-memory DB — flag not required */ }
    }

    static string SanitizeForTableSuffix(string typeName)
    {
        var sb = new StringBuilder(typeName.Length);
        foreach (var c in typeName)
            sb.Append(char.IsLetterOrDigit(c) || c == '_' ? c : '_');
        return sb.ToString();
    }

    static string VecTable(string tableName, string typeName)
        => $"\"{tableName}_vec_{SanitizeForTableSuffix(typeName)}\"";

    static string MetricKey(VectorDistance metric) => metric switch
    {
        VectorDistance.Cosine => "cosine",
        VectorDistance.Euclidean => "l2sq",
        VectorDistance.DotProduct => "ip",
        VectorDistance.Hamming => throw new NotSupportedException("DuckDB vss does not support Hamming distance."),
        _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, null)
    };

    static string DistanceCall(VectorDistance metric, string col, string lit) => metric switch
    {
        VectorDistance.Cosine => $"array_cosine_distance({col}, {lit})",
        VectorDistance.Euclidean => $"array_distance({col}, {lit})",
        VectorDistance.DotProduct => $"-array_inner_product({col}, {lit})", // negate so ORDER BY ASC = "nearest"
        _ => throw new NotSupportedException()
    };

    public string BuildCreateVectorTablesSql(string tableName, string typeName, VectorMapping mapping)
    {
        var vec = VecTable(tableName, typeName);
        var bare = $"{tableName}_vec_{SanitizeForTableSuffix(typeName)}";
        var sb = new StringBuilder();
        sb.Append($"""
            CREATE TABLE IF NOT EXISTS {vec} (
                docId VARCHAR PRIMARY KEY,
                typeName VARCHAR NOT NULL,
                embedding FLOAT[{mapping.Dimensions}] NOT NULL
            );

            """);

        if (mapping.IndexKind == VectorIndexKind.Hnsw)
        {
            sb.Append($"CREATE INDEX IF NOT EXISTS idx_{bare} ON {vec} USING HNSW (embedding) WITH (metric = '{MetricKey(mapping.Metric)}');");
        }

        return sb.ToString();
    }

    public string BuildVectorUpsertSql(string tableName, string typeName, VectorMapping mapping)
    {
        var vec = VecTable(tableName, typeName);
        return $"""
            INSERT INTO {vec} (docId, typeName, embedding)
            VALUES (@vecDocId, @vecTypeName, CAST(@embedding AS FLOAT[{mapping.Dimensions}]))
            ON CONFLICT (docId) DO UPDATE SET embedding = EXCLUDED.embedding;
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
        var lit = $"CAST(@embedding AS FLOAT[{mapping.Dimensions}])";
        var dist = DistanceCall(mapping.Metric, "v.embedding", lit);

        var sql = $"""
            SELECT d.Data, {dist} AS score
            FROM {vec} v
            INNER JOIN "{tableName}" d ON d.Id = v.docId AND d.TypeName = @typeName
            {(additionalWhere != null ? $"WHERE {additionalWhere}" : "")}
            ORDER BY {dist}
            LIMIT {k};
            """;

        return (sql, new Dictionary<string, object> { ["@embedding"] = FormatVectorParameter(query, mapping) });
    }

    public object FormatVectorParameter(ReadOnlyMemory<float> vector, VectorMapping mapping)
    {
        // DuckDB accepts the JSON-array literal cast to FLOAT[N].
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

    // DuckDB cannot add a GENERATED column via ALTER TABLE (generated columns are CREATE-TABLE-only), so
    // computed properties use alias mode here — the definition is inlined into each query. As a columnar
    // analytic engine, full scans are its model, so the materialized-column index buys little anyway.

    // ── Full-text search (fts extension, BM25) ───────────────────────────
    // DuckDB's fts index is a one-shot snapshot built by PRAGMA create_fts_index — there are no
    // triggers, so the index is rebuilt from current data immediately before every query.

    public bool SupportsFullText => true;
    public bool FullTextIndexRequiresRebuild => true;

    static string FtsSourceTable(string tableName, string typeName)
        => $"{tableName}_ftssrc_{SanitizeForTableSuffix(typeName)}";

    static string DuckStemmer(FullTextLanguage language) => language switch
    {
        FullTextLanguage.Simple => "none",
        FullTextLanguage.English => "english",
        FullTextLanguage.Spanish => "spanish",
        FullTextLanguage.French => "french",
        FullTextLanguage.German => "german",
        FullTextLanguage.Italian => "italian",
        FullTextLanguage.Portuguese => "portuguese",
        FullTextLanguage.Dutch => "dutch",
        FullTextLanguage.Russian => "russian",
        _ => "english"
    };

    public IReadOnlyList<string> BuildCreateFullTextSql(string tableName, string typeName, FullTextMapping mapping)
    {
        var src = FtsSourceTable(tableName, typeName);

        var text = new StringBuilder("concat_ws(' '");
        foreach (var path in mapping.JsonPaths)
            text.Append($", json_extract_string(Data, '$.{path}')");
        text.Append(')');

        var escapedType = typeName.Replace("'", "''");
        return new[]
        {
            "INSTALL fts; LOAD fts;",
            $"CREATE OR REPLACE TABLE \"{src}\" AS SELECT Id, {text} AS text FROM \"{tableName}\" WHERE TypeName = '{escapedType}';",
            $"PRAGMA create_fts_index('{src}', 'Id', 'text', stemmer='{DuckStemmer(mapping.Language)}', overwrite=1);"
        };
    }

    public (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildFullTextSearchSql(
        string tableName, string typeName, FullTextMapping mapping,
        string searchText, int maxResults, string? additionalWhere)
    {
        var src = FtsSourceTable(tableName, typeName);
        var sql = $"""
            SELECT d.Data, s.score FROM (
                SELECT Id, fts_main_{src}.match_bm25(Id, @ftsQuery, conjunctive := 0) AS score FROM "{src}"
            ) s
            INNER JOIN "{tableName}" d ON d.Id = s.Id AND d.TypeName = @typeName
            WHERE s.score IS NOT NULL
              {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
            ORDER BY s.score DESC
            LIMIT {maxResults};
            """;

        return (sql, new Dictionary<string, object> { ["@ftsQuery"] = searchText });
    }
}
