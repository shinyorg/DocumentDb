using System.Data.Common;
using MySqlConnector;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.FullText;

namespace Shiny.DocumentDb.MySql;

public class MySqlDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;

    public MySqlDatabaseProvider(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public DbConnection CreateConnection() => new MySqlConnection(this.connectionString);

    public async Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SET SESSION sql_mode = REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', '');";
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public string BuildCreateTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS `{tableName}` (
            Id VARCHAR(255) NOT NULL,
            TypeName VARCHAR(255) NOT NULL,
            Data JSON NOT NULL,
            CreatedAt DATETIME(6) NOT NULL,
            UpdatedAt DATETIME(6) NOT NULL,
            PRIMARY KEY (Id, TypeName)
        );
        """;

    public string BuildCreateTypenameIndexSql(string tableName)
        => $"CREATE INDEX idx_{tableName}_typename ON `{tableName}` (TypeName);";

    // ── Spatial (generic envelope sidecar; bbox prune + C# refine, no ST_*) ──
    public bool SupportsSpatial => true;

    public string? BuildSpatialDistanceSql(string? tableName, string jsonPath, string geoJsonParam, string wktParam)
        => this.PortableSpatialMode ? null
            : $"ST_Distance(ST_GeomFromGeoJSON(JSON_EXTRACT(Data, '$.{jsonPath}'), 1, 4326), ST_GeomFromGeoJSON({geoJsonParam}, 1, 4326))";

    /// <summary>Forces the dependency-free envelope tier (dedicated Geo* methods only; no native ST_* pushdown
    /// for DocumentFunctions-in-Where). Default false → native ST_* over a SPATIAL-indexed column.</summary>
    public bool PortableSpatial { get; init; }

    /// <summary>
    /// The effective spatial tier consulted by every spatial builder. Defaults to <see cref="PortableSpatial"/>
    /// so the public opt-in flag still drives MySQL. Derived providers whose engine lacks MySQL's SRID-aware
    /// native spatial (e.g. MariaDB, whose <c>ST_Distance</c> is not metric for SRID 4326 and whose geometry
    /// column syntax differs) override this to force the portable envelope tier.
    /// </summary>
    protected virtual bool PortableSpatialMode => this.PortableSpatial;

    public bool RequiresSpatialGeoJson => !this.PortableSpatialMode;

    // Predicates run over the sidecar's SPATIAL-indexed geom column (SRID 4326), so MySQL uses the real 2-D
    // spatial index for ST_Intersects/Contains/Within.
    public string? BuildSpatialFilterSql(
        string tableName, string jsonPath, string predicate,
        string geoJsonParam, string wktParam, string minLatParam, string maxLatParam, string minLngParam, string maxLngParam,
        string? metersParam)
    {
        if (this.PortableSpatialMode)
            return null;

        var query = $"ST_GeomFromGeoJSON({geoJsonParam}, 1, 4326)";
        var pred = predicate switch
        {
            "Intersects" => $"ST_Intersects(geom, {query})",
            "Disjoint" => $"ST_Disjoint(geom, {query})",
            "Contains" => $"ST_Contains(geom, {query})",
            "Within" => $"ST_Within(geom, {query})",
            "Covers" => $"ST_Contains(geom, {query})",     // MySQL has no ST_Covers — boundary-inclusive approx
            "CoveredBy" => $"ST_Within(geom, {query})",
            "Touches" => $"ST_Touches(geom, {query})",
            "Crosses" => $"ST_Crosses(geom, {query})",
            "Overlaps" => $"ST_Overlaps(geom, {query})",
            "Equals" => $"ST_Equals(geom, {query})",
            "WithinDistance" => $"ST_Distance(geom, {query}) <= {metersParam}",  // 4326 → meters
            _ => null
        };
        if (pred == null)
            return null;

        return $"Id IN (SELECT docId FROM `{tableName}_spatial` WHERE typeName = @typeName AND {pred})";
    }

    // Index inlined into CREATE TABLE (MySQL has no CREATE INDEX IF NOT EXISTS). The SPATIAL index requires a
    // NOT NULL, SRID-restricted geometry column.
    public string? BuildCreateSpatialTablesSql(string tableName)
    {
        var geomCol = this.PortableSpatialMode ? "" : "geom GEOMETRY NOT NULL SRID 4326,\n            ";
        var spatialIdx = this.PortableSpatialMode ? "" : $",\n            SPATIAL INDEX sidx_{tableName}_spatial (geom)";
        return $"""
        CREATE TABLE IF NOT EXISTS `{tableName}_spatial` (
            docId VARCHAR(255) NOT NULL,
            typeName VARCHAR(255) NOT NULL,
            minLat DOUBLE NOT NULL, maxLat DOUBLE NOT NULL,
            minLng DOUBLE NOT NULL, maxLng DOUBLE NOT NULL,
            {geomCol}PRIMARY KEY (docId, typeName),
            INDEX idx_{tableName}_spatial (typeName, minLat, maxLat, minLng, maxLng){spatialIdx}
        );
        """;
    }

    public string? BuildSpatialUpsertSql(string tableName)
    {
        var geomIns = this.PortableSpatialMode ? "" : ", geom";
        var geomVal = this.PortableSpatialMode ? "" : ", ST_GeomFromGeoJSON(@spatialGeoJson, 1, 4326)";
        var geomUpd = this.PortableSpatialMode ? "" : ", geom = VALUES(geom)";
        return $"""
        INSERT INTO `{tableName}_spatial` (docId, typeName, minLat, maxLat, minLng, maxLng{geomIns})
        VALUES (@spatialDocId, @spatialTypeName, @spatialMinLat, @spatialMaxLat, @spatialMinLng, @spatialMaxLng{geomVal})
        ON DUPLICATE KEY UPDATE
            minLat = VALUES(minLat), maxLat = VALUES(maxLat), minLng = VALUES(minLng), maxLng = VALUES(maxLng){geomUpd};
        """;
    }

    public string? BuildSpatialDeleteSql(string tableName)
        => $"DELETE FROM `{tableName}_spatial` WHERE docId = @spatialDocId AND typeName = @spatialTypeName;";

    public string? BuildSpatialClearSql(string tableName)
        => $"DELETE FROM `{tableName}_spatial` WHERE typeName = @typeName;";

    public string? BuildSpatialBoundingBoxQuerySql(string tableName, string? additionalWhere) => $"""
        SELECT d.Data FROM `{tableName}` d
        INNER JOIN `{tableName}_spatial` r ON r.docId = d.Id AND r.typeName = d.TypeName
        WHERE d.TypeName = @typeName
          AND r.maxLat >= @minLat AND r.minLat <= @maxLat
          AND r.maxLng >= @minLng AND r.minLng <= @maxLng
          {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
        """;

    // ── Temporal (system-time history sidecar) ──────────────────────────
    // Portable DML defaults apply, except count-pruning: MySQL forbids referencing the delete
    // target inside a subquery, so the cutoff subquery is wrapped in a derived table.

    public bool SupportsTemporal => true;

    public string BuildCreateHistoryTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS `{tableName}_history` (
            Id VARCHAR(255) NOT NULL,
            TypeName VARCHAR(255) NOT NULL,
            Version BIGINT NOT NULL,
            ValidFrom DATETIME(6) NOT NULL,
            ValidTo DATETIME(6) NULL,
            Operation VARCHAR(20) NOT NULL,
            Actor VARCHAR(255) NULL,
            Data JSON NULL,
            PRIMARY KEY (Id, TypeName, Version)
        );
        """;

    public string BuildHistoryPruneByCountSql(string tableName)
        => $"DELETE FROM `{tableName}_history` WHERE Id = @id AND TypeName = @typeName AND Version <= " +
           $"(SELECT m - @keep FROM (SELECT MAX(Version) AS m FROM `{tableName}_history` WHERE Id = @id AND TypeName = @typeName) t)";

    public string BuildInsertSql(string tableName) => $"""
        INSERT INTO `{tableName}` (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, @data, @now, @now);
        """;

    // Row-locking SELECT for the read-modify-write merge/replace fallback. MySQL pools connections (no
    // process-wide serialization), so without FOR UPDATE two concurrent Update(patch)/Upsert(patchIfUpdate:
    // false) calls on the same row would each read a stale snapshot and lose the other's write.
    public string BuildSelectDataForUpdateSql(string tableName)
        => $"SELECT Data FROM `{tableName}` WHERE Id = @id AND TypeName = @typeName FOR UPDATE";

    public string BuildUpdateSql(string tableName) => $"""
        UPDATE `{tableName}`
        SET Data = @data, UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildUpsertMergeSql(string tableName) => $"""
        INSERT INTO `{tableName}` (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, @data, @now, @now)
        ON DUPLICATE KEY UPDATE
            Data = JSON_MERGE_PATCH(Data, VALUES(Data)),
            UpdatedAt = VALUES(UpdatedAt);
        """;

    // ── Bulk import (IDocumentBackup) collision modes ──────────────────────
    // Insert mode reuses the default BuildBatchInsertSql (backticked, @data_i bound directly into the
    // JSON column). Replace / SkipExisting use MySQL's native upsert dialect instead of ANSI ON CONFLICT.

    public string BuildBatchReplaceSql(string tableName, int batchSize)
    {
        var sb = new System.Text.StringBuilder($"INSERT INTO `{tableName}` (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, @data_{i}, @now, @now)");
        }
        sb.Append(" ON DUPLICATE KEY UPDATE Data = VALUES(Data), UpdatedAt = @now;");
        return sb.ToString();
    }

    public string BuildBatchSkipExistingSql(string tableName, int batchSize)
    {
        var sb = new System.Text.StringBuilder($"INSERT IGNORE INTO `{tableName}` (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, @data_{i}, @now, @now)");
        }
        sb.Append(';');
        return sb.ToString();
    }

    // virtual: MariaDB rejects CAST(... AS JSON) and overrides these to use JSON_COMPACT instead.
    public virtual string BuildSetPropertySql(string tableName) => $"""
        UPDATE `{tableName}`
        SET Data = JSON_SET(Data, @path, CAST(@value AS JSON)), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildRemovePropertySql(string tableName) => $"""
        UPDATE `{tableName}`
        SET Data = JSON_REMOVE(Data, @path), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildMaxIdSql(string tableName)
        => $"SELECT MAX(CAST(Id AS SIGNED)) FROM `{tableName}` WHERE TypeName = @typeName;";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName)
        => $"CREATE INDEX {indexName} ON `{tableName}` ((CAST(JSON_EXTRACT(Data, '$.{jsonPath}') AS CHAR(255))));";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, IReadOnlyList<string> jsonPaths, string typeName)
    {
        if (jsonPaths.Count == 1)
            return this.BuildCreateJsonIndexSql(indexName, tableName, jsonPaths[0], typeName);
        var exprs = string.Join(", ", jsonPaths.Select(p => $"(CAST(JSON_EXTRACT(Data, '$.{p}') AS CHAR(255)))"));
        return $"CREATE INDEX {indexName} ON `{tableName}` ({exprs});";
    }

    public string BuildDropIndexSql(string indexName, string tableName)
        => $"DROP INDEX {indexName} ON `{tableName}`;";

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = '{tableName}' AND INDEX_NAME LIKE @prefix GROUP BY INDEX_NAME;";

    // MySQL's information_schema.tables is server-wide — it lists every schema's tables (including
    // information_schema/performance_schema system tables). Scope to the current database so ClearAll
    // only wipes this store's tables, not unrelated/system tables (e.g. information_schema.processlist).
    public string BuildListTablesSql()
        => "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = DATABASE();";

    public string JsonExtract(string column, string jsonPath)
        => $"NULLIF(JSON_UNQUOTE(JSON_EXTRACT({column}, '$.{jsonPath}')), 'null')";

    public string JsonExtractElement(string jsonPath)
        => $"NULLIF(JSON_UNQUOTE(JSON_EXTRACT(value, '$.{jsonPath}')), 'null')";

    public string JsonExtractElementNumeric(string jsonPath)
        => $"(JSON_UNQUOTE(JSON_EXTRACT(value, '$.{jsonPath}')) + 0)";
    public string CastIntegerAggregate(string expression)
        => $"CAST({expression} AS SIGNED)";
    public string JsonExtractNumeric(string column, string jsonPath)
        => $"(JSON_UNQUOTE(JSON_EXTRACT({column}, '$.{jsonPath}')) + 0)";

    // Typed extraction so grouped SUM/AVG of a decimal keeps its scale and MIN/MAX of a date/string compares
    // by that type rather than a lossy numeric cast. Falls back to the plain (unquoted text) extract for
    // string/date/guid, which orders correctly (ISO-8601 dates sort lexically).
    public string JsonExtractTyped(string column, string jsonPath, Type clrType)
    {
        var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (t.IsEnum) t = Enum.GetUnderlyingType(t);
        var raw = $"JSON_UNQUOTE(JSON_EXTRACT({column}, '$.{jsonPath}'))";
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte))
            return $"CAST({raw} AS SIGNED)";
        if (t == typeof(double) || t == typeof(float))
            return $"({raw} + 0)";
        if (t == typeof(decimal))
            return $"CAST({raw} AS DECIMAL(38,10))";
        return JsonExtract(column, jsonPath);
    }

    public string JsonArrayLength(string column, string jsonPath)
        => $"JSON_LENGTH({column}, '$.{jsonPath}')";

    // virtual: MariaDB has no JSON_TABLE and overrides this to fail loud (it can't unnest a correlated array).
    public virtual string JsonEachFrom(string column, string jsonPath)
        => $"JSON_TABLE({column}, '$.{jsonPath}[*]' COLUMNS(value JSON PATH '$')) AS jt";

    public string JsonObject(IEnumerable<string> keyValuePairs)
        => $"JSON_OBJECT({string.Join(", ", keyValuePairs)})";

    public virtual string JsonTrue() => "CAST('true' AS JSON)";

    public virtual string JsonFalse() => "CAST('false' AS JSON)";

    public string JsonNullCheck(string column, string jsonPath, bool isNull)
    {
        var raw = $"JSON_EXTRACT({column}, '$.{jsonPath}')";
        return isNull
            ? $"({raw} IS NULL OR JSON_TYPE({raw}) = 'NULL')"
            : $"({raw} IS NOT NULL AND JSON_TYPE({raw}) <> 'NULL')";
    }

    public string JsonEachPrimitiveValue => "JSON_UNQUOTE(value)";
    public string JsonEachPrimitiveNumericValue => "(JSON_UNQUOTE(value) + 0)";
    public string QuoteTable(string tableName) => $"`{tableName}`";

    public string ConcatStrings(params string[] parts) => $"CONCAT({string.Join(", ", parts)})";

    public bool SupportsSoundex => true;
    public string CastInteger(string expr) => $"CAST({expr} AS SIGNED)";

    public string TranslateScalar(ScalarFn fn, IReadOnlyList<string> args, Type resultType) => fn switch
    {
        ScalarFn.Length => $"CHAR_LENGTH({args[0]})",
        ScalarFn.IndexOf => $"(LOCATE({args[1]}, {args[0]}) - 1)",
        // MySQL DATETIME wants a space separator, not the stored ISO-8601 'T'.
        ScalarFn.Year => $"EXTRACT(YEAR FROM CAST(REPLACE({args[0]}, 'T', ' ') AS DATETIME))",
        ScalarFn.Month => $"EXTRACT(MONTH FROM CAST(REPLACE({args[0]}, 'T', ' ') AS DATETIME))",
        ScalarFn.Day => $"EXTRACT(DAY FROM CAST(REPLACE({args[0]}, 'T', ' ') AS DATETIME))",
        ScalarFn.Hour => $"EXTRACT(HOUR FROM CAST(REPLACE({args[0]}, 'T', ' ') AS DATETIME))",
        ScalarFn.Minute => $"EXTRACT(MINUTE FROM CAST(REPLACE({args[0]}, 'T', ' ') AS DATETIME))",
        ScalarFn.Second => $"EXTRACT(SECOND FROM CAST(REPLACE({args[0]}, 'T', ' ') AS DATETIME))",
        _ => Internal.Query.ScalarSqlDefaults.Translate(this, fn, args, resultType)
    };

    public virtual string BuildJsonSetExpression() => "JSON_SET(Data, @path, CAST(@value AS JSON))";

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"LIMIT {take} OFFSET {offset}";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is MySqlException mysqlEx && mysqlEx.Number == 1062;

    public bool SupportsComputedColumns => true;

    public IReadOnlyList<string> BuildCreateComputedColumnSql(string tableName, string typeName, ComputedMapping mapping, string expressionSql)
    {
        var col = mapping.ColumnName;
        var statements = new List<string>
        {
            // MySQL VIRTUAL generated columns are recomputed on read but are indexable; the type is declared.
            $"ALTER TABLE `{tableName}` ADD COLUMN {col} {MySqlComputedType(mapping.ResultType)} AS ({expressionSql}) VIRTUAL;"
        };
        if (mapping.Indexed)
            // MySQL has no partial (filtered) index; the column is NULL for other types' rows.
            statements.Add($"CREATE INDEX idx_{col} ON `{tableName}` ({col});");
        return statements;
    }

    static string MySqlComputedType(Type clrType)
    {
        var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (t.IsEnum) t = Enum.GetUnderlyingType(t);
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte)) return "BIGINT";
        if (t == typeof(double) || t == typeof(float)) return "DOUBLE";
        if (t == typeof(decimal)) return "DECIMAL(38,10)";
        if (t == typeof(bool)) return "TINYINT(1)";
        return "CHAR(255)";
    }

    // ── Full-text search (generated STORED column + FULLTEXT index) ───────
    // MySQL cannot FULLTEXT-index a JSON column, so a stored generated column concatenates the mapped
    // paths and the FULLTEXT index covers that. Natural-language mode is inherently OR + relevance-ranked.

    public bool SupportsFullText => true;

    static string FtsColumn(string typeName) => "fts_" + FullTextMappingFactory.SanitizeSuffix(typeName);

    public IReadOnlyList<string> BuildCreateFullTextSql(string tableName, string typeName, FullTextMapping mapping)
    {
        var col = FtsColumn(typeName);
        var idx = "ft_" + FullTextMappingFactory.SanitizeSuffix(typeName);

        var sb = new System.Text.StringBuilder("CONCAT_WS(' '");
        foreach (var path in mapping.JsonPaths)
            sb.Append($", JSON_UNQUOTE(JSON_EXTRACT(Data, '$.{path}'))");
        sb.Append(')');

        // ADD COLUMN / ADD FULLTEXT have no IF NOT EXISTS in MySQL — re-runs throw and the caller
        // swallows "already exists", so each statement stays idempotent in practice.
        return new[]
        {
            $"ALTER TABLE `{tableName}` ADD COLUMN {col} TEXT GENERATED ALWAYS AS ({sb}) STORED;",
            $"ALTER TABLE `{tableName}` ADD FULLTEXT INDEX {idx} ({col});"
        };
    }

    public (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildFullTextSearchSql(
        string tableName, string typeName, FullTextMapping mapping,
        string searchText, int maxResults, string? additionalWhere)
    {
        var col = FtsColumn(typeName);
        var match = $"MATCH(d.{col}) AGAINST(@ftsQuery IN NATURAL LANGUAGE MODE)";
        var sql = $"""
            SELECT d.Data, {match} AS score
            FROM `{tableName}` d
            WHERE d.TypeName = @typeName AND {match}
              {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
            ORDER BY score DESC
            LIMIT {maxResults};
            """;

        return (sql, new Dictionary<string, object> { ["@ftsQuery"] = searchText });
    }

    // ── Composable Lucene queries (DocumentFunctions.LuceneMatch / LuceneScore) ───────────────────────
    // MySQL full-text uses IN BOOLEAN MODE, whose +/- prefix syntax expresses required / optional /
    // excluded terms, phrases ("a b"), grouping, prefixes (foo*) and proximity ("a b"@N) — but not fuzzy,
    // per-term boost, or field scoping.

    public virtual FtCapabilities FullTextQueryCapabilities => FtCapabilities.Prefix | FtCapabilities.Proximity;

    public string? BuildFullTextMatchSql(string tableName, string typeName, FullTextMapping mapping, FtQuery query, Func<string, string> bindParam)
    {
        var col = FtsColumn(typeName);
        var p = bindParam(new MySqlBooleanTranslator().Write(query.Root));
        return $"(MATCH({col}) AGAINST({p} IN BOOLEAN MODE))";
    }

    public string? BuildFullTextScoreSql(string tableName, string typeName, FullTextMapping mapping, FtQuery query, Func<string, string> bindParam)
    {
        var col = FtsColumn(typeName);
        var p = bindParam(new MySqlBooleanTranslator().Write(query.Root));
        return $"MATCH({col}) AGAINST({p} IN BOOLEAN MODE)";
    }

    // Renders the AST to MySQL BOOLEAN MODE syntax. Boolean mode has no infix AND/OR; instead a leading
    // '+' makes a term required and '-' excludes it, so the Lucene Must/Should/MustNot occur maps directly.
    sealed class MySqlBooleanTranslator
    {
        public string Write(FtNode node) => this.Render(node);

        string Render(FtNode node) => node switch
        {
            FtBool b => this.RenderBool(b),
            FtBoost bo => this.Render(bo.Node),
            _ => this.Atom(node)
        };

        string RenderBool(FtBool node)
        {
            var parts = new List<string>(node.Clauses.Count);
            foreach (var clause in node.Clauses)
            {
                var inner = this.Render(clause.Node);
                // Parenthesize a nested multi-clause boolean group so a leading +/- (required/excluded) applies
                // to the whole group, not just its first term — otherwise `(a OR b) AND c` collapses to
                // `+"a" "b" +"c"`, silently demoting b to optional. A single term/phrase needs no grouping.
                if (NeedsGroup(clause.Node))
                    inner = "(" + inner + ")";
                parts.Add(clause.Occur switch
                {
                    FtOccur.Must => "+" + inner,
                    FtOccur.MustNot => "-" + inner,
                    _ => inner
                });
            }
            return string.Join(" ", parts);
        }

        // A nested boolean group with more than one clause must be parenthesized when placed under an occur;
        // an FtBoost is transparent to grouping so unwrap it first.
        static bool NeedsGroup(FtNode node) => node switch
        {
            FtBoost b => NeedsGroup(b.Node),
            FtBool { Clauses.Count: > 1 } => true,
            _ => false
        };

        string Atom(FtNode node) => node switch
        {
            FtTerm t => Quote(t.Text),
            FtPrefix p => Alnum(p.Text) + "*",
            FtPhrase ph => "\"" + string.Join(' ', ph.Terms) + "\"",
            FtProximity pr => "\"" + string.Join(' ', pr.Terms) + "\"@" + pr.Slop,
            _ => throw new NotSupportedException($"Full-text node '{node.GetType().Name}' cannot be translated for MySQL.")
        };

        static string Quote(string term) => "\"" + term.Replace("\"", " ") + "\"";
        static string Alnum(string term) => new string(term.Where(char.IsLetterOrDigit).ToArray());
    }
}
