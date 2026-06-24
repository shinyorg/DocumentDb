using System.Data.Common;
using MySqlConnector;
using Shiny.DocumentDb.Internal;

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

    public string BuildSetPropertySql(string tableName) => $"""
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

    public string JsonArrayLength(string column, string jsonPath)
        => $"JSON_LENGTH({column}, '$.{jsonPath}')";

    public string JsonEachFrom(string column, string jsonPath)
        => $"JSON_TABLE({column}, '$.{jsonPath}[*]' COLUMNS(value JSON PATH '$')) AS jt";

    public string JsonObject(IEnumerable<string> keyValuePairs)
        => $"JSON_OBJECT({string.Join(", ", keyValuePairs)})";

    public string JsonTrue() => "CAST('true' AS JSON)";

    public string JsonFalse() => "CAST('false' AS JSON)";

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

    public string BuildJsonSetExpression() => "JSON_SET(Data, @path, CAST(@value AS JSON))";

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"LIMIT {take} OFFSET {offset}";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is MySqlException mysqlEx && mysqlEx.Number == 1062;

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
}
