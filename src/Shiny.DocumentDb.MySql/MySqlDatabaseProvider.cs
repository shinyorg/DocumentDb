using System.Data.Common;
using MySqlConnector;

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

    public string BuildDropIndexSql(string indexName)
        => $"DROP INDEX {indexName};";

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = '{tableName}' AND INDEX_NAME LIKE @prefix GROUP BY INDEX_NAME;";

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

    public string BuildJsonSetExpression() => "JSON_SET(Data, @path, CAST(@value AS JSON))";

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"LIMIT {take} OFFSET {offset}";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is MySqlException mysqlEx && mysqlEx.Number == 1062;

    public bool SupportsBackup => false;

    public Task BackupAsync(DbConnection connection, string destinationPath, CancellationToken ct)
        => throw new NotSupportedException("MySQL backup is not supported through this provider.");
}
