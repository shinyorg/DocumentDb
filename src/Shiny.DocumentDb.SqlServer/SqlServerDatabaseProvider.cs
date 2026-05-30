using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Shiny.DocumentDb.SqlServer;

public class SqlServerDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;

    public SqlServerDatabaseProvider(string connectionString)
    {
        this.connectionString = connectionString;
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
    // computed column and an index over that column. Convention: the backing computed column
    // is named "cc_{indexName}" so DropIndex can find and remove it.
    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName) => $"""
        IF NOT EXISTS (SELECT * FROM sys.computed_columns WHERE name = 'cc_{indexName}' AND object_id = OBJECT_ID('{tableName}'))
            ALTER TABLE [{tableName}] ADD [cc_{indexName}] AS CAST(JSON_VALUE(Data, '$.{jsonPath}') AS NVARCHAR(450));
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{indexName}' AND object_id = OBJECT_ID('{tableName}'))
            CREATE INDEX {indexName} ON [{tableName}] ([cc_{indexName}]) WHERE TypeName = '{typeName}';
        """;

    public string BuildDropIndexSql(string indexName, string tableName) => $"""
        IF EXISTS (SELECT * FROM sys.indexes WHERE name = '{indexName}' AND object_id = OBJECT_ID('{tableName}'))
            DROP INDEX [{indexName}] ON [{tableName}];
        IF EXISTS (SELECT * FROM sys.computed_columns WHERE name = 'cc_{indexName}' AND object_id = OBJECT_ID('{tableName}'))
            ALTER TABLE [{tableName}] DROP COLUMN [cc_{indexName}];
        """;

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('{tableName}') AND name LIKE @prefix;";

    public string JsonExtract(string column, string jsonPath)
        => $"JSON_VALUE({column}, '$.{jsonPath}')";

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

    public string BuildJsonSetExpression() => "JSON_MODIFY(Data, @path, @value)";

    public object FormatPropertyValue(object? value) => value ?? DBNull.Value;

    public string BuildPaginationClause(int offset, int take)
        => $"OFFSET {offset} ROWS FETCH NEXT {take} ROWS ONLY";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is SqlException sqlEx && (sqlEx.Number == 2627 || sqlEx.Number == 2601);

}
