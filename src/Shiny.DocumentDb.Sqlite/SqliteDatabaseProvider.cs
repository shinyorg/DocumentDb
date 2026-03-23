using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace Shiny.DocumentDb.Sqlite;

public class SqliteDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;

    public SqliteDatabaseProvider(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public DbConnection CreateConnection() => new SqliteConnection(this.connectionString);

    public async Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
    {
        await using var walCmd = connection.CreateCommand();
        walCmd.CommandText = "PRAGMA journal_mode=WAL;";
        await walCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public string BuildCreateTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS {tableName} (
            Id TEXT NOT NULL,
            TypeName TEXT NOT NULL,
            Data TEXT NOT NULL,
            CreatedAt TEXT NOT NULL,
            UpdatedAt TEXT NOT NULL,
            PRIMARY KEY (Id, TypeName)
        );
        """;

    public string BuildCreateTypenameIndexSql(string tableName)
        => $"CREATE INDEX IF NOT EXISTS idx_{tableName}_typename ON {tableName} (TypeName);";

    public string BuildInsertSql(string tableName) => $"""
        INSERT INTO {tableName} (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, @data, @now, @now);
        """;

    public string BuildUpdateSql(string tableName) => $"""
        UPDATE {tableName}
        SET Data = @data, UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildUpsertMergeSql(string tableName) => $"""
        INSERT INTO {tableName} (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, @data, @now, @now)
        ON CONFLICT(Id, TypeName) DO UPDATE SET
            Data = json_patch({tableName}.Data, @data),
            UpdatedAt = @now;
        """;

    public string BuildSetPropertySql(string tableName) => $"""
        UPDATE {tableName}
        SET Data = json_set(Data, @path, json(@value)), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildRemovePropertySql(string tableName) => $"""
        UPDATE {tableName}
        SET Data = json_remove(Data, @path), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildMaxIdSql(string tableName)
        => $"SELECT MAX(CAST(Id AS INTEGER)) FROM {tableName} WHERE TypeName = @typeName;";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName)
        => $"CREATE INDEX IF NOT EXISTS {indexName} ON {tableName} (json_extract(Data, '$.{jsonPath}')) WHERE TypeName = '{typeName}';";

    public string BuildDropIndexSql(string indexName)
        => $"DROP INDEX IF EXISTS {indexName};";

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = '{tableName}' AND name LIKE @prefix;";

    public string JsonExtract(string column, string jsonPath)
        => $"json_extract({column}, '$.{jsonPath}')";

    public string JsonExtractElement(string jsonPath)
        => $"json_extract(value, '$.{jsonPath}')";

    public string JsonExtractElementNumeric(string jsonPath)
        => $"json_extract(value, '$.{jsonPath}')";
    public string CastIntegerAggregate(string expression)
        => expression;
    public string JsonExtractNumeric(string column, string jsonPath)
        => $"CAST(json_extract({column}, '$.{jsonPath}') AS REAL)";

    public string JsonArrayLength(string column, string jsonPath)
        => $"json_array_length({column}, '$.{jsonPath}')";

    public string JsonEachFrom(string column, string jsonPath)
        => $"json_each({column}, '$.{jsonPath}')";

    public string JsonObject(IEnumerable<string> keyValuePairs)
        => $"json_object({string.Join(", ", keyValuePairs)})";

    public string JsonTrue() => "json('true')";

    public string JsonFalse() => "json('false')";

    public string JsonNullCheck(string column, string jsonPath, bool isNull)
    {
        var extract = JsonExtract(column, jsonPath);
        return isNull ? $"{extract} IS NULL" : $"{extract} IS NOT NULL";
    }

    public string JsonEachPrimitiveValue => "value";
    public string JsonEachPrimitiveNumericValue => "value";
    public string QuoteTable(string tableName) => tableName;

    public string ConcatStrings(params string[] parts) => string.Join(" || ", parts);

    public string BuildJsonSetExpression() => "json_set(Data, @path, json(@value))";

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"LIMIT {take} OFFSET {offset}";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is SqliteException sqliteEx && sqliteEx.SqliteErrorCode == 19;

    public bool SupportsBackup => true;

    public async Task BackupAsync(DbConnection connection, string destinationPath, CancellationToken ct)
    {
        var destinationConnectionString = new SqliteConnectionStringBuilder
        {
            DataSource = destinationPath
        }.ToString();

        await using var destination = new SqliteConnection(destinationConnectionString);
        await destination.OpenAsync(ct).ConfigureAwait(false);
        ((SqliteConnection)connection).BackupDatabase(destination);
    }
}
