using System.Data.Common;
using Npgsql;

namespace Shiny.DocumentDb.PostgreSql;

public class PostgreSqlDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;

    public PostgreSqlDatabaseProvider(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public DbConnection CreateConnection() => new NpgsqlConnection(this.connectionString);

    public Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct) => Task.CompletedTask;

    public string BuildCreateTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS {tableName} (
            "Id" TEXT NOT NULL,
            "TypeName" TEXT NOT NULL,
            "Data" JSONB NOT NULL,
            "CreatedAt" TIMESTAMPTZ NOT NULL,
            "UpdatedAt" TIMESTAMPTZ NOT NULL,
            PRIMARY KEY ("Id", "TypeName")
        );
        """;

    public string BuildCreateTypenameIndexSql(string tableName)
        => $"CREATE INDEX IF NOT EXISTS idx_{tableName}_typename ON {tableName} (\"TypeName\");";

    public string BuildInsertSql(string tableName) => $"""
        INSERT INTO {tableName} ("Id", "TypeName", "Data", "CreatedAt", "UpdatedAt")
        VALUES (@id, @typeName, CAST(@data AS JSONB), @now, @now);
        """;

    public string BuildUpdateSql(string tableName) => $"""
        UPDATE {tableName}
        SET "Data" = CAST(@data AS JSONB), "UpdatedAt" = @now
        WHERE "Id" = @id AND "TypeName" = @typeName;
        """;

    public string BuildUpsertMergeSql(string tableName) => $"""
        INSERT INTO {tableName} ("Id", "TypeName", "Data", "CreatedAt", "UpdatedAt")
        VALUES (@id, @typeName, CAST(@data AS JSONB), @now, @now)
        ON CONFLICT("Id", "TypeName") DO UPDATE SET
            "Data" = {tableName}."Data" || CAST(EXCLUDED."Data" AS JSONB),
            "UpdatedAt" = EXCLUDED."UpdatedAt";
        """;

    public string BuildSetPropertySql(string tableName) => $"""
        UPDATE {tableName}
        SET "Data" = jsonb_set("Data", @path, CAST(@value AS JSONB)), "UpdatedAt" = @now
        WHERE "Id" = @id AND "TypeName" = @typeName;
        """;

    public string BuildRemovePropertySql(string tableName) => $"""
        UPDATE {tableName}
        SET "Data" = "Data" #- @path, "UpdatedAt" = @now
        WHERE "Id" = @id AND "TypeName" = @typeName;
        """;

    public string BuildMaxIdSql(string tableName)
        => $"SELECT MAX(CAST(\"Id\" AS BIGINT)) FROM {tableName} WHERE \"TypeName\" = @typeName;";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName)
        => $"CREATE INDEX IF NOT EXISTS {indexName} ON {tableName} (({JsonExtract("\"Data\"", jsonPath)})) WHERE \"TypeName\" = '{typeName}';";

    public string BuildDropIndexSql(string indexName)
        => $"DROP INDEX IF EXISTS {indexName};";

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT indexname FROM pg_indexes WHERE tablename = '{tableName}' AND indexname LIKE @prefix;";

    public string JsonExtract(string column, string jsonPath)
        => $"{column}->>'$.{jsonPath}'";

    public string JsonExtractElement(string jsonPath)
        => $"value->>'$.{jsonPath}'";

    public string JsonArrayLength(string column, string jsonPath)
        => $"jsonb_array_length({column}->'$.{jsonPath}')";

    public string JsonEachFrom(string column, string jsonPath)
        => $"jsonb_array_elements({column}->'$.{jsonPath}') AS value";

    public string JsonObject(IEnumerable<string> keyValuePairs)
        => $"jsonb_build_object({string.Join(", ", keyValuePairs)})";

    public string JsonTrue() => "'true'::jsonb";

    public string JsonFalse() => "'false'::jsonb";

    public string BuildPaginationClause(int offset, int take)
        => $"LIMIT {take} OFFSET {offset}";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is PostgresException pgEx && pgEx.SqlState == "23505";

    public bool SupportsBackup => false;

    public Task BackupAsync(DbConnection connection, string destinationPath, CancellationToken ct)
        => throw new NotSupportedException("PostgreSQL backup is not supported through this provider.");
}
