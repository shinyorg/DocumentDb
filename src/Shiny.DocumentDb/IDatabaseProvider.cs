using System.Data.Common;
using System.Text;

namespace Shiny.DocumentDb;

public interface IDatabaseProvider
{
    // Connection
    DbConnection CreateConnection();
    Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct);

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
    string BuildDropIndexSql(string indexName);
    string BuildListJsonIndexesSql(string tableName, string prefix);

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

    // Spatial (optional — only SQLite implements these)
    bool SupportsSpatial => false;
    string? BuildCreateSpatialTablesSql(string tableName) => null;
    string? BuildSpatialUpsertSql(string tableName) => null;
    string? BuildSpatialDeleteSql(string tableName) => null;
    string? BuildSpatialClearSql(string tableName) => null;
    string? BuildSpatialBoundingBoxQuerySql(string tableName, string? additionalWhere) => null;
}
