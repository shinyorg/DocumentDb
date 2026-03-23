using System.Data.Common;

namespace Shiny.DocumentDb;

public interface IDatabaseProvider
{
    // Connection
    DbConnection CreateConnection();
    Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct);

    // Schema DDL
    string BuildCreateTableSql(string tableName);
    string BuildCreateTypenameIndexSql(string tableName);

    // CRUD SQL builders
    string BuildInsertSql(string tableName);
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

    // Backup (optional)
    bool SupportsBackup { get; }
    Task BackupAsync(DbConnection connection, string destinationPath, CancellationToken ct);
}
