using System.Data.Common;
using Oracle.ManagedDataAccess.Client;

namespace Shiny.DocumentDb.Oracle;

public class OracleDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;

    public OracleDatabaseProvider(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public DbConnection CreateConnection() => new OracleConnection(this.connectionString);

    public async Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
    {
        await Task.CompletedTask;
    }

    public string BuildCreateTableSql(string tableName) => $"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE "{tableName}" (
                Id VARCHAR2(255) NOT NULL,
                TypeName VARCHAR2(255) NOT NULL,
                Data CLOB CONSTRAINT ensure_json__{tableName} CHECK (Data IS JSON),
                CreatedAt TIMESTAMP(6) NOT NULL,
                UpdatedAt TIMESTAMP(6) NOT NULL,
                CONSTRAINT pk_{tableName} PRIMARY KEY (Id, TypeName)
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF; -- -955 ist "name is already used by an existing object"
        END;
        """;

    public string BuildCreateTypenameIndexSql(string tableName)
        => $"CREATE INDEX idx_{tableName}_typename ON \"{tableName}\" (TypeName)";

    public string BuildInsertSql(string tableName) => $"""
        INSERT INTO "{tableName}" (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (:id, :typeName, :data, :now, :now)
        """;

    public string BuildUpdateSql(string tableName) => $"""
        UPDATE "{tableName}"
        SET Data = :data, UpdatedAt = :now
        WHERE Id = :id AND TypeName = :typeName
        """;

    public string BuildUpsertMergeSql(string tableName) => $"""
        MERGE INTO "{tableName}" t
        USING (SELECT :id AS Id, :typeName AS TypeName FROM DUAL) src
        ON (t.Id = src.Id AND t.TypeName = src.TypeName)
        WHEN MATCHED THEN
            UPDATE SET t.Data = JSON_MERGEPATCH(t.Data, :data), t.UpdatedAt = :now
        WHEN NOT MATCHED THEN
            INSERT (Id, TypeName, Data, CreatedAt, UpdatedAt)
            VALUES (:id, :typeName, :data, :now, :now)
        """;

    public string BuildSetPropertySql(string tableName) => $"""
        UPDATE "{tableName}"
        SET Data = JSON_TRANSFORM(Data, SET '$.' || :path = :value), UpdatedAt = :now
        WHERE Id = :id AND TypeName = :typeName
        """;

    public string BuildRemovePropertySql(string tableName) => $"""
        UPDATE "{tableName}"
        SET Data = JSON_TRANSFORM(Data, REMOVE '$.' || :path), UpdatedAt = :now
        WHERE Id = :id AND TypeName = :typeName
        """;

    public string BuildMaxIdSql(string tableName)
        => $"SELECT MAX(CAST(Id AS NUMBER)) FROM \"{tableName}\" WHERE TypeName = :typeName";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName)
        => $"CREATE INDEX {indexName} ON \"{tableName}\" (JSON_VALUE(Data, '$.{jsonPath}'))";

    public string BuildDropIndexSql(string indexName)
        => $"DROP INDEX {indexName}";

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT index_name FROM user_indexes WHERE table_name = '{tableName.ToUpper()}' AND index_name LIKE :prefix";

    public string JsonExtract(string column, string jsonPath)
        => $"JSON_VALUE({column}, '$.{jsonPath}')";

    public string JsonExtractElement(string jsonPath)
        => $"JSON_VALUE(value, '$.{jsonPath}')";

    public string JsonExtractElementNumeric(string jsonPath)
        => $"JSON_VALUE(value, '$.{jsonPath}' RETURNING NUMBER)";

    public string CastIntegerAggregate(string expression)
        => $"CAST({expression} AS NUMBER)";

    public string JsonExtractNumeric(string column, string jsonPath)
        => $"JSON_VALUE({column}, '$.{jsonPath}' RETURNING NUMBER)";

    public string JsonArrayLength(string column, string jsonPath)
        => $"JSON_LENGTH({column}, '$.{jsonPath}')"; // Ab Oracle 21c gibt es JSON_LENGTH

    public string JsonEachFrom(string column, string jsonPath)
        => $"JSON_TABLE({column}, '$.{jsonPath}[*]' COLUMNS (value VARCHAR2(4000) FORMAT JSON PATH '$')) jt";

    public string JsonObject(IEnumerable<string> keyValuePairs)
        => $"JSON_OBJECT({string.Join(", ", keyValuePairs)})";

    public string JsonTrue() => "'true'";

    public string JsonFalse() => "'false'";

    public string JsonNullCheck(string column, string jsonPath, bool isNull)
    {
        return isNull
            ? $"JSON_EXISTS({column}, '$.{jsonPath}?(@ == null)')"
            : $"NOT JSON_EXISTS({column}, '$.{jsonPath}?(@ == null)')";
    }

    public string JsonEachPrimitiveValue => "value";

    public string JsonEachPrimitiveNumericValue => "CAST(value AS NUMBER)";

    public string QuoteTable(string tableName) => $"\"{tableName}\"";

    public string ConcatStrings(params string[] parts) => string.Join(" || ", parts);

    public string BuildJsonSetExpression() => "JSON_TRANSFORM(Data, SET '$.' || :path = :value)";

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"OFFSET {offset} ROWS FETCH NEXT {take} ROWS ONLY";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is OracleException orclEx && orclEx.Number == 1; // ORA-00001: unique constraint violated
}