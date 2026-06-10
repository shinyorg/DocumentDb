using System.Data.Common;
using System.Text;
using Oracle.ManagedDataAccess.Client;
using Shiny.DocumentDb.Oracle.Internal;

namespace Shiny.DocumentDb.Oracle;

/// <summary>
/// Oracle Database provider. Requires Oracle 23ai or later (multi-row INSERT VALUES,
/// CREATE INDEX IF NOT EXISTS, JSON constructor). Documents are stored as IS JSON-checked
/// CLOBs; dynamic JSON path set/remove goes through helper PL/SQL functions because Oracle's
/// JSON_TRANSFORM only accepts literal path expressions.
/// </summary>
public class OracleDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;

    public OracleDatabaseProvider(string connectionString)
    {
        this.connectionString = connectionString;
    }

    // The dialect wrapper rewrites the core's @name placeholders / trailing semicolons /
    // FROM-less SELECTs into Oracle dialect at execution time
    public DbConnection CreateConnection()
        => new OracleDialectConnection(new OracleConnection(this.connectionString));

    public Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
        => Task.CompletedTask;

    public string BuildCreateTableSql(string tableName) => $"""
        BEGIN
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE "{tableName}" (
                    Id VARCHAR2(255) NOT NULL,
                    TypeName VARCHAR2(255) NOT NULL,
                    Data CLOB CONSTRAINT ensure_json_{tableName} CHECK (Data IS JSON),
                    CreatedAt TIMESTAMP(6) WITH TIME ZONE NOT NULL,
                    UpdatedAt TIMESTAMP(6) WITH TIME ZONE NOT NULL,
                    CONSTRAINT pk_{tableName} PRIMARY KEY (Id, TypeName)
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN RAISE; END IF; -- ORA-00955: name already used by an existing object
            END;

            -- JSON_TRANSFORM requires literal path expressions, so dynamic set/remove must be
            -- composed inside PL/SQL where the path can be inlined into the statement text
            EXECUTE IMMEDIATE q'[
                CREATE OR REPLACE FUNCTION shiny_json_set(doc CLOB, pth VARCHAR2, val CLOB) RETURN CLOB IS
                    res CLOB;
                BEGIN
                    EXECUTE IMMEDIATE 'SELECT JSON_TRANSFORM(:d, SET ''' || pth || ''' = JSON(:v) RETURNING CLOB) FROM DUAL'
                        INTO res USING doc, val;
                    RETURN res;
                END;]';

            EXECUTE IMMEDIATE q'[
                CREATE OR REPLACE FUNCTION shiny_json_remove(doc CLOB, pth VARCHAR2) RETURN CLOB IS
                    res CLOB;
                BEGIN
                    EXECUTE IMMEDIATE 'SELECT JSON_TRANSFORM(:d, REMOVE ''' || pth || ''' IGNORE ON MISSING RETURNING CLOB) FROM DUAL'
                        INTO res USING doc;
                    RETURN res;
                END;]';
        END;
        """;

    public string BuildCreateTypenameIndexSql(string tableName)
        => $"CREATE INDEX IF NOT EXISTS \"idx_{tableName}_typename\" ON \"{tableName}\" (TypeName)";

    public string BuildAddTenantColumnSql(string tableName) => $"""
        BEGIN
            EXECUTE IMMEDIATE 'ALTER TABLE "{tableName}" ADD (TenantId VARCHAR2(255))';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -1430 THEN RAISE; END IF; -- ORA-01430: column already exists
        END;
        """;

    public string BuildCreateTenantIndexSql(string tableName)
        => $"CREATE INDEX IF NOT EXISTS \"IX_{tableName}_TenantId\" ON \"{tableName}\" (TenantId, TypeName)";

    public string BuildInsertSql(string tableName) => $"""
        INSERT INTO "{tableName}" (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, @data, @now, @now)
        """;

    public string BuildBatchInsertSql(string tableName, int batchSize)
    {
        // Multi-row VALUES requires Oracle 23ai+
        var sb = new StringBuilder($"INSERT INTO \"{tableName}\" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0)
                sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, @data_{i}, @now, @now)");
        }
        return sb.ToString();
    }

    public string BuildUpdateSql(string tableName) => $"""
        UPDATE "{tableName}"
        SET Data = @data, UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName
        """;

    public string BuildUpsertMergeSql(string tableName) => $"""
        MERGE INTO "{tableName}" t
        USING (SELECT @id AS Id, @typeName AS TypeName FROM DUAL) src
        ON (t.Id = src.Id AND t.TypeName = src.TypeName)
        WHEN MATCHED THEN
            UPDATE SET t.Data = JSON_MERGEPATCH(t.Data, @data RETURNING CLOB), t.UpdatedAt = @now
        WHEN NOT MATCHED THEN
            INSERT (Id, TypeName, Data, CreatedAt, UpdatedAt)
            VALUES (@id, @typeName, @data, @now, @now)
        """;

    public string BuildSetPropertySql(string tableName) => $"""
        UPDATE "{tableName}"
        SET Data = shiny_json_set(Data, @path, @value), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName
        """;

    public string BuildRemovePropertySql(string tableName) => $"""
        UPDATE "{tableName}"
        SET Data = shiny_json_remove(Data, @path), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName
        """;

    public string BuildMaxIdSql(string tableName)
        => $"SELECT MAX(CAST(Id AS NUMBER DEFAULT NULL ON CONVERSION ERROR)) FROM \"{tableName}\" WHERE TypeName = @typeName";

    // Index names are quoted to preserve the core's lowercase idx_json_ naming — unquoted
    // identifiers fold to uppercase and would never match the LIKE prefix in list/drop
    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName)
        => $"CREATE INDEX \"{indexName}\" ON \"{tableName}\" (JSON_VALUE(Data, '$.{jsonPath}'))";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, IReadOnlyList<string> jsonPaths, string typeName)
    {
        if (jsonPaths.Count == 1)
            return this.BuildCreateJsonIndexSql(indexName, tableName, jsonPaths[0], typeName);
        var exprs = string.Join(", ", jsonPaths.Select(p => $"JSON_VALUE(Data, '$.{p}')"));
        return $"CREATE INDEX \"{indexName}\" ON \"{tableName}\" ({exprs})";
    }

    public string BuildDropIndexSql(string indexName, string tableName)
        => $"DROP INDEX \"{indexName}\"";

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT index_name FROM user_indexes WHERE table_name = '{tableName}' AND index_name LIKE @prefix";

    public string JsonExtract(string column, string jsonPath)
        => $"JSON_VALUE({column}, '$.{jsonPath}')";

    public string JsonExtractElement(string jsonPath)
        => $"JSON_VALUE(jval, '$.{jsonPath}')";

    public string JsonExtractElementNumeric(string jsonPath)
        => $"JSON_VALUE(jval, '$.{jsonPath}' RETURNING NUMBER)";

    public string CastIntegerAggregate(string expression)
        => $"CAST({expression} AS NUMBER)";

    public string JsonExtractNumeric(string column, string jsonPath)
        => $"JSON_VALUE({column}, '$.{jsonPath}' RETURNING NUMBER)";

    public string JsonArrayLength(string column, string jsonPath)
        => $"JSON_VALUE({column}, '$.{jsonPath}.size()' RETURNING NUMBER)";

    // Two projections of each element: jval carries JSON text for object elements
    // (JsonExtractElement reads into it), sval carries the unquoted scalar for primitive arrays
    public string JsonEachFrom(string column, string jsonPath)
        => $"JSON_TABLE({column}, '$.{jsonPath}[*]' COLUMNS (jval CLOB FORMAT JSON PATH '$', sval VARCHAR2(4000) PATH '$')) jt";

    public string JsonObject(IEnumerable<string> keyValuePairs)
    {
        // Pairs arrive flattened: 'key' literal followed by its value expression
        var list = keyValuePairs as IList<string> ?? keyValuePairs.ToList();
        var sb = new StringBuilder("JSON_OBJECT(");
        for (var i = 0; i + 1 < list.Count; i += 2)
        {
            if (i > 0)
                sb.Append(", ");
            sb.Append(list[i]).Append(" VALUE ").Append(list[i + 1]);
        }
        return sb.Append(" RETURNING CLOB)").ToString();
    }

    // Only used inside projection CASE expressions feeding JSON_OBJECT — 23ai boolean
    // literals serialize as real JSON booleans there ('true' strings would not)
    public string JsonTrue() => "TRUE";

    public string JsonFalse() => "FALSE";

    public string JsonNullCheck(string column, string jsonPath, bool isNull)
        => isNull
            ? $"(NOT JSON_EXISTS({column}, '$.{jsonPath}') OR JSON_EXISTS({column}, '$.{jsonPath}?(@ == null)'))"
            : $"JSON_EXISTS({column}, '$.{jsonPath}?(@ != null)')";

    public string JsonEachPrimitiveValue => "sval";

    public string JsonEachPrimitiveNumericValue => "CAST(sval AS NUMBER)";

    public string QuoteTable(string tableName) => $"\"{tableName}\"";

    public string ConcatStrings(params string[] parts) => string.Join(" || ", parts);

    public string BuildJsonSetExpression() => "shiny_json_set(Data, @path, @value)";

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"OFFSET {offset} ROWS FETCH NEXT {take} ROWS ONLY";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is OracleException oracleEx && oracleEx.Number == 1; // ORA-00001: unique constraint violated
}
