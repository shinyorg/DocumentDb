using System.Data.Common;
using System.Globalization;
using System.Text;
using Oracle.ManagedDataAccess.Client;
using Shiny.DocumentDb.Internal;
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

    // ── Spatial (generic envelope sidecar; bbox prune + C# refine, no SDO_GEOMETRY) ──
    public bool SupportsSpatial => true;

    public string? BuildCreateSpatialTablesSql(string tableName) => $"""
        BEGIN
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE "{tableName}_spatial" (
                    docId VARCHAR2(255) NOT NULL,
                    typeName VARCHAR2(255) NOT NULL,
                    minLat BINARY_DOUBLE NOT NULL, maxLat BINARY_DOUBLE NOT NULL,
                    minLng BINARY_DOUBLE NOT NULL, maxLng BINARY_DOUBLE NOT NULL,
                    CONSTRAINT pk_{tableName}_sp PRIMARY KEY (docId, typeName)
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN RAISE; END IF; -- ORA-00955: name already used
            END;
            EXECUTE IMMEDIATE 'CREATE INDEX IF NOT EXISTS "idx_{tableName}_sp" ON "{tableName}_spatial" (typeName, minLat, maxLat, minLng, maxLng)';
        END;
        """;

    public string? BuildSpatialUpsertSql(string tableName) => $"""
        MERGE INTO "{tableName}_spatial" t
        USING (SELECT @spatialDocId AS docId, @spatialTypeName AS typeName,
                      @spatialMinLat AS minLat, @spatialMaxLat AS maxLat,
                      @spatialMinLng AS minLng, @spatialMaxLng AS maxLng FROM DUAL) s
        ON (t.docId = s.docId AND t.typeName = s.typeName)
        WHEN MATCHED THEN UPDATE SET t.minLat = s.minLat, t.maxLat = s.maxLat, t.minLng = s.minLng, t.maxLng = s.maxLng
        WHEN NOT MATCHED THEN INSERT (docId, typeName, minLat, maxLat, minLng, maxLng)
            VALUES (s.docId, s.typeName, s.minLat, s.maxLat, s.minLng, s.maxLng)
        """;

    public string? BuildSpatialDeleteSql(string tableName)
        => $"DELETE FROM \"{tableName}_spatial\" WHERE docId = @spatialDocId AND typeName = @spatialTypeName";

    public string? BuildSpatialClearSql(string tableName)
        => $"DELETE FROM \"{tableName}_spatial\" WHERE typeName = @typeName";

    public string? BuildSpatialBoundingBoxQuerySql(string tableName, string? additionalWhere) => $"""
        SELECT d.Data FROM "{tableName}" d
        INNER JOIN "{tableName}_spatial" r ON r.docId = d.Id AND r.typeName = d.TypeName
        WHERE d.TypeName = @typeName
          AND r.maxLat >= @minLat AND r.minLat <= @maxLat
          AND r.maxLng >= @minLng AND r.minLng <= @maxLng
          {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
        """;

    // ── Temporal (system-time history sidecar) ──────────────────────────
    // Portable DML defaults apply (Oracle permits self-referencing DELETE subqueries and
    // INSERT ... SELECT from the same table). Only the idempotent DDL is provider-specific.

    public bool SupportsTemporal => true;

    public string BuildCreateHistoryTableSql(string tableName) => $"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE "{tableName}_history" (
                Id VARCHAR2(255) NOT NULL,
                TypeName VARCHAR2(255) NOT NULL,
                Version NUMBER NOT NULL,
                ValidFrom TIMESTAMP(6) WITH TIME ZONE NOT NULL,
                ValidTo TIMESTAMP(6) WITH TIME ZONE,
                Operation VARCHAR2(20) NOT NULL,
                Actor VARCHAR2(255),
                Data CLOB CONSTRAINT ensure_json_{tableName}_hist CHECK (Data IS JSON),
                CONSTRAINT pk_{tableName}_history PRIMARY KEY (Id, TypeName, Version)
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF; -- ORA-00955: name already used by an existing object
        END;
        """;

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

    // ── Bulk import (IDocumentBackup) collision modes ──────────────────────
    // Insert mode reuses BuildBatchInsertSql above. Oracle has no ON CONFLICT — Replace / SkipExisting
    // use MERGE with a source rowset built from UNION ALL'd SELECT ... FROM DUAL (one row per document),
    // mirroring BuildUpsertMergeSql's param naming and uncasted @data_i (the dialect wrapper rewrites
    // @-placeholders to :name and binds long JSON strings as CLOB).

    public string BuildBatchReplaceSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder($"MERGE INTO \"{tableName}\" t USING (");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(" UNION ALL ");
            sb.Append(i == 0
                ? $"SELECT @id_{i} AS Id, @typeName AS TypeName, @data_{i} AS Data FROM DUAL"
                : $"SELECT @id_{i}, @typeName, @data_{i} FROM DUAL");
        }
        sb.Append(") src ON (t.Id = src.Id AND t.TypeName = src.TypeName) ");
        sb.Append("WHEN MATCHED THEN UPDATE SET t.Data = src.Data, t.UpdatedAt = @now ");
        sb.Append("WHEN NOT MATCHED THEN INSERT (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES (src.Id, src.TypeName, src.Data, @now, @now)");
        return sb.ToString();
    }

    public string BuildBatchSkipExistingSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder($"MERGE INTO \"{tableName}\" t USING (");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(" UNION ALL ");
            sb.Append(i == 0
                ? $"SELECT @id_{i} AS Id, @typeName AS TypeName, @data_{i} AS Data FROM DUAL"
                : $"SELECT @id_{i}, @typeName, @data_{i} FROM DUAL");
        }
        sb.Append(") src ON (t.Id = src.Id AND t.TypeName = src.TypeName) ");
        sb.Append("WHEN NOT MATCHED THEN INSERT (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES (src.Id, src.TypeName, src.Data, @now, @now)");
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

    public string BuildListTablesSql()
        => "SELECT table_name FROM user_tables";

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

    // Oracle has no '&' operator — bitwise AND is the BITAND function.
    public string BitAnd(string left, string right) => $"BITAND({left}, {right})";
    public bool SupportsSoundex => true;

    // Oracle's CAST(... AS TIMESTAMP) can't parse the stored ISO-8601 'T' string (ORA-01858), so read
    // the fixed-layout date parts positionally: "YYYY-MM-DDTHH:MI:SS".
    public string TranslateScalar(ScalarFn fn, IReadOnlyList<string> args, Type resultType) => fn switch
    {
        ScalarFn.Year => $"TO_NUMBER(SUBSTR({args[0]}, 1, 4))",
        ScalarFn.Month => $"TO_NUMBER(SUBSTR({args[0]}, 6, 2))",
        ScalarFn.Day => $"TO_NUMBER(SUBSTR({args[0]}, 9, 2))",
        ScalarFn.Hour => $"TO_NUMBER(SUBSTR({args[0]}, 12, 2))",
        ScalarFn.Minute => $"TO_NUMBER(SUBSTR({args[0]}, 15, 2))",
        ScalarFn.Second => $"TO_NUMBER(SUBSTR({args[0]}, 18, 2))",
        _ => global::Shiny.DocumentDb.Internal.Query.ScalarSqlDefaults.Translate(this, fn, args, resultType)
    };

    public string BuildJsonSetExpression() => "shiny_json_set(Data, @path, @value)";

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"OFFSET {offset} ROWS FETCH NEXT {take} ROWS ONLY";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is OracleException oracleEx && oracleEx.Number == 1; // ORA-00001: unique constraint violated

    // ── Vector (Oracle 23ai native VECTOR / AI Vector Search) ───────────

    public bool SupportsVector => true;

    static string SanitizeForTableSuffix(string typeName)
    {
        var sb = new StringBuilder(typeName.Length);
        foreach (var c in typeName)
            sb.Append(char.IsLetterOrDigit(c) || c == '_' ? c : '_');
        return sb.ToString();
    }

    static string VecBare(string tableName, string typeName)
        => $"{tableName}_vec_{SanitizeForTableSuffix(typeName)}";

    // Sidecar identifiers are quoted to preserve case, matching the JSON-index naming convention.
    static string VecTable(string tableName, string typeName)
        => $"\"{VecBare(tableName, typeName)}\"";

    // Distance keyword shared by VECTOR_DISTANCE() and the CREATE VECTOR INDEX DISTANCE clause.
    static string MetricKeyword(VectorDistance metric) => metric switch
    {
        VectorDistance.Cosine => "COSINE",
        VectorDistance.Euclidean => "EUCLIDEAN",
        VectorDistance.DotProduct => "DOT",
        VectorDistance.Hamming => throw new NotSupportedException("Oracle FLOAT32 vectors do not support Hamming distance."),
        _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, null)
    };

    public string BuildCreateVectorTablesSql(string tableName, string typeName, VectorMapping mapping)
    {
        var bare = VecBare(tableName, typeName);
        var vec = VecTable(tableName, typeName);

        var sb = new StringBuilder();
        sb.Append("BEGIN\n");
        sb.Append("    BEGIN\n");
        sb.Append($"        EXECUTE IMMEDIATE 'CREATE TABLE {vec} (\n");
        sb.Append("            docId VARCHAR2(255) NOT NULL,\n");
        sb.Append("            typeName VARCHAR2(255) NOT NULL,\n");
        sb.Append($"            embedding VECTOR({mapping.Dimensions}, FLOAT32) NOT NULL,\n");
        sb.Append($"            CONSTRAINT pk_{bare} PRIMARY KEY (docId)\n");
        sb.Append("        )';\n");
        sb.Append("    EXCEPTION\n");
        sb.Append("        WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; -- ORA-00955: name already used\n");
        sb.Append("    END;\n");

        // Vector indexes need the database's vector memory pool (vector_memory_size) configured.
        // Where it isn't — or the edition lacks the feature — index creation fails and we silently
        // fall back to an exact sequential scan, which VECTOR_DISTANCE still serves correctly.
        var indexClause = mapping.IndexKind switch
        {
            VectorIndexKind.Hnsw =>
                $"ORGANIZATION INMEMORY NEIGHBOR GRAPH DISTANCE {MetricKeyword(mapping.Metric)} " +
                $"PARAMETERS (TYPE HNSW, NEIGHBORS {mapping.IndexOptions.HnswM ?? 16}, EFCONSTRUCTION {mapping.IndexOptions.HnswEfConstruction ?? 64})",
            VectorIndexKind.Ivf =>
                $"ORGANIZATION NEIGHBOR PARTITIONS DISTANCE {MetricKeyword(mapping.Metric)} " +
                $"PARAMETERS (TYPE IVF, NEIGHBOR PARTITIONS {mapping.IndexOptions.IvfLists ?? 100})",
            _ => null // None / Flat / DiskAnn / QuantizedFlat → exact scan, no index DDL
        };

        if (indexClause != null)
        {
            sb.Append("    BEGIN\n");
            sb.Append($"        EXECUTE IMMEDIATE 'CREATE VECTOR INDEX \"idx_{bare}\" ON {vec} (embedding) {indexClause}';\n");
            sb.Append("    EXCEPTION\n");
            sb.Append("        WHEN OTHERS THEN NULL; -- index unsupported/already present → exact scan fallback\n");
            sb.Append("    END;\n");
        }

        sb.Append("END;");
        return sb.ToString();
    }

    public string BuildVectorUpsertSql(string tableName, string typeName, VectorMapping mapping) => $"""
        MERGE INTO {VecTable(tableName, typeName)} t
        USING (SELECT @vecDocId AS docId FROM DUAL) src
        ON (t.docId = src.docId)
        WHEN MATCHED THEN
            UPDATE SET t.embedding = TO_VECTOR(@embedding)
        WHEN NOT MATCHED THEN
            INSERT (docId, typeName, embedding)
            VALUES (@vecDocId, @vecTypeName, TO_VECTOR(@embedding))
        """;

    public string BuildVectorDeleteSql(string tableName, string typeName)
        => $"DELETE FROM {VecTable(tableName, typeName)} WHERE docId = @vecDocId";

    public string BuildVectorClearSql(string tableName, string typeName)
        => $"DELETE FROM {VecTable(tableName, typeName)} WHERE typeName = @vecTypeName";

    public (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildVectorSearchSql(
        string tableName, string typeName, VectorMapping mapping,
        ReadOnlyMemory<float> query, int k, string? additionalWhere)
    {
        var vec = VecTable(tableName, typeName);
        var dist = $"VECTOR_DISTANCE(v.embedding, TO_VECTOR(@embedding), {MetricKeyword(mapping.Metric)})";

        // APPROX lets Oracle use the vector index when one exists; it transparently performs an
        // exact search when there is none, so it's safe to request only when an index was asked for.
        var approx = mapping.IndexKind is VectorIndexKind.Hnsw or VectorIndexKind.Ivf ? "APPROX " : "";

        var sql = $"""
            SELECT d.Data, {dist} AS score
            FROM {vec} v
            INNER JOIN "{tableName}" d ON d.Id = v.docId AND d.TypeName = @typeName
            {(additionalWhere != null ? $"WHERE {additionalWhere}" : "")}
            ORDER BY score
            FETCH {approx}FIRST {k} ROWS ONLY
            """;

        return (sql, new Dictionary<string, object> { ["@embedding"] = FormatVectorParameter(query, mapping) });
    }

    public object FormatVectorParameter(ReadOnlyMemory<float> vector, VectorMapping mapping)
    {
        // Oracle's TO_VECTOR parses a JSON-array string literal, e.g. '[1,2,3]'. Long embeddings
        // exceed the 4000-char VARCHAR2 bind limit and are bound as CLOB by OracleDialectCommand,
        // which TO_VECTOR also accepts.
        var sb = new StringBuilder(vector.Length * 12);
        sb.Append('[');
        var span = vector.Span;
        for (var i = 0; i < span.Length; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(span[i].ToString("R", CultureInfo.InvariantCulture));
        }
        sb.Append(']');
        return sb.ToString();
    }

    public bool SupportsComputedColumns => true;

    public IReadOnlyList<string> BuildCreateComputedColumnSql(string tableName, string typeName, ComputedMapping mapping, string expressionSql)
    {
        var col = mapping.ColumnName;
        var statements = new List<string>
        {
            // Oracle virtual columns infer their type from the (already-cast) expression and are indexable.
            $"ALTER TABLE \"{tableName}\" ADD ({col} AS ({expressionSql}))"
        };
        if (mapping.Indexed)
            statements.Add($"CREATE INDEX idx_{col} ON \"{tableName}\" ({col})");
        return statements;
    }

    // ── Full-text search (Oracle Text CTXSYS.CONTEXT) ────────────────────
    // Oracle Text cannot index a virtual column, so a real VARCHAR2 column is maintained by a
    // BEFORE INSERT/UPDATE trigger (one per mapped type) and a CONTEXT index (SYNC ON COMMIT) covers it.

    public bool SupportsFullText => true;

    static string FtsColumn(string typeName) => "fts_" + FullTextMappingFactory.SanitizeSuffix(typeName);

    public IReadOnlyList<string> BuildCreateFullTextSql(string tableName, string typeName, FullTextMapping mapping)
    {
        var col = FtsColumn(typeName);
        var trg = $"trg_{tableName}_{FullTextMappingFactory.SanitizeSuffix(typeName)}";
        var idx = $"idx_fts_{tableName}_{FullTextMappingFactory.SanitizeSuffix(typeName)}";
        var escapedType = typeName.Replace("'", "''");

        var assign = new StringBuilder();
        for (var i = 0; i < mapping.JsonPaths.Count; i++)
        {
            if (i > 0) assign.Append(" || ' ' || ");
            assign.Append($"JSON_VALUE(:new.Data, '$.{mapping.JsonPaths[i]}' RETURNING VARCHAR2(4000))");
        }

        // One PL/SQL block: add the backing column, (re)create the sync trigger, build the CONTEXT
        // index — each step ignores "already exists" so the block is idempotent.
        var block = $"""
            BEGIN
                BEGIN
                    EXECUTE IMMEDIATE 'ALTER TABLE "{tableName}" ADD ({col} VARCHAR2(4000))';
                EXCEPTION
                    WHEN OTHERS THEN IF SQLCODE != -1430 THEN RAISE; END IF; -- ORA-01430: column already exists
                END;

                EXECUTE IMMEDIATE q'[
                    CREATE OR REPLACE TRIGGER "{trg}" BEFORE INSERT OR UPDATE ON "{tableName}" FOR EACH ROW
                    WHEN (new.TypeName = '{escapedType}')
                    BEGIN
                        :new.{col} := {assign};
                    END;]';

                BEGIN
                    EXECUTE IMMEDIATE 'CREATE INDEX "{idx}" ON "{tableName}" ({col}) INDEXTYPE IS CTXSYS.CONTEXT PARAMETERS (''SYNC (ON COMMIT)'')';
                EXCEPTION
                    WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; -- ORA-00955: name already used
                END;
            END;
            """;

        return new[] { block };
    }

    public (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildFullTextSearchSql(
        string tableName, string typeName, FullTextMapping mapping,
        string searchText, int maxResults, string? additionalWhere)
    {
        var col = FtsColumn(typeName);
        var sql = $"""
            SELECT d.Data, SCORE(1) AS score
            FROM "{tableName}" d
            WHERE d.TypeName = @typeName AND CONTAINS(d.{col}, @ftsQuery, 1) > 0
              {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
            ORDER BY SCORE(1) DESC
            FETCH FIRST {maxResults} ROWS ONLY
            """;

        // Brace-escape each term so Oracle Text reserved words (and, or, not, within) are treated as
        // literals, then OR-combine.
        var terms = FullTextMappingFactory.Tokenize(searchText).Select(t => "{" + t + "}");
        var query = string.Join(" OR ", terms);
        return (sql, new Dictionary<string, object> { ["@ftsQuery"] = query });
    }
}
