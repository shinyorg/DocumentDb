using System.Data.Common;
using System.Globalization;
using System.Text;
using Oracle.ManagedDataAccess.Client;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.FullText;
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

    // ── Spatial ──
    public bool SupportsSpatial => true;

    /// <summary>Forces the dependency-free envelope tier (no SDO column/index, no native pushdown for
    /// DocumentFunctions-in-Where). Default false → native SDO_GEOMETRY column + spatial index.</summary>
    public bool PortableSpatial { get; init; }

    // Native pushdown stores geometry in an SDO_GEOMETRY column indexed by an MDSYS spatial index (populated
    // from GeoJSON on write), so the index-backed SDO_* operators do the 2-D pruning.
    public bool RequiresSpatialGeoJson => !this.PortableSpatial;

    // Oracle Spatial's domain index (MDSYS.SPATIAL_INDEX_V2) cannot be created on a quoted lower-case table —
    // it looks metadata up by the uppercase name and fails on a case-sensitive identifier. So in native mode
    // the sidecar is an UNQUOTED identifier (stored upper-case) with UPPER-cased USER_SDO_GEOM_METADATA. The
    // envelope tier keeps the quoted name (no SDO index, and preserves case-sensitive table names).
    string SpatialTable(string tableName) => this.PortableSpatial ? $"\"{tableName}_spatial\"" : $"{tableName}_spatial";

    // Predicates run over the sidecar's spatially-indexed geom column via the index-backed SDO_RELATE operator
    // — the spatial index does the pruning, no bbox prefilter needed. Disjoint can't be served by the index (it
    // finds interacting rows), so it uses the procedural SDO_GEOM.RELATE.
    public string? BuildSpatialFilterSql(
        string tableName, string jsonPath, string predicate,
        string geoJsonParam, string wktParam, string minLatParam, string maxLatParam, string minLngParam, string maxLngParam,
        string? metersParam)
    {
        if (this.PortableSpatial)
            return null;

        var query = $"SDO_UTIL.FROM_GEOJSON({geoJsonParam})";
        // The canonical index-backed operator: SDO_RELATE(geom, query, 'mask=<relationship>') = 'TRUE'. It's
        // more reliable than the named convenience operators (SDO_COVEREDBY etc. can silently under-match).
        string Rel(string mask) => $"SDO_RELATE(geom, {query}, 'mask={mask}') = 'TRUE'";
        // OGC predicates map to a UNION of Oracle masks: Oracle splits containment into INSIDE (strict) vs
        // COVEREDBY (boundary-touching), and CONTAINS (strict) vs COVERS — mutually exclusive — whereas the C#
        // relate engine treats strict-inside as within/coveredby too. So Within/CoveredBy = INSIDE+COVEREDBY and
        // Contains/Covers = CONTAINS+COVERS to reproduce OGC semantics.
        var pred = predicate switch
        {
            "Intersects" => Rel("ANYINTERACT"),
            "Contains" => Rel("CONTAINS+COVERS"),
            "Within" => Rel("INSIDE+COVEREDBY"),
            "Covers" => Rel("CONTAINS+COVERS"),
            "CoveredBy" => Rel("INSIDE+COVEREDBY"),
            "Touches" => Rel("TOUCH"),
            "Overlaps" => Rel("OVERLAPBDYINTERSECT"),
            "Equals" => Rel("EQUAL"),
            // Disjoint can't be served by a positive-mask index probe; procedural relate returns 'DISJOINT' when true.
            "Disjoint" => $"SDO_GEOM.RELATE(geom, 'DISJOINT', {query}, 0.005) <> 'FALSE'",
            "WithinDistance" => $"SDO_WITHIN_DISTANCE(geom, {query}, 'distance=' || TO_CHAR({metersParam}) || ' unit=METER') = 'TRUE'",
            _ => null   // Crosses has no clean SDO operator
        };
        if (pred == null)
            return null;

        return $"Id IN (SELECT docId FROM {this.SpatialTable(tableName)} WHERE typeName = @typeName AND {pred})";
    }

    // Verifies Oracle Spatial (MDSYS.SDO_GEOM) at init when native pushdown is on — fail-loud (only reached
    // when a geometry is mapped) — then provisions the SDO_GEOMETRY column, registers spatial metadata in
    // USER_SDO_GEOM_METADATA (SRID 4326), and creates the MDSYS spatial index. PortableSpatial skips all of
    // this (the envelope-sidecar tier needs no SDO).
    public string? BuildCreateSpatialTablesSql(string tableName)
    {
        var sp = this.SpatialTable(tableName);
        var geomCol = this.PortableSpatial ? "" : "geom SDO_GEOMETRY,\n                    ";
        // Unquoted native sidecar is stored upper-case; USER_SDO_GEOM_METADATA must match that upper-case name.
        var metaName = $"{tableName}_spatial".ToUpperInvariant();
        var nativeProvision = this.PortableSpatial ? "" : $$"""

            -- Register the geometry column (SRID 4326 = WGS84 lon/lat) before indexing it; idempotent, and
            -- COMMITted so the ODCI index routine sees it. Name is upper-case to match the unquoted table.
            DELETE FROM USER_SDO_GEOM_METADATA WHERE TABLE_NAME = '{{metaName}}' AND COLUMN_NAME = 'GEOM';
            INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID)
            VALUES ('{{metaName}}', 'GEOM',
                SDO_DIM_ARRAY(
                    SDO_DIM_ELEMENT('Longitude', -180, 180, 0.005),
                    SDO_DIM_ELEMENT('Latitude', -90, 90, 0.005)),
                4326);
            COMMIT;
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX "sidx_{{tableName}}_sp" ON {{sp}}(geom) INDEXTYPE IS MDSYS.SPATIAL_INDEX_V2';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE NOT IN (-955, -1408, -29879) THEN RAISE; END IF; -- already indexed
            END;
            """;
        return $$"""
        DECLARE
            n NUMBER;
        BEGIN
            {{(this.PortableSpatial ? "" : $$"""
            SELECT COUNT(*) INTO n FROM all_objects WHERE owner = 'MDSYS' AND object_name = 'SDO_GEOM';
            IF n = 0 THEN
                RAISE_APPLICATION_ERROR(-20991, 'Oracle Spatial (MDSYS.SDO_GEOM) is required for native spatial pushdown; install Oracle Spatial or set PortableSpatial = true.');
            END IF;
            """)}}
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {{sp}} (
                    docId VARCHAR2(255) NOT NULL,
                    typeName VARCHAR2(255) NOT NULL,
                    minLat BINARY_DOUBLE NOT NULL, maxLat BINARY_DOUBLE NOT NULL,
                    minLng BINARY_DOUBLE NOT NULL, maxLng BINARY_DOUBLE NOT NULL,
                    {{geomCol}}CONSTRAINT pk_{{tableName}}_sp PRIMARY KEY (docId, typeName)
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN RAISE; END IF; -- ORA-00955: name already used
            END;
            EXECUTE IMMEDIATE 'CREATE INDEX IF NOT EXISTS "idx_{{tableName}}_sp" ON {{sp}} (typeName, minLat, maxLat, minLng, maxLng)';
            {{nativeProvision}}
        END;
        """;
    }

    public string? BuildSpatialUpsertSql(string tableName)
    {
        var geomSel = this.PortableSpatial ? "" : ", SDO_UTIL.FROM_GEOJSON(@spatialGeoJson) AS geom";
        var geomUpd = this.PortableSpatial ? "" : ", t.geom = s.geom";
        var geomIns = this.PortableSpatial ? "" : ", geom";
        var geomVal = this.PortableSpatial ? "" : ", s.geom";
        return $"""
        MERGE INTO {this.SpatialTable(tableName)} t
        USING (SELECT @spatialDocId AS docId, @spatialTypeName AS typeName,
                      @spatialMinLat AS minLat, @spatialMaxLat AS maxLat,
                      @spatialMinLng AS minLng, @spatialMaxLng AS maxLng{geomSel} FROM DUAL) s
        ON (t.docId = s.docId AND t.typeName = s.typeName)
        WHEN MATCHED THEN UPDATE SET t.minLat = s.minLat, t.maxLat = s.maxLat, t.minLng = s.minLng, t.maxLng = s.maxLng{geomUpd}
        WHEN NOT MATCHED THEN INSERT (docId, typeName, minLat, maxLat, minLng, maxLng{geomIns})
            VALUES (s.docId, s.typeName, s.minLat, s.maxLat, s.minLng, s.maxLng{geomVal})
        """;
    }

    public string? BuildSpatialDeleteSql(string tableName)
        => $"DELETE FROM {this.SpatialTable(tableName)} WHERE docId = @spatialDocId AND typeName = @spatialTypeName";

    public string? BuildSpatialClearSql(string tableName)
        => $"DELETE FROM {this.SpatialTable(tableName)} WHERE typeName = @typeName";

    public string? BuildSpatialBoundingBoxQuerySql(string tableName, string? additionalWhere) => $"""
        SELECT d.Data FROM "{tableName}" d
        INNER JOIN {this.SpatialTable(tableName)} r ON r.docId = d.Id AND r.typeName = d.TypeName
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
                TenantId VARCHAR2(255),
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

    // Row-locking SELECT for the read-modify-write merge/replace fallback. Oracle pools connections (no
    // process-wide serialization), so without FOR UPDATE two concurrent Update(patch)/Upsert(patchIfUpdate:
    // false) calls on the same row would each read a stale snapshot and lose the other's write.
    public string BuildSelectDataForUpdateSql(string tableName)
        => $"SELECT Data FROM \"{tableName}\" WHERE Id = @id AND TypeName = @typeName FOR UPDATE";

    // Oracle has no shared row lock — FOR SHARE does not exist, and silently degrading to an unlocked read
    // would defeat the point of asking for a lock.
    public string BuildLockClause(LockMode mode) => mode switch
    {
        LockMode.Update => " FOR UPDATE",
        LockMode.Share => throw new NotSupportedException(
            "Oracle has no shared row lock (no FOR SHARE). Use LockMode.Update."),
        _ => string.Empty
    };

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

    // Backup Insert restore — mirrors BuildBatchInsertSql but binds CreatedAt/UpdatedAt per row so a v2 backup
    // preserves timestamps. Overridden (rather than using the interface default) because Oracle's dialect
    // wrapper rejects the terminating semicolon the default appends.
    public string BuildBackupInsertSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder($"INSERT INTO \"{tableName}\" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0)
                sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, @data_{i}, @ca_{i}, @ua_{i})");
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

    // Sidecar and JSON index identifiers are created quoted to preserve case, so user_indexes holds them
    // verbatim rather than folded to upper — hence no UPPER() on the table name here. Size comes from the
    // segment; Oracle tracks index usage only with monitoring explicitly enabled, so scans stay null.
    public string BuildListAllIndexesSql(string tableName)
        => $"""
            SELECT i.index_name, i.index_type, s.bytes, NULL
            FROM user_indexes i
            LEFT JOIN user_segments s ON s.segment_name = i.index_name AND s.segment_type = 'INDEX'
            WHERE i.table_name = '{tableName}'
            ORDER BY i.index_name
            """;

    // Oracle plans in two steps: populate the plan table, then format it. DBMS_XPLAN.DISPLAY reads the
    // most recent statement in PLAN_TABLE, so the pair has to run on one connection, in order.
    public IReadOnlyList<string> BuildExplainSql(string sql)
        => ["EXPLAIN PLAN FOR " + sql, "SELECT PLAN_TABLE_OUTPUT FROM TABLE(DBMS_XPLAN.DISPLAY())"];

    public string BuildListTablesSql()
        => "SELECT table_name FROM user_tables";

    // user_tab_columns is the current schema's columns - Oracle's information_schema equivalent, and the
    // only one a connection is guaranteed to be able to read without extra grants.
    public string BuildListColumnsSql()
        => "SELECT table_name, column_name, data_type FROM user_tab_columns";

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

    // Typed extraction for grouped aggregates: Oracle NUMBER is exact (no float round-off) for SUM/AVG of a
    // decimal, and MIN/MAX of a date/string returns VARCHAR2 so it compares by value (ISO-8601 dates sort
    // lexically) rather than a bogus numeric cast.
    public string JsonExtractTyped(string column, string jsonPath, Type clrType)
    {
        var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (t.IsEnum) t = Enum.GetUnderlyingType(t);
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte)
            || t == typeof(double) || t == typeof(float) || t == typeof(decimal))
            return $"JSON_VALUE({column}, '$.{jsonPath}' RETURNING NUMBER)";
        return $"JSON_VALUE({column}, '$.{jsonPath}')";
    }

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

    public string BuildJsonSetExpression(string sourceExpression, string pathParameter, string valueParameter)
        => $"shiny_json_set({sourceExpression}, {pathParameter}, {valueParameter})";

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"OFFSET {offset} ROWS FETCH NEXT {take} ROWS ONLY";

    public string BuildLimitClause(int take)
        => $"OFFSET 0 ROWS FETCH NEXT {take} ROWS ONLY";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is OracleException oracleEx && oracleEx.Number == 1; // ORA-00001: unique constraint violated

    // ── Vector (Oracle 23ai native VECTOR / AI Vector Search) ───────────

    public bool SupportsVector => true;

    // Sidecar identifiers are quoted to preserve case, matching the JSON-index naming convention.
    // Cast because VectorTableName is a default interface member — see the note in the PostgreSQL provider.
    string VecTable(string tableName, string typeName)
        => $"\"{((IDatabaseProvider)this).VectorTableName(tableName, typeName)}\"";

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
        var bare = ((IDatabaseProvider)this).VectorTableName(tableName, typeName);
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

    public string BuildVectorDocIdsSql(string tableName, string typeName)
        => $"SELECT docId FROM {VecTable(tableName, typeName)} WHERE typeName = @vecTypeName";

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

    static string FtsColumn(string typeName) => "fts_" + IDatabaseProvider.SanitizeTypeSuffix(typeName);

    /// <summary>
    /// The backing column is added <b>unquoted</b> by <see cref="BuildCreateFullTextSql"/>, so Oracle
    /// folds it to upper case in the catalog — while the table name is quoted at creation and is stored
    /// verbatim. Hence the asymmetry in this lookup.
    /// </summary>
    public string BuildFullTextProbeSql(string tableName, string typeName)
        => $"SELECT 1 FROM user_tab_columns WHERE table_name = '{tableName}' " +
           $"AND column_name = '{FtsColumn(typeName).ToUpperInvariant()}'";

    public IReadOnlyList<string> BuildCreateFullTextSql(string tableName, string typeName, FullTextMapping mapping)
    {
        var col = FtsColumn(typeName);
        var trg = $"trg_{tableName}_{IDatabaseProvider.SanitizeTypeSuffix(typeName)}";
        var idx = $"idx_fts_{tableName}_{IDatabaseProvider.SanitizeTypeSuffix(typeName)}";
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

    // ── Composable Lucene queries (DocumentFunctions.LuceneMatch) ─────────────────────────────────────
    // Oracle Text CONTAINS expresses terms, phrases, AND/OR/NOT (& | ~), grouping, prefix (foo%),
    // NEAR() proximity and fuzzy (?foo). LuceneScore is not supported standalone: Oracle's SCORE(n)
    // requires a co-located CONTAINS(...,n) label in the same query, so ranking must go through
    // store.FullTextSearch.

    public FtCapabilities FullTextQueryCapabilities => FtCapabilities.Prefix | FtCapabilities.Fuzzy | FtCapabilities.Proximity;

    public string? BuildFullTextMatchSql(string tableName, string typeName, FullTextMapping mapping, FtQuery query, Func<string, string> bindParam)
    {
        var col = FtsColumn(typeName);
        var p = bindParam(new OracleTextTranslator().Write(query.Root));
        return $"(CONTAINS({col}, {p}, 1) > 0)";
    }

    // Renders the AST to an Oracle Text CONTAINS expression. Terms are brace-escaped so reserved words
    // (and/or/not/near) are treated literally and operators cannot be injected.
    sealed class OracleTextTranslator : FtSqlTranslator
    {
        protected override string Term(string term) => Brace(term);
        protected override string Prefix(string term) => Alnum(term) + "%";
        protected override string Phrase(IReadOnlyList<string> terms) => Brace(string.Join(' ', terms));
        protected override string Proximity(IReadOnlyList<string> terms, int slop) => $"NEAR(({string.Join(", ", terms.Select(Brace))}), {slop})";
        protected override string Fuzzy(string term, int maxEdits) => "?" + Alnum(term);
        protected override string And(IReadOnlyList<string> parts) => string.Join(" & ", parts);
        protected override string Or(IReadOnlyList<string> parts) => string.Join(" | ", parts);
        protected override string AndNot(string basePart, string negated) => $"{basePart} ~ {negated}";

        static string Brace(string term) => "{" + term.Replace("{", "").Replace("}", "") + "}";
        static string Alnum(string term) => new string(term.Where(char.IsLetterOrDigit).ToArray());
    }

    // ── Blobs ──────────────────────────────────────────────────────────
    // No ON CONFLICT on Oracle — MERGE provides the upsert. Binary binding is promoted to
    // OracleDbType.Blob in OracleDialectCommand.

    /// <summary>A BLOB holds up to (4 GB - 1) * block size; 2 GB is the practical single-bind ceiling.</summary>
    public long MaxBlobSize => 2147483647L;

    public string BuildCreateBlobTableSql(string tableName) => $"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE "{tableName}_blobs" (
                Id VARCHAR2(255) NOT NULL,
                TypeName VARCHAR2(255) NOT NULL,
                BlobKey VARCHAR2(255) NOT NULL,
                Data BLOB NOT NULL,
                Length NUMBER NOT NULL,
                ContentType VARCHAR2(255),
                FileName VARCHAR2(1024),
                Hash VARCHAR2(64),
                CreatedAt TIMESTAMP(6) WITH TIME ZONE NOT NULL,
                UpdatedAt TIMESTAMP(6) WITH TIME ZONE NOT NULL,
                CONSTRAINT pk_{tableName}_blobs PRIMARY KEY (Id, TypeName, BlobKey)
            )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF; -- ORA-00955: name already used by an existing object
        END;
        """;

    public string BuildBlobUpsertSql(string tableName)
        => $"MERGE INTO \"{tableName}_blobs\" t " +
           "USING (SELECT @id AS Id, @typeName AS TypeName, @blobKey AS BlobKey FROM dual) s " +
           "ON (t.Id = s.Id AND t.TypeName = s.TypeName AND t.BlobKey = s.BlobKey) " +
           "WHEN MATCHED THEN UPDATE SET Data = @data, Length = @length, ContentType = @contentType, " +
           "FileName = @fileName, Hash = @hash, UpdatedAt = @updatedAt " +
           "WHEN NOT MATCHED THEN INSERT (Id, TypeName, BlobKey, Data, Length, ContentType, FileName, Hash, CreatedAt, UpdatedAt) " +
           "VALUES (@id, @typeName, @blobKey, @data, @length, @contentType, @fileName, @hash, @createdAt, @updatedAt)";

}
