using System.Data.Common;
using System.Globalization;
using System.Text;
using DuckDB.NET.Data;
using Shiny.DocumentDb.DuckDb.Internal;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.DuckDb;

public class DuckDbDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;

    public DuckDbDatabaseProvider(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public DbConnection CreateConnection() => new DuckDbAtConnection(this.connectionString);

    // DuckDB is embedded — opening a fresh connection per op creates a separate database
    // (in-memory) or contends on the file lock. Keep one long-lived connection and serialize.
    public bool RequiresSingleConnection => true;

    public async Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "INSTALL json; LOAD json;";
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public string BuildCreateTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS "{tableName}" (
            Id VARCHAR NOT NULL,
            TypeName VARCHAR NOT NULL,
            Data JSON NOT NULL,
            CreatedAt TIMESTAMPTZ NOT NULL,
            UpdatedAt TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (Id, TypeName)
        );
        """;

    public string BuildCreateTypenameIndexSql(string tableName)
        => $"CREATE INDEX IF NOT EXISTS idx_{tableName}_typename ON \"{tableName}\" (TypeName);";

    // ── Temporal (system-time history sidecar) ──────────────────────────
    // Portable DML defaults apply; the JSON payload needs an explicit cast on insert.

    public bool SupportsTemporal => true;

    public string BuildCreateHistoryTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS "{tableName}_history" (
            Id VARCHAR NOT NULL,
            TypeName VARCHAR NOT NULL,
            Version BIGINT NOT NULL,
            ValidFrom TIMESTAMPTZ NOT NULL,
            ValidTo TIMESTAMPTZ NULL,
            Operation VARCHAR NOT NULL,
            Actor VARCHAR NULL,
            Data JSON NULL,
            PRIMARY KEY (Id, TypeName, Version)
        );
        """;

    public string BuildHistoryInsertSql(string tableName)
        => $"INSERT INTO \"{tableName}_history\" (Id, TypeName, Version, ValidFrom, ValidTo, Operation, Actor, Data) " +
           "SELECT @id, @typeName, COALESCE(MAX(Version), 0) + 1, @validFrom, NULL, @operation, @actor, CAST(@data AS JSON) " +
           $"FROM \"{tableName}_history\" WHERE Id = @id AND TypeName = @typeName";

    public string BuildInsertSql(string tableName) => $"""
        INSERT INTO "{tableName}" (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, CAST(@data AS JSON), @now, @now);
        """;

    public string BuildUpdateSql(string tableName) => $"""
        UPDATE "{tableName}"
        SET Data = CAST(@data AS JSON), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    // DuckDB exposes json_merge_patch (RFC 7396) since v0.10 — use the SupportsJsonMergePatch fast path.
    public bool SupportsJsonMergePatch => true;

    public string BuildUpsertMergeSql(string tableName) => $"""
        INSERT INTO "{tableName}" (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, CAST(@data AS JSON), @now, @now)
        ON CONFLICT (Id, TypeName) DO UPDATE SET
            Data = json_merge_patch("{tableName}".Data, EXCLUDED.Data),
            UpdatedAt = EXCLUDED.UpdatedAt;
        """;

    public bool SupportsBatchUpsert => true;

    // Multi-row deep-merge upsert in one round-trip. EXCLUDED.Data carries each conflicting row's
    // patch, so a single ON CONFLICT clause merges every row via json_merge_patch.
    public string BuildBatchUpsertSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO \"{tableName}\" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, CAST(@data_{i} AS JSON), @now, @now)");
        }
        sb.Append($" ON CONFLICT (Id, TypeName) DO UPDATE SET Data = json_merge_patch(\"{tableName}\".Data, EXCLUDED.Data), UpdatedAt = EXCLUDED.UpdatedAt;");
        return sb.ToString();
    }

    // DuckDB has no json_set/json_remove. We construct an RFC 7396 merge patch from the path by
    // folding the reversed path parts with string concatenation: ["city","shippingAddress"] +
    // value '"NY"' folds into '{"city":"NY"}' then '{"shippingAddress":{"city":"NY"}}'. The
    // accumulator is a VARCHAR (raw JSON text) because json_object with a dynamic VARCHAR key
    // inside a lambda is unreliable; once folded we CAST the string to JSON and merge.
    // FormatPropertyValue pre-quotes string values (e.g. "NY" → "\"NY\""), so @value is valid JSON.
    public string BuildSetPropertySql(string tableName) => $$"""
        UPDATE "{{tableName}}"
        SET Data = json_merge_patch(Data, CAST(
            list_reduce(
                list_reverse(string_split(REPLACE(@path, '$.', ''), '.')),
                (acc, part) -> '{"' || part || '":' || acc || '}',
                @value
            ) AS JSON
        )), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    // RFC 7396 semantics: setting a field to JSON null removes it from the document.
    public string BuildRemovePropertySql(string tableName) => $$"""
        UPDATE "{{tableName}}"
        SET Data = json_merge_patch(Data, CAST(
            list_reduce(
                list_reverse(string_split(REPLACE(@path, '$.', ''), '.')),
                (acc, part) -> '{"' || part || '":' || acc || '}',
                'null'
            ) AS JSON
        )), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildMaxIdSql(string tableName)
        => $"SELECT MAX(CAST(Id AS BIGINT)) FROM \"{tableName}\" WHERE TypeName = @typeName;";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName)
        => $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{tableName}\" (json_extract_string(Data, '$.{jsonPath}'));";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, IReadOnlyList<string> jsonPaths, string typeName)
    {
        if (jsonPaths.Count == 1)
            return this.BuildCreateJsonIndexSql(indexName, tableName, jsonPaths[0], typeName);
        var exprs = string.Join(", ", jsonPaths.Select(p => $"json_extract_string(Data, '$.{p}')"));
        return $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{tableName}\" ({exprs});";
    }

    public string BuildDropIndexSql(string indexName, string tableName)
        => $"DROP INDEX IF EXISTS {indexName};";

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT index_name FROM duckdb_indexes() WHERE table_name = '{tableName}' AND index_name LIKE @prefix;";

    public string JsonExtract(string column, string jsonPath)
        => $"json_extract_string({column}, '$.{jsonPath}')";

    public string JsonExtractTyped(string column, string jsonPath, Type clrType)
    {
        var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (t.IsEnum) t = Enum.GetUnderlyingType(t); // enums are stored as their numeric value
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte))
            return $"CAST(json_extract_string({column}, '$.{jsonPath}') AS BIGINT)";
        if (t == typeof(double) || t == typeof(float))
            return $"CAST(json_extract_string({column}, '$.{jsonPath}') AS DOUBLE)";
        if (t == typeof(decimal))
            return $"CAST(json_extract_string({column}, '$.{jsonPath}') AS DECIMAL(38,10))";
        if (t == typeof(bool))
            return $"CAST(json_extract_string({column}, '$.{jsonPath}') AS BOOLEAN)";
        return $"json_extract_string({column}, '$.{jsonPath}')";
    }

    public string JsonExtractElement(string jsonPath)
        => $"json_extract_string(value, '$.{jsonPath}')";

    public string JsonExtractElementTyped(string jsonPath, Type clrType)
    {
        var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (t.IsEnum) t = Enum.GetUnderlyingType(t); // enums are stored as their numeric value
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte))
            return $"CAST(json_extract_string(value, '$.{jsonPath}') AS BIGINT)";
        if (t == typeof(double) || t == typeof(float))
            return $"CAST(json_extract_string(value, '$.{jsonPath}') AS DOUBLE)";
        if (t == typeof(decimal))
            return $"CAST(json_extract_string(value, '$.{jsonPath}') AS DECIMAL(38,10))";
        if (t == typeof(bool))
            return $"CAST(json_extract_string(value, '$.{jsonPath}') AS BOOLEAN)";
        return $"json_extract_string(value, '$.{jsonPath}')";
    }

    public string JsonExtractElementNumeric(string jsonPath)
        => $"CAST(json_extract_string(value, '$.{jsonPath}') AS DOUBLE)";

    public string CastIntegerAggregate(string expression)
        => $"CAST({expression} AS BIGINT)";

    public string JsonExtractNumeric(string column, string jsonPath)
        => $"CAST(json_extract_string({column}, '$.{jsonPath}') AS DOUBLE)";

    public string JsonArrayLength(string column, string jsonPath)
        => $"json_array_length({column}, '$.{jsonPath}')";

    public string JsonEachFrom(string column, string jsonPath)
        => $"(SELECT unnest(CAST(json_extract({column}, '$.{jsonPath}') AS JSON[])) AS value) je";

    public string JsonObject(IEnumerable<string> keyValuePairs)
        => $"json_object({string.Join(", ", keyValuePairs)})";

    public string JsonTrue() => "CAST('true' AS JSON)";

    public string JsonFalse() => "CAST('false' AS JSON)";

    public string JsonNullCheck(string column, string jsonPath, bool isNull)
    {
        var raw = $"json_extract({column}, '$.{jsonPath}')";
        // DuckDB returns SQL NULL when the path is missing, and JSON 'null' literal when the field is explicitly null.
        return isNull
            ? $"({raw} IS NULL OR json_type({raw}) = 'NULL')"
            : $"({raw} IS NOT NULL AND json_type({raw}) <> 'NULL')";
    }

    public string JsonEachPrimitiveValue => "json_extract_string(value, '$')";
    public string JsonEachPrimitiveNumericValue => "CAST(json_extract_string(value, '$') AS DOUBLE)";

    public string QuoteTable(string tableName) => $"\"{tableName.Replace("\"", "\"\"")}\"";

    public string ConcatStrings(params string[] parts) => string.Join(" || ", parts);

    public string BuildJsonSetExpression() => """
        json_merge_patch(Data, CAST(list_reduce(
            list_reverse(string_split(REPLACE(@path, '$.', ''), '.')),
            (acc, part) -> '{"' || part || '":' || acc || '}',
            @value
        ) AS JSON))
        """;

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"LIMIT {take} OFFSET {offset}";

    public bool IsDuplicateKeyException(Exception ex)
    {
        // DuckDB.NET throws DuckDBException; constraint violations carry "Constraint Error" or
        // "violates primary key constraint" in the message.
        var msg = ex.Message;
        return msg.Contains("Duplicate key", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("PRIMARY KEY", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("UNIQUE constraint", StringComparison.OrdinalIgnoreCase);
    }

    public string BuildBatchInsertSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO \"{tableName}\" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, CAST(@data_{i} AS JSON), @now, @now)");
        }
        sb.Append(';');
        return sb.ToString();
    }

    // ── Vector (DuckDB vss extension) ────────────────────────────────────

    public bool SupportsVector => true;

    public async Task LoadVectorExtensionAsync(DbConnection connection, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "INSTALL vss; LOAD vss;";
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        // HNSW persistence on file-backed DBs requires the experimental flag.
        await using var flagCmd = connection.CreateCommand();
        flagCmd.CommandText = "SET hnsw_enable_experimental_persistence = true;";
        try { await flagCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false); }
        catch { /* in-memory DB — flag not required */ }
    }

    static string SanitizeForTableSuffix(string typeName)
    {
        var sb = new StringBuilder(typeName.Length);
        foreach (var c in typeName)
            sb.Append(char.IsLetterOrDigit(c) || c == '_' ? c : '_');
        return sb.ToString();
    }

    static string VecTable(string tableName, string typeName)
        => $"\"{tableName}_vec_{SanitizeForTableSuffix(typeName)}\"";

    static string MetricKey(VectorDistance metric) => metric switch
    {
        VectorDistance.Cosine => "cosine",
        VectorDistance.Euclidean => "l2sq",
        VectorDistance.DotProduct => "ip",
        VectorDistance.Hamming => throw new NotSupportedException("DuckDB vss does not support Hamming distance."),
        _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, null)
    };

    static string DistanceCall(VectorDistance metric, string col, string lit) => metric switch
    {
        VectorDistance.Cosine => $"array_cosine_distance({col}, {lit})",
        VectorDistance.Euclidean => $"array_distance({col}, {lit})",
        VectorDistance.DotProduct => $"-array_inner_product({col}, {lit})", // negate so ORDER BY ASC = "nearest"
        _ => throw new NotSupportedException()
    };

    public string BuildCreateVectorTablesSql(string tableName, string typeName, VectorMapping mapping)
    {
        var vec = VecTable(tableName, typeName);
        var bare = $"{tableName}_vec_{SanitizeForTableSuffix(typeName)}";
        var sb = new StringBuilder();
        sb.Append($"""
            CREATE TABLE IF NOT EXISTS {vec} (
                docId VARCHAR PRIMARY KEY,
                typeName VARCHAR NOT NULL,
                embedding FLOAT[{mapping.Dimensions}] NOT NULL
            );

            """);

        if (mapping.IndexKind == VectorIndexKind.Hnsw)
        {
            sb.Append($"CREATE INDEX IF NOT EXISTS idx_{bare} ON {vec} USING HNSW (embedding) WITH (metric = '{MetricKey(mapping.Metric)}');");
        }

        return sb.ToString();
    }

    public string BuildVectorUpsertSql(string tableName, string typeName, VectorMapping mapping)
    {
        var vec = VecTable(tableName, typeName);
        return $"""
            INSERT INTO {vec} (docId, typeName, embedding)
            VALUES (@vecDocId, @vecTypeName, CAST(@embedding AS FLOAT[{mapping.Dimensions}]))
            ON CONFLICT (docId) DO UPDATE SET embedding = EXCLUDED.embedding;
            """;
    }

    public string BuildVectorDeleteSql(string tableName, string typeName)
        => $"DELETE FROM {VecTable(tableName, typeName)} WHERE docId = @vecDocId;";

    public string BuildVectorClearSql(string tableName, string typeName)
        => $"DELETE FROM {VecTable(tableName, typeName)} WHERE typeName = @vecTypeName;";

    public (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildVectorSearchSql(
        string tableName, string typeName, VectorMapping mapping,
        ReadOnlyMemory<float> query, int k, string? additionalWhere)
    {
        var vec = VecTable(tableName, typeName);
        var lit = $"CAST(@embedding AS FLOAT[{mapping.Dimensions}])";
        var dist = DistanceCall(mapping.Metric, "v.embedding", lit);

        var sql = $"""
            SELECT d.Data, {dist} AS score
            FROM {vec} v
            INNER JOIN "{tableName}" d ON d.Id = v.docId AND d.TypeName = @typeName
            {(additionalWhere != null ? $"WHERE {additionalWhere}" : "")}
            ORDER BY {dist}
            LIMIT {k};
            """;

        return (sql, new Dictionary<string, object> { ["@embedding"] = FormatVectorParameter(query, mapping) });
    }

    public object FormatVectorParameter(ReadOnlyMemory<float> vector, VectorMapping mapping)
    {
        // DuckDB accepts the JSON-array literal cast to FLOAT[N].
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
}
