using System.Data.Common;
using System.Globalization;
using System.Text;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.Sqlite;

public class SqliteDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;

    public SqliteDatabaseProvider(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public string ConnectionString => this.connectionString;

    /// <summary>
    /// When true, attempts to load the <c>sqlite-vec</c> extension on every connection. The
    /// caller must ensure the extension binary is reachable via <see cref="VectorExtensionPath"/>.
    /// Defaults to <c>false</c>; flip on if any <c>MapVectorProperty</c> is registered.
    /// </summary>
    public bool EnableVectorExtension { get; init; }

    /// <summary>
    /// Extension binary path/name passed to <see cref="SqliteConnection.LoadExtension(string)"/>.
    /// Defaults to <c>"vec0"</c>; the loader searches the standard OS paths and the application
    /// directory.
    /// </summary>
    public string VectorExtensionPath { get; init; } = "vec0";

    public DbConnection CreateConnection() => new SqliteConnection(this.connectionString);

    // SQLite locks the whole database on writes — keep one long-lived connection and serialize.
    public bool RequiresSingleConnection => true;

    public async Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
    {
        if (!OperatingSystem.IsBrowser())
        {
            await using var walCmd = connection.CreateCommand();
            walCmd.CommandText = "PRAGMA journal_mode=WAL;";
            await walCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    public string BuildCreateTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS {QuoteTable(tableName)} (
            Id TEXT NOT NULL,
            TypeName TEXT NOT NULL,
            Data TEXT NOT NULL,
            CreatedAt TEXT NOT NULL,
            UpdatedAt TEXT NOT NULL,
            PRIMARY KEY (Id, TypeName)
        );
        """;

    public string BuildCreateTypenameIndexSql(string tableName)
        => $"CREATE INDEX IF NOT EXISTS idx_{tableName}_typename ON {QuoteTable(tableName)} (TypeName);";

    public string BuildInsertSql(string tableName) => $"""
        INSERT INTO {QuoteTable(tableName)} (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, @data, @now, @now);
        """;

    public string BuildUpdateSql(string tableName) => $"""
        UPDATE {QuoteTable(tableName)}
        SET Data = @data, UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildUpsertMergeSql(string tableName) => $"""
        INSERT INTO {QuoteTable(tableName)} (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, @data, @now, @now)
        ON CONFLICT(Id, TypeName) DO UPDATE SET
            Data = json_patch({QuoteTable(tableName)}.Data, @data),
            UpdatedAt = @now;
        """;

    public string BuildSetPropertySql(string tableName) => $"""
        UPDATE {QuoteTable(tableName)}
        SET Data = json_set(Data, @path, json(@value)), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildRemovePropertySql(string tableName) => $"""
        UPDATE {QuoteTable(tableName)}
        SET Data = json_remove(Data, @path), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildMaxIdSql(string tableName)
        => $"SELECT MAX(CAST(Id AS INTEGER)) FROM {QuoteTable(tableName)} WHERE TypeName = @typeName;";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName)
        => $"CREATE INDEX IF NOT EXISTS {indexName} ON {QuoteTable(tableName)} (json_extract(Data, '$.{jsonPath}')) WHERE TypeName = '{typeName}';";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, IReadOnlyList<string> jsonPaths, string typeName)
    {
        if (jsonPaths.Count == 1)
            return this.BuildCreateJsonIndexSql(indexName, tableName, jsonPaths[0], typeName);
        var exprs = string.Join(", ", jsonPaths.Select(p => $"json_extract(Data, '$.{p}')"));
        return $"CREATE INDEX IF NOT EXISTS {indexName} ON {QuoteTable(tableName)} ({exprs}) WHERE TypeName = '{typeName}';";
    }

    public string BuildDropIndexSql(string indexName, string tableName)
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
    public string QuoteTable(string tableName) => $"\"{tableName.Replace("\"", "\"\"")}\"";

    public string ConcatStrings(params string[] parts) => string.Join(" || ", parts);

    public string BuildJsonSetExpression() => "json_set(Data, @path, json(@value))";

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"LIMIT {take} OFFSET {offset}";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is SqliteException sqliteEx && sqliteEx.SqliteErrorCode == 19;


    // ── Spatial (R*Tree) ───────────────────────────────────────────────
    // R*Tree virtual tables are not available in WASM builds of SQLite.

    public bool SupportsSpatial => !OperatingSystem.IsBrowser();

    public string BuildCreateSpatialTablesSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS {tableName}_spatial_map (
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            docId TEXT NOT NULL,
            typeName TEXT NOT NULL,
            UNIQUE(docId, typeName)
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS {tableName}_spatial USING rtree(
            id,
            minLat, maxLat,
            minLng, maxLng
        );
        """;

    public string BuildSpatialUpsertSql(string tableName) => $"""
        INSERT INTO {tableName}_spatial_map (docId, typeName)
        VALUES (@spatialDocId, @spatialTypeName)
        ON CONFLICT(docId, typeName) DO UPDATE SET docId = docId;

        INSERT OR REPLACE INTO {tableName}_spatial (id, minLat, maxLat, minLng, maxLng)
        VALUES (
            (SELECT rowid FROM {tableName}_spatial_map WHERE docId = @spatialDocId AND typeName = @spatialTypeName),
            @spatialLat, @spatialLat, @spatialLng, @spatialLng
        );
        """;

    public string BuildSpatialDeleteSql(string tableName) => $"""
        DELETE FROM {tableName}_spatial WHERE id IN (
            SELECT rowid FROM {tableName}_spatial_map WHERE docId = @spatialDocId AND typeName = @spatialTypeName
        );
        DELETE FROM {tableName}_spatial_map WHERE docId = @spatialDocId AND typeName = @spatialTypeName;
        """;

    public string BuildSpatialClearSql(string tableName) => $"""
        DELETE FROM {tableName}_spatial WHERE id IN (
            SELECT rowid FROM {tableName}_spatial_map WHERE typeName = @typeName
        );
        DELETE FROM {tableName}_spatial_map WHERE typeName = @typeName;
        """;

    public string BuildSpatialBoundingBoxQuerySql(string tableName, string? additionalWhere) => $"""
        SELECT d.Data FROM {tableName} d
        INNER JOIN {tableName}_spatial_map m ON m.docId = d.Id AND m.typeName = d.TypeName
        INNER JOIN {tableName}_spatial r ON r.id = m.rowid
        WHERE d.TypeName = @typeName
          AND r.maxLat >= @minLat AND r.minLat <= @maxLat
          AND r.maxLng >= @minLng AND r.minLng <= @maxLng
          {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
        """;

    // ── Vector (sqlite-vec) ──────────────────────────────────────────────
    // Sidecar virtual table per type. vec0 indexes only an integer rowid, so we keep a map
    // table that bridges docId -> rowid, mirroring the R*Tree spatial pattern.

    public bool SupportsVector => this.EnableVectorExtension;

    public async Task LoadVectorExtensionAsync(DbConnection connection, CancellationToken ct)
    {
        if (!this.EnableVectorExtension) return;
        if (connection is not SqliteConnection sqlite)
            throw new InvalidOperationException("LoadVectorExtensionAsync expects a SqliteConnection.");

        // EnableExtensions is per-connection. Idempotent — safe to call repeatedly.
        sqlite.EnableExtensions(true);
        try
        {
            sqlite.LoadExtension(this.VectorExtensionPath);
        }
        catch (Exception ex)
        {
            throw new NotSupportedException(
                $"Failed to load the sqlite-vec extension from '{this.VectorExtensionPath}'. " +
                "Install the sqlite-vec native binary and ensure it is on the load path, then " +
                "set SqliteDatabaseProvider.VectorExtensionPath to its file name (without extension).",
                ex);
        }
        await Task.CompletedTask;
    }

    static string SanitizeForTableSuffix(string typeName)
    {
        var sb = new StringBuilder(typeName.Length);
        foreach (var c in typeName)
            sb.Append(char.IsLetterOrDigit(c) || c == '_' ? c : '_');
        return sb.ToString();
    }

    static string VecTableName(string tableName, string typeName)
        => $"{tableName}_vec_{SanitizeForTableSuffix(typeName)}";

    static string VecMapTableName(string tableName, string typeName)
        => $"{tableName}_vec_map_{SanitizeForTableSuffix(typeName)}";

    static string MetricKeyword(VectorDistance metric) => metric switch
    {
        VectorDistance.Cosine => "cosine",
        VectorDistance.Euclidean => "L2",
        VectorDistance.DotProduct => "dot",
        VectorDistance.Hamming => throw new NotSupportedException("sqlite-vec does not support Hamming distance on float embeddings."),
        _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, null)
    };

    public string BuildCreateVectorTablesSql(string tableName, string typeName, VectorMapping mapping)
    {
        var vec = VecTableName(tableName, typeName);
        var map = VecMapTableName(tableName, typeName);
        var keyword = MetricKeyword(mapping.Metric);
        return $"""
            CREATE TABLE IF NOT EXISTS {map} (
                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                docId TEXT NOT NULL UNIQUE
            );
            CREATE VIRTUAL TABLE IF NOT EXISTS {vec} USING vec0(
                embedding float[{mapping.Dimensions}] distance_metric={keyword}
            );
            """;
    }

    public string BuildVectorUpsertSql(string tableName, string typeName, VectorMapping mapping)
    {
        var vec = VecTableName(tableName, typeName);
        var map = VecMapTableName(tableName, typeName);
        return $"""
            INSERT INTO {map} (docId) VALUES (@vecDocId)
            ON CONFLICT(docId) DO UPDATE SET docId = docId;

            INSERT OR REPLACE INTO {vec} (rowid, embedding)
            VALUES (
                (SELECT rowid FROM {map} WHERE docId = @vecDocId),
                @embedding
            );
            """;
    }

    public string BuildVectorDeleteSql(string tableName, string typeName)
    {
        var vec = VecTableName(tableName, typeName);
        var map = VecMapTableName(tableName, typeName);
        return $"""
            DELETE FROM {vec} WHERE rowid IN (
                SELECT rowid FROM {map} WHERE docId = @vecDocId
            );
            DELETE FROM {map} WHERE docId = @vecDocId;
            """;
    }

    public string BuildVectorClearSql(string tableName, string typeName)
    {
        var vec = VecTableName(tableName, typeName);
        var map = VecMapTableName(tableName, typeName);
        return $"""
            DELETE FROM {vec};
            DELETE FROM {map};
            """;
    }

    public (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildVectorSearchSql(
        string tableName, string typeName, VectorMapping mapping,
        ReadOnlyMemory<float> query, int k, string? additionalWhere)
    {
        var vec = VecTableName(tableName, typeName);
        var map = VecMapTableName(tableName, typeName);

        // sqlite-vec post-filters: pull more candidates than k so the predicate doesn't starve.
        var multiplier = 4;
        if (mapping.IndexOptions.ProviderHints.TryGetValue("sqlite.postFilterMultiplier", out var hint) && hint is int m)
            multiplier = Math.Max(1, m);
        var candidateK = additionalWhere == null ? k : k * multiplier;

        var sql = $"""
            SELECT d.Data, v.distance AS score
            FROM {vec} v
            INNER JOIN {map} m ON m.rowid = v.rowid
            INNER JOIN {QuoteTable(tableName)} d ON d.Id = m.docId AND d.TypeName = @typeName
            WHERE v.embedding MATCH @embedding
              AND k = {candidateK}
              {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
            ORDER BY v.distance
            LIMIT {k};
            """;

        return (sql, new Dictionary<string, object> { ["@embedding"] = FormatVectorParameter(query, mapping) });
    }

    public object FormatVectorParameter(ReadOnlyMemory<float> vector, VectorMapping mapping)
    {
        // sqlite-vec accepts JSON-array text via the implicit text -> vec coercion.
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
