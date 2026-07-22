using System.Data.Common;
using System.Globalization;
using System.Text;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.FullText;

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
    /// When true, attempts to load the <c>sqlite-vec</c> extension on every connection via
    /// <see cref="SqliteConnection.LoadExtension(string)"/>. The caller must ensure the extension
    /// binary is reachable via <see cref="VectorExtensionPath"/>. Defaults to <c>false</c>; flip
    /// on if any <c>MapVectorProperty</c> is registered.
    /// <para>
    /// This path does <b>not</b> work on iOS (and usually not on Android): Apple forbids
    /// <c>dlopen</c> of loose dynamic libraries, and the bundled <c>e_sqlite3</c> is compiled with
    /// runtime extension loading disabled. On those platforms link <c>sqlite-vec</c> statically and
    /// register it with <c>sqlite3_auto_extension(sqlite3_vec_init)</c> before any connection opens,
    /// then set <see cref="VectorExtensionPreloaded"/> instead of this flag.
    /// </para>
    /// </summary>
    public bool EnableVectorExtension { get; init; }

    /// <summary>
    /// When true, the <c>sqlite-vec</c> extension is assumed to be already available on every
    /// connection — e.g. statically linked into the app and registered via
    /// <c>sqlite3_auto_extension(sqlite3_vec_init)</c>. The provider skips
    /// <see cref="SqliteConnection.LoadExtension(string)"/> entirely (the only workable approach on
    /// iOS). Mutually complementary with <see cref="EnableVectorExtension"/>; if both are set, the
    /// preloaded path wins and no runtime load is attempted.
    /// </summary>
    public bool VectorExtensionPreloaded { get; init; }

    /// <summary>
    /// Extension binary path/name passed to <see cref="SqliteConnection.LoadExtension(string)"/>.
    /// Defaults to <c>"vec0"</c>; the loader searches the standard OS paths and the application
    /// directory. Ignored when <see cref="VectorExtensionPreloaded"/> is set.
    /// </summary>
    public string VectorExtensionPath { get; init; } = "vec0";

    public DbConnection CreateConnection() => new SqliteConnection(this.connectionString);

    // SQLite locks the whole database on writes — keep one long-lived connection and serialize.
    public bool RequiresSingleConnection => true;

    // SQLite ships soundex() only with the SQLITE_SOUNDEX compile flag (absent from the bundled
    // e_sqlite3), so register the canonical CLR implementation as a connection UDF instead.
    public bool SupportsSoundex => true;
    public bool SupportsUserFunctions => true;

    public async Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
    {
        if (connection is SqliteConnection sqlite)
        {
            sqlite.CreateFunction<string?, string?>("soundex", s => s == null ? null : DocumentFunctions.Soundex(s));
            SqliteSpatialFunctions.Register(sqlite);
        }

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

    public bool SupportsBatchUpsert => true;

    // Multi-row deep-merge upsert in one round-trip. excluded.Data carries each conflicting row's
    // patch, so a single ON CONFLICT clause merges every row via json_patch.
    public string BuildBatchUpsertSql(string tableName, int batchSize)
    {
        var qt = QuoteTable(tableName);
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {qt} (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, @data_{i}, @now, @now)");
        }
        sb.Append($" ON CONFLICT(Id, TypeName) DO UPDATE SET Data = json_patch({qt}.Data, excluded.Data), UpdatedAt = @now;");
        return sb.ToString();
    }

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

    public string BuildListTablesSql()
        => "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';";

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

    // SQLite has no EXTRACT; date parts come from strftime over the stored ISO-8601 string.
    public string TranslateScalar(ScalarFn fn, IReadOnlyList<string> args, Type resultType) => fn switch
    {
        ScalarFn.Year => $"CAST(strftime('%Y', {args[0]}) AS INTEGER)",
        ScalarFn.Month => $"CAST(strftime('%m', {args[0]}) AS INTEGER)",
        ScalarFn.Day => $"CAST(strftime('%d', {args[0]}) AS INTEGER)",
        ScalarFn.Hour => $"CAST(strftime('%H', {args[0]}) AS INTEGER)",
        ScalarFn.Minute => $"CAST(strftime('%M', {args[0]}) AS INTEGER)",
        ScalarFn.Second => $"CAST(strftime('%S', {args[0]}) AS INTEGER)",
        _ => Internal.Query.ScalarSqlDefaults.Translate(this, fn, args, resultType)
    };

    public string BuildJsonSetExpression() => "json_set(Data, @path, json(@value))";

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    // Microsoft.Data.Sqlite binds a CLR decimal as TEXT. SQLite type affinity ranks TEXT above REAL,
    // so a numeric JSON value (stored as REAL) is never > / < / = a decimal parameter. Bind decimals
    // as double (REAL) so comparisons against json_extract numbers work as expected.
    public object? NormalizeParameterValue(object? value) => value is decimal d ? (double)d : value;

    public string BuildPaginationClause(int offset, int take)
        => $"LIMIT {take} OFFSET {offset}";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is SqliteException sqliteEx && sqliteEx.SqliteErrorCode == 19;


    // ── Temporal (system-time history sidecar) ──────────────────────────
    // All history DML uses the portable IDatabaseProvider defaults; only the DDL is provider-specific.

    public bool SupportsTemporal => true;

    public string BuildCreateHistoryTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS {QuoteTable(tableName + "_history")} (
            Id TEXT NOT NULL,
            TypeName TEXT NOT NULL,
            Version INTEGER NOT NULL,
            ValidFrom TEXT NOT NULL,
            ValidTo TEXT NULL,
            Operation TEXT NOT NULL,
            Actor TEXT NULL,
            Data TEXT NULL,
            TenantId TEXT NULL,
            PRIMARY KEY (Id, TypeName, Version)
        );
        """;


    // ── Blobs ──────────────────────────────────────────────────────────
    // All blob DML uses the portable IDatabaseProvider defaults (SQLite has supported
    // ON CONFLICT … DO UPDATE since 3.24); only the DDL is provider-specific.

    /// <summary>SQLITE_MAX_LENGTH defaults to 1 GB, which caps a single BLOB value.</summary>
    public long MaxBlobSize => 1024L * 1024 * 1024;

    public string BuildCreateBlobTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS {QuoteTable(tableName + "_blobs")} (
            Id TEXT NOT NULL,
            TypeName TEXT NOT NULL,
            BlobKey TEXT NOT NULL,
            Data BLOB NOT NULL,
            Length INTEGER NOT NULL,
            ContentType TEXT NULL,
            FileName TEXT NULL,
            Hash TEXT NULL,
            CreatedAt TEXT NOT NULL,
            UpdatedAt TEXT NOT NULL,
            PRIMARY KEY (Id, TypeName, BlobKey)
        );
        """;


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
            @spatialMinLat, @spatialMaxLat, @spatialMinLng, @spatialMaxLng
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

    public string? BuildSpatialFilterSql(
        string tableName, string jsonPath, string predicate,
        string geoJsonParam, string wktParam, string minLatParam, string maxLatParam, string minLngParam, string maxLngParam,
        string? metersParam)
    {
        var udf = predicate switch
        {
            "Intersects" => "docdb_st_intersects",
            "Disjoint" => "docdb_st_disjoint",
            "Contains" => "docdb_st_contains",
            "Within" => "docdb_st_within",
            "Covers" => "docdb_st_covers",
            "CoveredBy" => "docdb_st_coveredby",
            "Touches" => "docdb_st_touches",
            "Crosses" => "docdb_st_crosses",
            "Overlaps" => "docdb_st_overlaps",
            "Equals" => "docdb_st_equals",
            "WithinDistance" => "docdb_st_dwithin",
            _ => null
        };
        if (udf == null)
            return null;

        var stored = $"json_extract(Data, '$.{jsonPath}')";
        var refine = metersParam != null
            ? $"{udf}({stored}, {geoJsonParam}, {metersParam}) = 1"
            : $"{udf}({stored}, {geoJsonParam}) = 1";

        // Disjoint is anti-selective — the R*Tree can't prune it (matches are mostly rows *outside* the bbox),
        // so it scans and refines. Every other predicate implies bbox overlap, so we prune with the R*Tree.
        if (predicate == "Disjoint")
            return refine;

        var candidates = $"""
            Id IN (
                SELECT m.docId FROM {QuoteTable(tableName + "_spatial_map")} m
                JOIN {QuoteTable(tableName + "_spatial")} r ON r.id = m.rowid
                WHERE m.typeName = @typeName
                  AND r.maxLat >= {minLatParam} AND r.minLat <= {maxLatParam}
                  AND r.maxLng >= {minLngParam} AND r.minLng <= {maxLngParam}
            )
            """;
        return $"({candidates} AND {refine})";
    }

    public string? BuildSpatialDistanceSql(string? tableName, string jsonPath, string geoJsonParam, string wktParam)
        => $"docdb_st_distance(json_extract(Data, '$.{jsonPath}'), {geoJsonParam})";

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

    public bool SupportsVector => this.EnableVectorExtension || this.VectorExtensionPreloaded;

    public async Task LoadVectorExtensionAsync(DbConnection connection, CancellationToken ct)
    {
        // Statically-linked + auto-registered builds (the only path that works on iOS) need no
        // runtime load — vec0 is already on every connection.
        if (this.VectorExtensionPreloaded || !this.EnableVectorExtension) return;
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
            var platformHint = OperatingSystem.IsIOS() || OperatingSystem.IsTvOS() || OperatingSystem.IsAndroid()
                ? " On iOS/Android the bundled SQLite cannot dlopen extensions: statically link sqlite-vec, " +
                  "register it with sqlite3_auto_extension(sqlite3_vec_init) at startup, and set " +
                  "SqliteDatabaseProvider.VectorExtensionPreloaded = true (do not set EnableVectorExtension)."
                : "";
            throw new NotSupportedException(
                $"Failed to load the sqlite-vec extension from '{this.VectorExtensionPath}'. " +
                "Install the sqlite-vec native binary and ensure it is on the load path, then " +
                "set SqliteDatabaseProvider.VectorExtensionPath to its file name (without extension)." +
                platformHint,
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
        VectorDistance.DotProduct => throw new NotSupportedException(
            "sqlite-vec supports only Cosine and Euclidean distance_metric — DotProduct (inner product) is not available. Use Cosine or Euclidean."),
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

    // ── Full-text search (FTS5) ──────────────────────────────────────────
    // A single FTS5 table per documents table stores the searchable text; per-type triggers keep it in
    // sync with the documents table (one trigger set per mapped type, guarded by TypeName so multiple
    // types can share a table). The FTS rowid mirrors the documents table rowid for the join back.

    public bool SupportsFullText => true;

    static string FtsTableName(string tableName) => $"{tableName}_fts";

    static string FtsTokenizer(FullTextLanguage language)
        // FTS5 ships the Porter stemmer for English only; everything else tokenizes without stemming.
        => language == FullTextLanguage.English ? "porter unicode61" : "unicode61";

    // SQL that concatenates the mapped JSON paths from a trigger row alias (new/old) into one text blob.
    static string FtsTextExpression(string rowAlias, IReadOnlyList<string> jsonPaths)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < jsonPaths.Count; i++)
        {
            if (i > 0) sb.Append(" || ' ' || ");
            sb.Append($"COALESCE(json_extract({rowAlias}.Data, '$.{jsonPaths[i]}'), '')");
        }
        return sb.ToString();
    }

    public IReadOnlyList<string> BuildCreateFullTextSql(string tableName, string typeName, FullTextMapping mapping)
    {
        var fts = FtsTableName(tableName);
        var suffix = SanitizeForTableSuffix(typeName);
        var newText = FtsTextExpression("new", mapping.JsonPaths);
        var oldExists = "old.TypeName = " + Quote(typeName);
        var newMatches = "new.TypeName = " + Quote(typeName);

        return new[]
        {
            $"CREATE VIRTUAL TABLE IF NOT EXISTS {fts} USING fts5(docId UNINDEXED, typeName UNINDEXED, text, tokenize='{FtsTokenizer(mapping.Language)}');",

            $"""
            CREATE TRIGGER IF NOT EXISTS {tableName}_fts_ai_{suffix} AFTER INSERT ON {QuoteTable(tableName)}
            WHEN {newMatches} BEGIN
                INSERT INTO {fts}(rowid, docId, typeName, text) VALUES (new.rowid, new.Id, new.TypeName, {newText});
            END;
            """,

            $"""
            CREATE TRIGGER IF NOT EXISTS {tableName}_fts_ad_{suffix} AFTER DELETE ON {QuoteTable(tableName)}
            WHEN {oldExists} BEGIN
                DELETE FROM {fts} WHERE rowid = old.rowid;
            END;
            """,

            $"""
            CREATE TRIGGER IF NOT EXISTS {tableName}_fts_au_{suffix} AFTER UPDATE ON {QuoteTable(tableName)}
            WHEN {newMatches} BEGIN
                DELETE FROM {fts} WHERE rowid = old.rowid;
                INSERT INTO {fts}(rowid, docId, typeName, text) VALUES (new.rowid, new.Id, new.TypeName, {newText});
            END;
            """
        };
    }

    public bool SupportsComputedColumns => true;

    public IReadOnlyList<string> BuildCreateComputedColumnSql(string tableName, string typeName, ComputedMapping mapping, string expressionSql)
    {
        var col = mapping.ColumnName;
        var statements = new List<string>
        {
            // SQLite permits only VIRTUAL generated columns via ALTER TABLE ADD COLUMN — the value is
            // recomputed on read but is still indexable, so a sort/filter on it is index-served.
            $"ALTER TABLE {QuoteTable(tableName)} ADD COLUMN {col} AS ({expressionSql}) VIRTUAL;"
        };
        if (mapping.Indexed)
            statements.Add($"CREATE INDEX IF NOT EXISTS idx_{col} ON {QuoteTable(tableName)} ({col}) WHERE TypeName = {Quote(typeName)};");
        return statements;
    }

    // Builds an injection-safe FTS5 MATCH expression that ORs each whitespace-delimited term, quoting
    // every term as a literal so the user's text can never inject MATCH operators.
    static string BuildMatchQuery(string searchText)
    {
        var tokens = searchText.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length == 0)
            return "\"" + searchText.Trim().Replace("\"", "\"\"") + "\"";

        var sb = new StringBuilder();
        for (var i = 0; i < tokens.Length; i++)
        {
            if (i > 0) sb.Append(" OR ");
            sb.Append('"').Append(tokens[i].Replace("\"", "\"\"")).Append('"');
        }
        return sb.ToString();
    }

    public (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildFullTextSearchSql(
        string tableName, string typeName, FullTextMapping mapping,
        string searchText, int maxResults, string? additionalWhere)
    {
        var fts = FtsTableName(tableName);
        // bm25() is negative with more-relevant rows more negative; negate so higher = better, and
        // ORDER BY the raw bm25 ascending to surface the best matches first.
        var sql = $"""
            SELECT d.Data, -bm25({fts}) AS score
            FROM {fts}
            INNER JOIN {QuoteTable(tableName)} d ON d.rowid = {fts}.rowid
            WHERE {fts} MATCH @ftsQuery
              AND d.TypeName = @typeName
              {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
            ORDER BY bm25({fts})
            LIMIT {maxResults};
            """;

        return (sql, new Dictionary<string, object> { ["@ftsQuery"] = BuildMatchQuery(searchText) });
    }

    // ── Composable Lucene queries (DocumentFunctions.LuceneMatch / LuceneScore) ───────────────────────
    // FTS5 natively supports phrases, AND/OR/NOT, prefixes ("foo" *) and NEAR() proximity — but not fuzzy,
    // per-term boost, or field scoping. The query is translated to a bound MATCH expression and pushed into
    // the FTS5 virtual table via an `Id IN (subquery)` filter; score correlates back for -bm25().

    public FtCapabilities FullTextQueryCapabilities => FtCapabilities.Prefix | FtCapabilities.Proximity;

    public string? BuildFullTextMatchSql(string tableName, string typeName, FullTextMapping mapping, FtQuery query, Func<string, string> bindParam)
    {
        var fts = FtsTableName(tableName);
        var match = bindParam(new Fts5Translator().Write(query.Root));
        return $"(Id IN (SELECT docId FROM {fts} WHERE {fts} MATCH {match} AND typeName = @typeName))";
    }

    public string? BuildFullTextScoreSql(string tableName, string typeName, FullTextMapping mapping, FtQuery query, Func<string, string> bindParam)
    {
        var fts = FtsTableName(tableName);
        var match = bindParam(new Fts5Translator().Write(query.Root));
        // Correlate the FTS row back to the outer document row; bm25 is negative (more relevant = more
        // negative), so negate for higher-is-better. Non-matching rows yield NULL (sorted last). bm25() takes
        // the FTS table name itself, so the sub-table cannot be aliased.
        return $"(SELECT -bm25({fts}) FROM {fts} WHERE {fts}.docId = {QuoteTable(tableName)}.Id AND {fts}.typeName = @typeName AND {fts} MATCH {match})";
    }

    // Renders the FtQuery AST to an FTS5 MATCH expression. Every term is quoted so user text can never
    // inject MATCH operators.
    sealed class Fts5Translator : FtSqlTranslator
    {
        protected override string Term(string term) => FtsQuote(term);
        protected override string Prefix(string term) => FtsQuote(term) + " *";
        protected override string Phrase(IReadOnlyList<string> terms) => "\"" + string.Join(' ', terms.Select(t => t.Replace("\"", "\"\""))) + "\"";
        protected override string Proximity(IReadOnlyList<string> terms, int slop) => $"NEAR({string.Join(' ', terms.Select(FtsQuote))}, {slop})";
        protected override string And(IReadOnlyList<string> parts) => string.Join(" AND ", parts);
        protected override string Or(IReadOnlyList<string> parts) => string.Join(" OR ", parts);
        protected override string AndNot(string basePart, string negated) => $"({basePart}) NOT {negated}";

        static string FtsQuote(string term) => "\"" + term.Replace("\"", "\"\"") + "\"";
    }

    // Single-quoted SQL string literal for DDL embedding (trigger WHEN clauses).
    static string Quote(string value) => "'" + value.Replace("'", "''") + "'";
}
