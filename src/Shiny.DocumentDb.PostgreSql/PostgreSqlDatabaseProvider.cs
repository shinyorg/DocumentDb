using System.Data.Common;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Npgsql;
using NpgsqlTypes;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.PostgreSql;

public class PostgreSqlDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;

    public PostgreSqlDatabaseProvider(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public string ConnectionString => this.connectionString;

    public DbConnection CreateConnection() => new NpgsqlConnection(this.connectionString);

    /// <summary>
    /// When true, provisions the <c>fuzzystrmatch</c> extension on each connection so
    /// <c>DocumentFunctions.Soundex(...)</c> translates to PostgreSQL's <c>soundex()</c>. Requires the
    /// connecting role to have privileges to <c>CREATE EXTENSION</c> (run once by an admin otherwise).
    /// </summary>
    public bool EnableFuzzyStringMatch { get; init; }

    public bool SupportsSoundex => this.EnableFuzzyStringMatch;

    public Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
        => this.EnableFuzzyStringMatch
            ? RunStatement(connection, "CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;", ct)
            : Task.CompletedTask;

    public string BuildCreateTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS "{tableName}" (
            Id TEXT NOT NULL,
            TypeName TEXT NOT NULL,
            Data JSONB NOT NULL,
            CreatedAt TIMESTAMPTZ NOT NULL,
            UpdatedAt TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (Id, TypeName)
        );
        """;

    public string BuildCreateTypenameIndexSql(string tableName)
        => $"CREATE INDEX IF NOT EXISTS idx_{tableName}_typename ON \"{tableName}\" (TypeName);";

    // ── Temporal (system-time history sidecar) ──────────────────────────
    // Portable DML defaults apply; the JSONB payload needs an explicit cast on insert.

    public bool SupportsTemporal => true;

    public string BuildCreateHistoryTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS "{tableName}_history" (
            Id TEXT NOT NULL,
            TypeName TEXT NOT NULL,
            Version BIGINT NOT NULL,
            ValidFrom TIMESTAMPTZ NOT NULL,
            ValidTo TIMESTAMPTZ NULL,
            Operation TEXT NOT NULL,
            Actor TEXT NULL,
            Data JSONB NULL,
            PRIMARY KEY (Id, TypeName, Version)
        );
        """;

    public string BuildHistoryInsertSql(string tableName)
        => $"INSERT INTO \"{tableName}_history\" (Id, TypeName, Version, ValidFrom, ValidTo, Operation, Actor, Data) " +
           "SELECT @id, @typeName, COALESCE(MAX(Version), 0) + 1, @validFrom, NULL, @operation, @actor, CAST(@data AS JSONB) " +
           $"FROM \"{tableName}_history\" WHERE Id = @id AND TypeName = @typeName";

    public string BuildInsertSql(string tableName) => $"""
        INSERT INTO "{tableName}" (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, CAST(@data AS JSONB), @now, @now);
        """;

    public string BuildUpdateSql(string tableName) => $"""
        UPDATE "{tableName}"
        SET Data = CAST(@data AS JSONB), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public bool SupportsJsonMergePatch => false;

    public string BuildSelectDataForUpdateSql(string tableName)
        => $"SELECT Data::text FROM \"{tableName}\" WHERE Id = @id AND TypeName = @typeName FOR UPDATE";

    public string BuildUpsertMergeSql(string tableName)
        => throw new NotSupportedException(
            "PostgreSQL's jsonb concat operator is a shallow merge, not RFC 7396 deep merge. " +
            "DocumentStore uses the read-merge-write fallback when SupportsJsonMergePatch is false.");

    public string BuildSetPropertySql(string tableName) => $"""
        UPDATE "{tableName}"
        SET Data = jsonb_set(Data, string_to_array(REPLACE(@path, '$.', ''), '.')::text[], CAST(@value AS JSONB)), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildRemovePropertySql(string tableName) => $"""
        UPDATE "{tableName}"
        SET Data = Data #- string_to_array(REPLACE(@path, '$.', ''), '.')::text[], UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildMaxIdSql(string tableName)
        => $"SELECT MAX(CAST(Id AS BIGINT)) FROM \"{tableName}\" WHERE TypeName = @typeName;";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName)
        => $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{tableName}\" (({BuildPgJsonExtract("Data", jsonPath)})) WHERE TypeName = '{typeName}';";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, IReadOnlyList<string> jsonPaths, string typeName)
    {
        if (jsonPaths.Count == 1)
            return this.BuildCreateJsonIndexSql(indexName, tableName, jsonPaths[0], typeName);
        var exprs = string.Join(", ", jsonPaths.Select(p => $"({BuildPgJsonExtract("Data", p)})"));
        return $"CREATE INDEX IF NOT EXISTS {indexName} ON \"{tableName}\" ({exprs}) WHERE TypeName = '{typeName}';";
    }

    public string BuildDropIndexSql(string indexName, string tableName)
        => $"DROP INDEX IF EXISTS {indexName};";

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT indexname FROM pg_indexes WHERE tablename = '{tableName.ToLowerInvariant()}' AND indexname LIKE @prefix;";

    static string BuildPgPath(string jsonPath)
    {
        var parts = jsonPath.Split('.');
        return "'{" + string.Join(",", parts) + "}'";
    }

    static string BuildPgJsonExtract(string column, string jsonPath)
        => $"{column} #>> {BuildPgPath(jsonPath)}";

    public string JsonExtract(string column, string jsonPath)
        => BuildPgJsonExtract(column, jsonPath);

    public string JsonExtractTyped(string column, string jsonPath, Type clrType)
    {
        var extract = BuildPgJsonExtract(column, jsonPath);
        var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (t.IsEnum) t = Enum.GetUnderlyingType(t); // enums are stored as their numeric value
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte))
            return $"({extract})::BIGINT";
        if (t == typeof(double) || t == typeof(float))
            return $"({extract})::DOUBLE PRECISION";
        if (t == typeof(decimal))
            return $"({extract})::NUMERIC";
        if (t == typeof(bool))
            return $"({extract})::BOOLEAN";
        return extract;
    }

    public string JsonExtractElement(string jsonPath)
        => BuildPgJsonExtract("value", jsonPath);

    public string JsonExtractElementTyped(string jsonPath, Type clrType)
    {
        var extract = BuildPgJsonExtract("value", jsonPath);
        var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (t.IsEnum) t = Enum.GetUnderlyingType(t); // enums are stored as their numeric value
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte))
            return $"({extract})::BIGINT";
        if (t == typeof(double) || t == typeof(float))
            return $"({extract})::DOUBLE PRECISION";
        if (t == typeof(decimal))
            return $"({extract})::NUMERIC";
        if (t == typeof(bool))
            return $"({extract})::BOOLEAN";
        return extract;
    }

    public string JsonExtractElementNumeric(string jsonPath)
        => $"CAST({BuildPgJsonExtract("value", jsonPath)} AS DOUBLE PRECISION)";
    public string CastIntegerAggregate(string expression)
        => expression;
    public string JsonExtractNumeric(string column, string jsonPath)
        => $"CAST({BuildPgJsonExtract(column, jsonPath)} AS DOUBLE PRECISION)";

    public string JsonArrayLength(string column, string jsonPath)
        => $"jsonb_array_length({column} #> {BuildPgPath(jsonPath)})";

    public string JsonEachFrom(string column, string jsonPath)
        => $"jsonb_array_elements({column} #> {BuildPgPath(jsonPath)}) AS value";

    public string JsonObject(IEnumerable<string> keyValuePairs)
        => $"jsonb_build_object({string.Join(", ", keyValuePairs)})";

    public string JsonTrue() => "'true'::jsonb";

    public string JsonFalse() => "'false'::jsonb";

    public string JsonNullCheck(string column, string jsonPath, bool isNull)
    {
        var extract = BuildPgJsonExtract(column, jsonPath);
        return isNull ? $"{extract} IS NULL" : $"{extract} IS NOT NULL";
    }

    public string JsonEachPrimitiveValue => "value #>> '{}'";
    public string JsonEachPrimitiveNumericValue => "CAST(value #>> '{}' AS DOUBLE PRECISION)";
    public string QuoteTable(string tableName) => $"\"{tableName}\"";

    public string ConcatStrings(params string[] parts) => string.Join(" || ", parts);

    public string TranslateScalar(ScalarFn fn, IReadOnlyList<string> args, Type resultType) => fn switch
    {
        // PostgreSQL has no INSTR; STRPOS is the 1-based equivalent (0 when not found → -1, matching .NET).
        ScalarFn.IndexOf => $"(STRPOS({args[0]}, {args[1]}) - 1)",
        _ => Internal.Query.ScalarSqlDefaults.Translate(this, fn, args, resultType)
    };

    public string BuildJsonSetExpression()
        => "jsonb_set(Data, string_to_array(REPLACE(@path, '$.', ''), '.')::text[], CAST(@value AS JSONB))";

    public object FormatPropertyValue(object? value) => DocumentStore.ToJsonLiteral(value);

    public string BuildPaginationClause(int offset, int take)
        => $"LIMIT {take} OFFSET {offset}";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is PostgresException pgEx && pgEx.SqlState == "23505";

    public string BuildBatchInsertSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO \"{tableName}\" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, CAST(@data_{i} AS JSONB), @now, @now)");
        }
        sb.Append(';');
        return sb.ToString();
    }

    // ── Bulk import collision modes ──────────────────────────────────────
    // Same JSONB cast on @data_i as BuildBatchInsertSql; PostgreSQL supports ANSI ON CONFLICT.

    public string BuildBatchReplaceSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO \"{tableName}\" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, CAST(@data_{i} AS JSONB), @now, @now)");
        }
        sb.Append(" ON CONFLICT (Id, TypeName) DO UPDATE SET Data = EXCLUDED.Data, UpdatedAt = @now;");
        return sb.ToString();
    }

    public string BuildBatchSkipExistingSql(string tableName, int batchSize)
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO \"{tableName}\" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES ");
        for (var i = 0; i < batchSize; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"(@id_{i}, @typeName, CAST(@data_{i} AS JSONB), @now, @now)");
        }
        sb.Append(" ON CONFLICT (Id, TypeName) DO NOTHING;");
        return sb.ToString();
    }

    // ── Native binary COPY bulk insert ───────────────────────────────────
    // 10-100× faster than multi-row INSERT for BulkWriteMode.Insert. Column types match
    // BuildCreateTableSql: Id/TypeName TEXT, Data JSONB, CreatedAt/UpdatedAt TIMESTAMPTZ.

    public bool SupportsBulkCopy => true;

    public async Task<long> BulkCopyInsertAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string tableName,
        string typeName,
        IReadOnlyList<RawBulkRow> rows,
        DateTimeOffset timestamp,
        CancellationToken cancellationToken)
    {
        if (connection is not NpgsqlConnection npgsql)
            throw new InvalidOperationException(
                $"PostgreSQL bulk copy requires an {nameof(NpgsqlConnection)} but received {connection.GetType().Name}.");

        // Npgsql binary import enlists in the connection's current transaction implicitly, so the
        // `transaction` argument needs no explicit wiring here.
        await using var importer = await npgsql.BeginBinaryImportAsync(
            $"COPY {QuoteTable(tableName)} (Id, TypeName, Data, CreatedAt, UpdatedAt) FROM STDIN (FORMAT BINARY)",
            cancellationToken).ConfigureAwait(false);

        foreach (var row in rows)
        {
            await importer.StartRowAsync(cancellationToken).ConfigureAwait(false);
            await importer.WriteAsync(row.Id, NpgsqlDbType.Text, cancellationToken).ConfigureAwait(false);
            await importer.WriteAsync(typeName, NpgsqlDbType.Text, cancellationToken).ConfigureAwait(false);
            await importer.WriteAsync(row.Data, NpgsqlDbType.Jsonb, cancellationToken).ConfigureAwait(false);
            await importer.WriteAsync(timestamp, NpgsqlDbType.TimestampTz, cancellationToken).ConfigureAwait(false);
            await importer.WriteAsync(timestamp, NpgsqlDbType.TimestampTz, cancellationToken).ConfigureAwait(false);
        }

        var count = await importer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        return (long)count;
    }

    // ── Native change feed: LISTEN/NOTIFY via row-level triggers ───────────

    public bool SupportsChangeFeed => true;

    public async Task<IAsyncDisposable> SubscribeChangesAsync(
        string tableName,
        string typeName,
        Func<RawDocumentChange, CancellationToken, Task> onChange,
        CancellationToken cancellationToken)
    {
        // Provision the trigger/function up front so connection or permission errors surface here.
        await this.EnsureChangeFeedTriggerAsync(tableName, cancellationToken).ConfigureAwait(false);
        var channel = ChannelName(tableName);
        return new ChangeFeedSubscription(cancellationToken, token => this.RunListenerAsync(channel, typeName, onChange, token));
    }

    static string ChannelName(string tableName) => $"ddb_{tableName}";
    static string QuoteIdent(string id) => "\"" + id.Replace("\"", "\"\"") + "\"";
    static string QuoteLiteral(string s) => "'" + s.Replace("'", "''") + "'";

    async Task EnsureChangeFeedTriggerAsync(string tableName, CancellationToken ct)
    {
        var channel = ChannelName(tableName);
        var fn = QuoteIdent($"ddb_fn_{tableName}");
        var trg = QuoteIdent($"ddb_trg_{tableName}");
        var table = QuoteIdent(tableName);
        var sql = $"""
            CREATE OR REPLACE FUNCTION {fn}() RETURNS trigger LANGUAGE plpgsql AS $fn$
            BEGIN
                IF (TG_OP = 'DELETE') THEN
                    PERFORM pg_notify({QuoteLiteral(channel)}, json_build_object('op', TG_OP, 'id', OLD.id, 'type', OLD.typename)::text);
                ELSE
                    PERFORM pg_notify({QuoteLiteral(channel)}, json_build_object('op', TG_OP, 'id', NEW.id, 'type', NEW.typename)::text);
                END IF;
                RETURN NULL;
            END;
            $fn$;
            DROP TRIGGER IF EXISTS {trg} ON {table};
            CREATE TRIGGER {trg} AFTER INSERT OR UPDATE OR DELETE ON {table}
                FOR EACH ROW EXECUTE FUNCTION {fn}();
            """;

        await using var conn = new NpgsqlConnection(this.connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    async Task RunListenerAsync(
        string channel,
        string typeName,
        Func<RawDocumentChange, CancellationToken, Task> onChange,
        CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this.connectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"LISTEN {QuoteIdent(channel)};";
            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        // Decouple the synchronous notification callback from the async user handler so handler
        // execution can't block the connection's notification pump, and ordering is preserved.
        var queue = Channel.CreateUnbounded<RawDocumentChange>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

        void Handler(object? sender, NpgsqlNotificationEventArgs e)
        {
            if (e.Channel == channel && TryParsePayload(e.Payload, typeName, out var raw))
                queue.Writer.TryWrite(raw);
        }
        conn.Notification += Handler;

        var consumer = Task.Run(async () =>
        {
            try
            {
                await foreach (var raw in queue.Reader.ReadAllAsync(token).ConfigureAwait(false))
                    await onChange(raw, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }, token);

        try
        {
            while (!token.IsCancellationRequested)
                await conn.WaitAsync(token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        finally
        {
            conn.Notification -= Handler;
            queue.Writer.TryComplete();
            try { await consumer.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }
    }

    // ── Vector (pgvector) ────────────────────────────────────────────────

    public bool SupportsVector => true;

    public Task LoadVectorExtensionAsync(DbConnection connection, CancellationToken ct)
        => RunStatement(connection, "CREATE EXTENSION IF NOT EXISTS vector;", ct);

    static async Task RunStatement(DbConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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

    static string OpClass(VectorDistance metric) => metric switch
    {
        VectorDistance.Cosine => "vector_cosine_ops",
        VectorDistance.Euclidean => "vector_l2_ops",
        VectorDistance.DotProduct => "vector_ip_ops",
        VectorDistance.Hamming => "bit_hamming_ops",
        _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, null)
    };

    static string DistanceOperator(VectorDistance metric) => metric switch
    {
        VectorDistance.Cosine => "<=>",
        VectorDistance.Euclidean => "<->",
        VectorDistance.DotProduct => "<#>",
        VectorDistance.Hamming => "<+>",
        _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, null)
    };

    public string BuildCreateVectorTablesSql(string tableName, string typeName, VectorMapping mapping)
    {
        var vec = VecTable(tableName, typeName);
        var idx = $"idx_{tableName}_vec_{SanitizeForTableSuffix(typeName)}";
        var sb = new StringBuilder();
        sb.Append($"""
            CREATE TABLE IF NOT EXISTS {vec} (
                docId TEXT PRIMARY KEY,
                typeName TEXT NOT NULL,
                embedding vector({mapping.Dimensions}) NOT NULL
            );

            """);

        if (mapping.IndexKind == VectorIndexKind.Hnsw)
        {
            var m = mapping.IndexOptions.HnswM ?? 16;
            var ef = mapping.IndexOptions.HnswEfConstruction ?? 64;
            sb.Append($"CREATE INDEX IF NOT EXISTS {idx} ON {vec} USING hnsw (embedding {OpClass(mapping.Metric)}) WITH (m = {m}, ef_construction = {ef});");
        }
        else if (mapping.IndexKind == VectorIndexKind.Ivf)
        {
            var lists = mapping.IndexOptions.IvfLists ?? 100;
            sb.Append($"CREATE INDEX IF NOT EXISTS {idx} ON {vec} USING ivfflat (embedding {OpClass(mapping.Metric)}) WITH (lists = {lists});");
        }
        // None / Flat / DiskAnn / QuantizedFlat → no index DDL, sequential scan.

        return sb.ToString();
    }

    public string BuildVectorUpsertSql(string tableName, string typeName, VectorMapping mapping)
    {
        var vec = VecTable(tableName, typeName);
        return $"""
            INSERT INTO {vec} (docId, typeName, embedding)
            VALUES (@vecDocId, @vecTypeName, CAST(@embedding AS vector))
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
        var op = DistanceOperator(mapping.Metric);
        var efSearch = mapping.IndexOptions.HnswEfSearch;
        var prelude = efSearch.HasValue ? $"SET LOCAL hnsw.ef_search = {efSearch.Value}; " : "";

        var sql = $"""
            {prelude}
            SELECT d.Data::text, (v.embedding {op} CAST(@embedding AS vector)) AS score
            FROM {vec} v
            INNER JOIN "{tableName}" d ON d.Id = v.docId AND d.TypeName = @typeName
            {(additionalWhere != null ? $"WHERE {additionalWhere}" : "")}
            ORDER BY v.embedding {op} CAST(@embedding AS vector)
            LIMIT {k};
            """;

        return (sql, new Dictionary<string, object> { ["@embedding"] = FormatVectorParameter(query, mapping) });
    }

    public object FormatVectorParameter(ReadOnlyMemory<float> vector, VectorMapping mapping)
    {
        // pgvector accepts vector literal as text: '[1,2,3]' cast to vector.
        var sb = new StringBuilder(vector.Length * 12);
        sb.Append('[');
        var span = vector.Span;
        for (var i = 0; i < span.Length; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(span[i].ToString("R", System.Globalization.CultureInfo.InvariantCulture));
        }
        sb.Append(']');
        return sb.ToString();
    }

    static bool TryParsePayload(string? payload, string typeName, out RawDocumentChange change)
    {
        change = default;
        if (string.IsNullOrEmpty(payload))
            return false;

        using var doc = JsonDocument.Parse(payload);
        var root = doc.RootElement;
        if (root.GetProperty("type").GetString() != typeName)
            return false;

        var id = root.GetProperty("id").GetString() ?? "";
        var changeType = root.GetProperty("op").GetString() switch
        {
            "INSERT" => DocumentChangeType.Inserted,
            "UPDATE" => DocumentChangeType.Updated,
            "DELETE" => DocumentChangeType.Removed,
            _ => (DocumentChangeType?)null
        };
        if (changeType is null)
            return false;

        change = new RawDocumentChange(changeType.Value, id, null);
        return true;
    }

    // ── Full-text search (generated tsvector column + GIN index) ──────────

    public bool SupportsFullText => true;

    static string PgRegConfig(FullTextLanguage language) => language switch
    {
        FullTextLanguage.Simple => "simple",
        FullTextLanguage.English => "english",
        FullTextLanguage.Spanish => "spanish",
        FullTextLanguage.French => "french",
        FullTextLanguage.German => "german",
        FullTextLanguage.Italian => "italian",
        FullTextLanguage.Portuguese => "portuguese",
        FullTextLanguage.Dutch => "dutch",
        FullTextLanguage.Russian => "russian",
        _ => "english"
    };

    static string FtsColumn(string typeName) => "fts_" + FullTextMappingFactory.SanitizeSuffix(typeName).ToLowerInvariant();

    static string PgTsVectorExpression(string config, IReadOnlyList<string> jsonPaths)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < jsonPaths.Count; i++)
        {
            if (i > 0) sb.Append(" || ' ' || ");
            sb.Append($"coalesce(Data #>> {BuildPgPath(jsonPaths[i])}, '')");
        }
        return $"to_tsvector('{config}', {sb})";
    }

    public IReadOnlyList<string> BuildCreateFullTextSql(string tableName, string typeName, FullTextMapping mapping)
    {
        var col = FtsColumn(typeName);
        var idx = $"idx_fts_{tableName}_{FullTextMappingFactory.SanitizeSuffix(typeName).ToLowerInvariant()}";
        var expr = PgTsVectorExpression(PgRegConfig(mapping.Language), mapping.JsonPaths);
        return new[]
        {
            $"ALTER TABLE \"{tableName}\" ADD COLUMN IF NOT EXISTS {col} tsvector GENERATED ALWAYS AS ({expr}) STORED;",
            $"CREATE INDEX IF NOT EXISTS {idx} ON \"{tableName}\" USING GIN ({col});"
        };
    }

    public (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildFullTextSearchSql(
        string tableName, string typeName, FullTextMapping mapping,
        string searchText, int maxResults, string? additionalWhere)
    {
        var col = FtsColumn(typeName);
        var tsQuery = $"to_tsquery('{PgRegConfig(mapping.Language)}', @ftsQuery)";
        var sql = $"""
            SELECT d.Data, ts_rank(d.{col}, {tsQuery}) AS score
            FROM "{tableName}" d
            WHERE d.TypeName = @typeName AND d.{col} @@ {tsQuery}
              {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
            ORDER BY score DESC
            LIMIT {maxResults};
            """;

        // Tokens are alphanumeric → safe to OR-join straight into to_tsquery.
        var query = string.Join(" | ", FullTextMappingFactory.Tokenize(searchText));
        return (sql, new Dictionary<string, object> { ["@ftsQuery"] = query });
    }
}
