using System.Data.Common;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Npgsql;
using NpgsqlTypes;
using Shiny.DocumentDb.Internal.FullText;
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

    public virtual bool SupportsSoundex => this.EnableFuzzyStringMatch;

    public virtual Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct)
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

    // ── Spatial ──
    public bool SupportsSpatial => true;

    public string? BuildSpatialDistanceSql(string? tableName, string jsonPath, string geoJsonParam, string wktParam)
        => this.PortableSpatial ? null
            : $"ST_Distance((ST_GeomFromGeoJSON((Data->'{jsonPath}')::text))::geography, (ST_GeomFromGeoJSON({geoJsonParam}))::geography)";

    /// <summary>Forces the dependency-free envelope tier (no PostGIS, no native ST_* pushdown for
    /// DocumentFunctions-in-Where). Default false → PostGIS is enabled at init and used natively.</summary>
    public bool PortableSpatial { get; init; }

    // Native pushdown stores geometry in a GiST-indexed geometry column (populated from GeoJSON on write).
    public bool RequiresSpatialGeoJson => !this.PortableSpatial;

    // Predicates run over the sidecar's GiST-indexed geom column, so the query planner uses the real 2-D
    // spatial index (ST_Intersects/Contains/… internally do the index bbox filter + exact test).
    public string? BuildSpatialFilterSql(
        string tableName, string jsonPath, string predicate,
        string geoJsonParam, string wktParam, string minLatParam, string maxLatParam, string minLngParam, string maxLngParam,
        string? metersParam)
    {
        if (this.PortableSpatial)
            return null;

        var query = $"ST_GeomFromGeoJSON({geoJsonParam})";
        var pred = predicate switch
        {
            "Intersects" => $"ST_Intersects(geom, {query})",
            "Disjoint" => $"NOT ST_Intersects(geom, {query})",
            "Contains" => $"ST_Contains(geom, {query})",
            "Within" => $"ST_Within(geom, {query})",
            "Covers" => $"ST_Covers(geom, {query})",
            "CoveredBy" => $"ST_CoveredBy(geom, {query})",
            "Touches" => $"ST_Touches(geom, {query})",
            "Crosses" => $"ST_Crosses(geom, {query})",
            "Overlaps" => $"ST_Overlaps(geom, {query})",
            "Equals" => $"ST_Equals(geom, {query})",
            "WithinDistance" => $"ST_DWithin(geom::geography, ({query})::geography, {metersParam})",
            _ => null
        };
        if (pred == null)
            return null;

        return $"Id IN (SELECT docId FROM \"{tableName}_spatial\" WHERE typeName = @typeName AND {pred})";
    }

    public virtual string? DescribeSpatialInitFailure(Exception exception)
    {
        if (this.PortableSpatial)
            return null;

        // 0A000 feature_not_supported — the extension isn't installed on the server at all (the stock
        // postgres image); 42501 insufficient_privilege — it's there but the role may not CREATE EXTENSION.
        // Anything else is a real DDL or connection failure and must surface as itself.
        if (exception is not PostgresException { SqlState: "0A000" or "42501" })
            return null;

        return "Native spatial support on PostgreSQL requires PostGIS. Use a PostGIS-enabled server (e.g. " +
            "the postgis/postgis image rather than the stock postgres image), grant the connecting role " +
            "permission to CREATE EXTENSION, or set PortableSpatial = true on PostgreSqlDatabaseProvider " +
            "(DocumentStoreSettings.PortableSpatial when wiring through Aspire) to use the dependency-free " +
            "envelope tier.";
    }

    // Native ST_* pushdown requires PostGIS; enable it at spatial-table init (fail-loud if the role can't
    // CREATE EXTENSION). PortableSpatial skips it (dedicated Geo* envelope methods need no extension).
    public virtual string? BuildCreateSpatialTablesSql(string tableName)
    {
        var geomCol = this.PortableSpatial ? "" : "geom geometry,\n            ";
        var gist = this.PortableSpatial ? "" :
            $"\nCREATE INDEX IF NOT EXISTS gist_{tableName}_spatial ON \"{tableName}_spatial\" USING GIST (geom);";
        return $"""
        {(this.PortableSpatial ? "" : "CREATE EXTENSION IF NOT EXISTS postgis;")}
        CREATE TABLE IF NOT EXISTS "{tableName}_spatial" (
            docId TEXT NOT NULL,
            typeName TEXT NOT NULL,
            minLat double precision NOT NULL, maxLat double precision NOT NULL,
            minLng double precision NOT NULL, maxLng double precision NOT NULL,
            {geomCol}PRIMARY KEY (docId, typeName)
        );
        CREATE INDEX IF NOT EXISTS idx_{tableName}_spatial ON "{tableName}_spatial" (typeName, minLat, maxLat, minLng, maxLng);{gist}
        """;
    }

    public string? BuildSpatialUpsertSql(string tableName)
    {
        var geomIns = this.PortableSpatial ? "" : ", geom";
        var geomVal = this.PortableSpatial ? "" : ", ST_GeomFromGeoJSON(@spatialGeoJson)";
        var geomUpd = this.PortableSpatial ? "" : ", geom = ST_GeomFromGeoJSON(@spatialGeoJson)";
        return $"""
        INSERT INTO "{tableName}_spatial" (docId, typeName, minLat, maxLat, minLng, maxLng{geomIns})
        VALUES (@spatialDocId, @spatialTypeName, @spatialMinLat, @spatialMaxLat, @spatialMinLng, @spatialMaxLng{geomVal})
        ON CONFLICT (docId, typeName) DO UPDATE SET
            minLat = EXCLUDED.minLat, maxLat = EXCLUDED.maxLat, minLng = EXCLUDED.minLng, maxLng = EXCLUDED.maxLng{geomUpd};
        """;
    }

    public string? BuildSpatialDeleteSql(string tableName)
        => $"DELETE FROM \"{tableName}_spatial\" WHERE docId = @spatialDocId AND typeName = @spatialTypeName;";

    public string? BuildSpatialClearSql(string tableName)
        => $"DELETE FROM \"{tableName}_spatial\" WHERE typeName = @typeName;";

    public string? BuildSpatialBoundingBoxQuerySql(string tableName, string? additionalWhere) => $"""
        SELECT d.Data::text FROM "{tableName}" d
        INNER JOIN "{tableName}_spatial" r ON r.docId = d.Id AND r.typeName = d.TypeName
        WHERE d.TypeName = @typeName
          AND r.maxLat >= @minLat AND r.minLat <= @maxLat
          AND r.maxLng >= @minLng AND r.minLng <= @maxLng
          {(additionalWhere != null ? $"AND ({additionalWhere})" : "")}
        """;

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
            TenantId TEXT NULL,
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

    // CockroachDB inherits both modes unchanged — it is wire-compatible and supports FOR UPDATE / FOR SHARE.
    public virtual string BuildLockClause(LockMode mode) => mode switch
    {
        LockMode.Update => " FOR UPDATE",
        LockMode.Share => " FOR SHARE",
        _ => string.Empty
    };

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

    // pg_stat_user_indexes.idx_scan is the count since the last stats reset, which is what makes
    // "this index has never been used" answerable here and not on most other engines.
    public virtual string BuildListAllIndexesSql(string tableName)
        => $"""
            SELECT i.indexname,
                   i.indexdef,
                   pg_relation_size(c.oid),
                   s.idx_scan
            FROM pg_indexes i
            LEFT JOIN pg_class c ON c.relname = i.indexname
            LEFT JOIN pg_stat_user_indexes s ON s.indexrelname = i.indexname AND s.relname = i.tablename
            WHERE i.tablename = '{tableName.ToLowerInvariant()}'
            ORDER BY i.indexname;
            """;

    public virtual IReadOnlyList<string> BuildExplainSql(string sql) => ["EXPLAIN " + sql];

    static string BuildPgPath(string jsonPath)
    {
        var parts = jsonPath.Split('.');
        return "'{" + string.Join(",", parts) + "}'";
    }

    // Parenthesized so the `#>>` extraction binds as a single value: PostgreSQL gives `#>>` and `||`
    // (string concat) equal precedence, so an unparenthesized extraction inside a concat would mis-parse
    // (`a #>> p1 || b #>> p2` → `((a #>> p1 || b) #>> p2)`).
    static string BuildPgJsonExtract(string column, string jsonPath)
        => $"({column} #>> {BuildPgPath(jsonPath)})";

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

    public string BuildJsonSetExpression(string sourceExpression, string pathParameter, string valueParameter)
        => $"jsonb_set({sourceExpression}, string_to_array(REPLACE({pathParameter}, '$.', ''), '.')::text[], CAST({valueParameter} AS JSONB))";

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

    // Casts the bound JSON parameter to JSONB — used by the interface-default backup Insert SQL so the
    // timestamp-preserving restore path gets the same cast as BuildBatchInsertSql above.
    public string JsonInsertValueExpr(string dataParam) => $"CAST({dataParam} AS JSONB)";

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

    public virtual bool SupportsBulkCopy => true;

    public virtual async Task<long> BulkCopyInsertAsync(
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

    public virtual bool SupportsChangeFeed => true;

    public virtual async Task<IAsyncDisposable> SubscribeChangesAsync(
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
            CREATE OR REPLACE TRIGGER {trg} AFTER INSERT OR UPDATE OR DELETE ON {table}
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

        // If the user's handler throws, tear the whole subscription down instead of letting the consumer die
        // silently while the producer keeps writing to an unbounded queue (a memory leak for the life of the
        // now-dead subscription).
        using var faultCts = CancellationTokenSource.CreateLinkedTokenSource(token);
        var loopToken = faultCts.Token;

        var consumer = Task.Run(async () =>
        {
            try
            {
                await foreach (var raw in queue.Reader.ReadAllAsync(loopToken).ConfigureAwait(false))
                    await onChange(raw, loopToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
            catch { faultCts.Cancel(); }
        }, loopToken);

        try
        {
            while (!loopToken.IsCancellationRequested)
                await conn.WaitAsync(loopToken).ConfigureAwait(false);
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

    public virtual bool SupportsVector => true;

    public virtual Task LoadVectorExtensionAsync(DbConnection connection, CancellationToken ct)
        => RunStatement(connection, "CREATE EXTENSION IF NOT EXISTS vector;", ct);

    static async Task RunStatement(DbConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    // Cast because VectorTableName is a default interface member: the convention lives on
    // IDatabaseProvider so a provider can override it and a tool can read it, but reaching it from the
    // implementing class means going through the interface.
    protected string VecTable(string tableName, string typeName)
        => $"\"{((IDatabaseProvider)this).VectorTableName(tableName, typeName)}\"";

    static string OpClass(VectorDistance metric) => metric switch
    {
        VectorDistance.Cosine => "vector_cosine_ops",
        VectorDistance.Euclidean => "vector_l2_ops",
        VectorDistance.DotProduct => "vector_ip_ops",
        // pgvector's Hamming distance requires a bit(n) column, but embeddings are stored as vector(n) — so
        // Hamming can't be expressed here. Reject it cleanly rather than emit DDL that fails on the server.
        VectorDistance.Hamming => throw new NotSupportedException(
            "pgvector Hamming distance requires a bit(n) column; float embeddings stored as vector(n) support Cosine, Euclidean and DotProduct only."),
        _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, null)
    };

    protected static string DistanceOperator(VectorDistance metric) => metric switch
    {
        VectorDistance.Cosine => "<=>",
        VectorDistance.Euclidean => "<->",
        VectorDistance.DotProduct => "<#>",
        VectorDistance.Hamming => throw new NotSupportedException(
            "pgvector Hamming distance requires a bit(n) column; float embeddings support Cosine, Euclidean and DotProduct only."),
        _ => throw new ArgumentOutOfRangeException(nameof(metric), metric, null)
    };

    public virtual string BuildCreateVectorTablesSql(string tableName, string typeName, VectorMapping mapping)
    {
        var vec = VecTable(tableName, typeName);
        var idx = $"idx_{((IDatabaseProvider)this).VectorTableName(tableName, typeName)}";
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

    public string BuildVectorDocIdsSql(string tableName, string typeName)
        => $"SELECT docId FROM {VecTable(tableName, typeName)} WHERE typeName = @vecTypeName;";

    public virtual (string Sql, IReadOnlyDictionary<string, object> Parameters) BuildVectorSearchSql(
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

    public bool SupportsComputedColumns => true;

    public IReadOnlyList<string> BuildCreateComputedColumnSql(string tableName, string typeName, ComputedMapping mapping, string expressionSql)
    {
        var col = mapping.ColumnName;
        var statements = new List<string>
        {
            // PostgreSQL generated columns are STORED only; the type is declared (aligned with the cast the
            // expression already applies). The immutable JSON expression is recomputed on write.
            $"ALTER TABLE \"{tableName}\" ADD COLUMN IF NOT EXISTS {col} {PgComputedType(mapping.ResultType)} GENERATED ALWAYS AS ({expressionSql}) STORED;"
        };
        if (mapping.Indexed)
            statements.Add($"CREATE INDEX IF NOT EXISTS idx_{col} ON \"{tableName}\" ({col}) WHERE TypeName = '{typeName}';");
        return statements;
    }

    static string PgComputedType(Type clrType)
    {
        var t = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (t.IsEnum) t = Enum.GetUnderlyingType(t);
        if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte)) return "BIGINT";
        if (t == typeof(double) || t == typeof(float)) return "DOUBLE PRECISION";
        if (t == typeof(decimal)) return "NUMERIC";
        if (t == typeof(bool)) return "BOOLEAN";
        return "TEXT";
    }

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

    static string FtsColumn(string typeName) => "fts_" + IDatabaseProvider.SanitizeTypeSuffix(typeName).ToLowerInvariant();

    public string BuildFullTextProbeSql(string tableName, string typeName)
        => $"SELECT 1 FROM information_schema.columns " +
           $"WHERE table_name = '{tableName.ToLowerInvariant()}' AND column_name = '{FtsColumn(typeName)}';";

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
        var idx = $"idx_fts_{tableName}_{IDatabaseProvider.SanitizeTypeSuffix(typeName).ToLowerInvariant()}";
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

    // ── Composable Lucene queries (DocumentFunctions.LuceneMatch / LuceneScore) ───────────────────────
    // tsquery supports terms, phrases (<->), AND/OR/NOT (& | !), grouping and prefixes (:*) — but not
    // fuzzy, per-term boost, or a true within-N proximity, so only Prefix is advertised.

    public FtCapabilities FullTextQueryCapabilities => FtCapabilities.Prefix;

    public string? BuildFullTextMatchSql(string tableName, string typeName, FullTextMapping mapping, FtQuery query, Func<string, string> bindParam)
    {
        var col = FtsColumn(typeName);
        var p = bindParam(new PgTsQueryTranslator().Write(query.Root));
        return $"({col} @@ to_tsquery('{PgRegConfig(mapping.Language)}', {p}))";
    }

    public string? BuildFullTextScoreSql(string tableName, string typeName, FullTextMapping mapping, FtQuery query, Func<string, string> bindParam)
    {
        var col = FtsColumn(typeName);
        var p = bindParam(new PgTsQueryTranslator().Write(query.Root));
        return $"ts_rank({col}, to_tsquery('{PgRegConfig(mapping.Language)}', {p}))";
    }

    // Renders the AST to a to_tsquery string. Each lexeme is single-quoted so user text can never inject
    // tsquery operators (& | ! : *).
    sealed class PgTsQueryTranslator : FtSqlTranslator
    {
        protected override string Term(string term) => Lexeme(term);
        protected override string Prefix(string term) => Lexeme(term) + ":*";
        protected override string Phrase(IReadOnlyList<string> terms) => string.Join(" <-> ", terms.Select(Lexeme));
        protected override string And(IReadOnlyList<string> parts) => string.Join(" & ", parts);
        protected override string Or(IReadOnlyList<string> parts) => string.Join(" | ", parts);
        protected override string AndNot(string basePart, string negated) => $"{basePart} & !{negated}";

        static string Lexeme(string term) => "'" + term.Replace("'", "''") + "'";
    }

    // ── Blobs ──────────────────────────────────────────────────────────
    // Portable DML applies: PostgreSQL is the origin dialect for ON CONFLICT … DO UPDATE.

    /// <summary>A bytea field is capped at 1 GB.</summary>
    public virtual long MaxBlobSize => 1024L * 1024 * 1024;

    public virtual string BuildCreateBlobTableSql(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS "{tableName}_blobs" (
            Id TEXT NOT NULL,
            TypeName TEXT NOT NULL,
            BlobKey TEXT NOT NULL,
            Data BYTEA NOT NULL,
            Length BIGINT NOT NULL,
            ContentType TEXT NULL,
            FileName TEXT NULL,
            Hash TEXT NULL,
            CreatedAt TIMESTAMPTZ NOT NULL,
            UpdatedAt TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (Id, TypeName, BlobKey)
        );
        """;

}
