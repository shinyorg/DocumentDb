using System.Data.Common;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Npgsql;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.PostgreSql;

public class PostgreSqlDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;

    public PostgreSqlDatabaseProvider(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public DbConnection CreateConnection() => new NpgsqlConnection(this.connectionString);

    public Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct) => Task.CompletedTask;

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
}
