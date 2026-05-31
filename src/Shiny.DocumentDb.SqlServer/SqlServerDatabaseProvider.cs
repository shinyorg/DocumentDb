using System.Data.Common;
using Microsoft.Data.SqlClient;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.SqlServer;

public class SqlServerDatabaseProvider : IDatabaseProvider
{
    readonly string connectionString;
    readonly SqlServerChangeFeedOptions changeFeed;

    public SqlServerDatabaseProvider(string connectionString, SqlServerChangeFeedOptions? changeFeedOptions = null)
    {
        this.connectionString = connectionString;
        this.changeFeed = changeFeedOptions ?? new SqlServerChangeFeedOptions();
    }

    public DbConnection CreateConnection() => new SqlConnection(this.connectionString);

    public Task InitializeConnectionAsync(DbConnection connection, CancellationToken ct) => Task.CompletedTask;

    public string BuildCreateTableSql(string tableName) => $"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{tableName}')
        CREATE TABLE [{tableName}] (
            Id NVARCHAR(450) NOT NULL,
            TypeName NVARCHAR(450) NOT NULL,
            Data JSON NOT NULL,
            CreatedAt DATETIME2 NOT NULL,
            UpdatedAt DATETIME2 NOT NULL,
            CONSTRAINT PK_{tableName} PRIMARY KEY (Id, TypeName)
        );
        """;

    public string BuildCreateTypenameIndexSql(string tableName)
        => $"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_{tableName}_typename') CREATE INDEX idx_{tableName}_typename ON [{tableName}] (TypeName);";

    public string BuildAddTenantColumnSql(string tableName)
        => $"IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{tableName}') AND name = 'TenantId') ALTER TABLE [{tableName}] ADD TenantId NVARCHAR(450) NULL;";

    public string BuildCreateTenantIndexSql(string tableName)
        => $"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_{tableName}_TenantId') CREATE INDEX IX_{tableName}_TenantId ON [{tableName}] (TenantId, TypeName);";

    public string BuildInsertSql(string tableName) => $"""
        INSERT INTO [{tableName}] (Id, TypeName, Data, CreatedAt, UpdatedAt)
        VALUES (@id, @typeName, @data, @now, @now);
        """;

    public string BuildUpdateSql(string tableName) => $"""
        UPDATE [{tableName}]
        SET Data = @data, UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildUpsertMergeSql(string tableName) => $$"""
        MERGE [{{tableName}}] AS target
        USING (VALUES (@id, @typeName, @data, @now)) AS source (Id, TypeName, Data, Now)
        ON target.Id = source.Id AND target.TypeName = source.TypeName
        WHEN MATCHED THEN UPDATE SET
            Data = (
                SELECT '{' + STRING_AGG(
                    '"' + STRING_ESCAPE(k, 'json') + '":' + v, ','
                ) WITHIN GROUP (ORDER BY k) + '}'
                FROM (
                    SELECT
                        COALESCE(s.[key], t.[key]) as k,
                        CASE COALESCE(s.[type], t.[type])
                            WHEN 0 THEN 'null'
                            WHEN 1 THEN '"' + STRING_ESCAPE(COALESCE(s.[value], t.[value]), 'json') + '"'
                            ELSE COALESCE(s.[value], t.[value])
                        END as v
                    FROM OPENJSON(target.Data) t
                    FULL OUTER JOIN OPENJSON(source.Data) s ON s.[key] = t.[key]
                ) AS merged
            ),
            UpdatedAt = source.Now
        WHEN NOT MATCHED THEN INSERT (Id, TypeName, Data, CreatedAt, UpdatedAt)
            VALUES (source.Id, source.TypeName, source.Data, source.Now, source.Now);
        """;

    public string BuildSetPropertySql(string tableName) => $"""
        UPDATE [{tableName}]
        SET Data.modify(@path, @value), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildRemovePropertySql(string tableName) => $"""
        UPDATE [{tableName}]
        SET Data.modify(@path, NULL), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public string BuildMaxIdSql(string tableName)
        => $"SELECT MAX(CAST(Id AS BIGINT)) FROM [{tableName}] WHERE TypeName = @typeName;";

    public string BuildCreateJsonIndexSql(string indexName, string tableName, string jsonPath, string typeName)
        => $"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{indexName}') CREATE INDEX {indexName} ON [{tableName}] (TypeName) WHERE TypeName = '{typeName}';";

    public string BuildDropIndexSql(string indexName)
        => $"DROP INDEX IF EXISTS {indexName};";

    public string BuildListJsonIndexesSql(string tableName, string prefix)
        => $"SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('{tableName}') AND name LIKE @prefix;";

    public string JsonExtract(string column, string jsonPath)
        => $"JSON_VALUE({column}, '$.{jsonPath}')";

    public string JsonExtractElement(string jsonPath)
        => $"JSON_VALUE(value, '$.{jsonPath}')";

    public string JsonExtractElementNumeric(string jsonPath)
        => $"CAST(JSON_VALUE(value, '$.{jsonPath}') AS FLOAT)";
    public string CastIntegerAggregate(string expression)
        => $"CAST({expression} AS BIGINT)";
    public string JsonExtractNumeric(string column, string jsonPath)
        => $"CAST(JSON_VALUE({column}, '$.{jsonPath}') AS FLOAT)";

    public string JsonArrayLength(string column, string jsonPath)
        => $"(SELECT COUNT(*) FROM OPENJSON({column}, '$.{jsonPath}'))";

    public string JsonEachFrom(string column, string jsonPath)
        => $"OPENJSON({column}, '$.{jsonPath}')";

    public string JsonObject(IEnumerable<string> keyValuePairs)
    {
        var pairs = keyValuePairs.ToList();
        var parts = new List<string>();
        for (var i = 0; i < pairs.Count; i += 2)
        {
            var key = pairs[i].Trim('\'');
            var value = pairs[i + 1];
            parts.Add($"'{key}':{value}");
        }
        return $"JSON_OBJECT({string.Join(", ", parts)})";
    }

    public string JsonTrue() => "CAST(1 AS BIT)";

    public string JsonFalse() => "CAST(0 AS BIT)";

    public string JsonNullCheck(string column, string jsonPath, bool isNull)
    {
        var extract = JsonExtract(column, jsonPath);
        return isNull ? $"{extract} IS NULL" : $"{extract} IS NOT NULL";
    }

    public string JsonEachPrimitiveValue => "value";
    public string JsonEachPrimitiveNumericValue => "CAST(value AS FLOAT)";
    public string QuoteTable(string tableName) => $"[{tableName}]";

    public string ConcatStrings(params string[] parts) => $"CONCAT({string.Join(", ", parts)})";

    public string BuildJsonSetExpression() => "JSON_MODIFY(Data, @path, @value)";

    public object FormatPropertyValue(object? value) => value ?? DBNull.Value;

    public string BuildPaginationClause(int offset, int take)
        => $"OFFSET {offset} ROWS FETCH NEXT {take} ROWS ONLY";

    public bool IsDuplicateKeyException(Exception ex)
        => ex is SqlException sqlEx && (sqlEx.Number == 2627 || sqlEx.Number == 2601);

    // ── Native change feed: Change Tracking (+ optional Query Notifications) ──

    public bool SupportsChangeFeed => true;

    public async Task<IAsyncDisposable> SubscribeChangesAsync(
        string tableName,
        string typeName,
        Func<RawDocumentChange, CancellationToken, Task> onChange,
        CancellationToken cancellationToken)
    {
        // Provision change tracking up front so permission/configuration errors surface here.
        await this.EnsureChangeTrackingAsync(tableName, cancellationToken).ConfigureAwait(false);
        var baseline = await this.GetCurrentVersionAsync(cancellationToken).ConfigureAwait(false);
        return new ChangeFeedSubscription(cancellationToken,
            token => this.RunPollLoopAsync(tableName, typeName, baseline, onChange, token));
    }

    async Task EnsureChangeTrackingAsync(string tableName, CancellationToken ct)
    {
        await using var conn = new SqlConnection(this.connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        await using (var dbCmd = conn.CreateCommand())
        {
            dbCmd.CommandText = """
                IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
                    ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
                """;
            await dbCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using var tableCmd = conn.CreateCommand();
        tableCmd.CommandText = $"""
            IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('[{tableName}]'))
                ALTER TABLE [{tableName}] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);
            """;
        await tableCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    async Task<long> GetCurrentVersionAsync(CancellationToken ct)
    {
        await using var conn = new SqlConnection(this.connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT ISNULL(CHANGE_TRACKING_CURRENT_VERSION(), 0);";
        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is null or DBNull ? 0L : Convert.ToInt64(result);
    }

    async Task RunPollLoopAsync(
        string tableName,
        string typeName,
        long baseline,
        Func<RawDocumentChange, CancellationToken, Task> onChange,
        CancellationToken token)
    {
        var last = baseline;
        var useNotify = this.changeFeed.UseQueryNotifications;
        using var signal = useNotify ? new SemaphoreSlim(0, 1) : null;
        var started = false;

        try
        {
            if (useNotify)
            {
                try { SqlDependency.Start(this.connectionString); started = true; }
                catch { useNotify = false; }
            }

            while (!token.IsCancellationRequested)
            {
                var current = await this.GetCurrentVersionAsync(token).ConfigureAwait(false);
                if (current > last)
                {
                    await this.ReadChangesAsync(tableName, typeName, last, onChange, token).ConfigureAwait(false);
                    last = current;
                }

                if (useNotify && signal != null)
                {
                    try { await this.ArmDependencyAsync(tableName, typeName, signal, token).ConfigureAwait(false); }
                    catch { /* fall back to interval wait this round */ }
                    try { await signal.WaitAsync(this.changeFeed.PollInterval, token).ConfigureAwait(false); }
                    catch (OperationCanceledException) { }
                }
                else
                {
                    await Task.Delay(this.changeFeed.PollInterval, token).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            if (started)
            {
                try { SqlDependency.Stop(this.connectionString); } catch { }
            }
        }
    }

    async Task ReadChangesAsync(
        string tableName,
        string typeName,
        long sinceVersion,
        Func<RawDocumentChange, CancellationToken, Task> onChange,
        CancellationToken token)
    {
        await using var conn = new SqlConnection(this.connectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT ct.Id, ct.SYS_CHANGE_OPERATION, CAST(d.Data AS NVARCHAR(MAX))
            FROM CHANGETABLE(CHANGES [{tableName}], @last) AS ct
            LEFT JOIN [{tableName}] AS d ON d.Id = ct.Id AND d.TypeName = ct.TypeName
            WHERE ct.TypeName = @typeName
            ORDER BY ct.SYS_CHANGE_VERSION;
            """;
        cmd.Parameters.AddWithValue("@last", sinceVersion);
        cmd.Parameters.AddWithValue("@typeName", typeName);

        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            var id = reader.GetString(0);
            var op = reader.GetString(1).Trim();
            var changeType = op switch
            {
                "I" => DocumentChangeType.Inserted,
                "U" => DocumentChangeType.Updated,
                "D" => DocumentChangeType.Removed,
                _ => (DocumentChangeType?)null
            };
            if (changeType is null)
                continue;

            var json = changeType == DocumentChangeType.Removed || reader.IsDBNull(2)
                ? null
                : reader.GetString(2);
            await onChange(new RawDocumentChange(changeType.Value, id, json), token).ConfigureAwait(false);
        }
    }

    async Task ArmDependencyAsync(string tableName, string typeName, SemaphoreSlim signal, CancellationToken token)
    {
        await using var conn = new SqlConnection(this.connectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        // Query Notifications require a schema-qualified name and explicit columns.
        cmd.CommandText = $"SELECT [Id], [TypeName] FROM dbo.[{tableName}] WHERE [TypeName] = @typeName;";
        cmd.Parameters.AddWithValue("@typeName", typeName);

        var dependency = new SqlDependency(cmd);
        dependency.OnChange += (_, _) =>
        {
            try { signal.Release(); } catch (SemaphoreFullException) { }
        };

        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false)) { } // drain to register the notification
    }

}
