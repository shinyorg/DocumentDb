using System.Data.Common;
using System.Globalization;

namespace Shiny.DocumentDb.Aspire.Hosting.Internal;

/// <summary>
/// The "has this store already been seeded" marker, kept <b>inside the database it describes</b>.
/// That is the whole point: destroy the data (drop the container volume, delete the SQLite file) and
/// the marker goes with it, so the next AppHost start treats the database as brand new and seeds again.
/// An AppHost-side state file could not tell the difference.
/// </summary>
/// <remarks>
/// This talks raw ADO rather than going through <c>IDocumentStore</c> — the AppHost has a connection
/// string and a <see cref="DocumentProviderKind"/>, not a configured store, and the marker must be
/// readable before any DocumentDb table exists.
/// </remarks>
internal sealed class SeedMarkerStore(DocumentProviderKind kind, string connectionString)
{
    internal const string TableName = "__shiny_documentdb_aspire_seed";

    /// <summary>
    /// Returns the UTC timestamp of the last completed seed for <paramref name="storeName"/>, or
    /// <c>null</c> when this store has never been seeded against this database.
    /// </summary>
    public async Task<string?> ReadMarkerAsync(string storeName, CancellationToken ct)
    {
        await using var connection = this.Open();
        await connection.OpenAsync(ct);
        await ExecuteAsync(connection, this.CreateTableSql(), ct);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT seeded_at_utc FROM {TableName} WHERE store_name = @store_name";
        AddParameter(cmd, "@store_name", storeName);

        var value = await cmd.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>Records a completed seed. Called only after the callback succeeds.</summary>
    public async Task WriteMarkerAsync(string storeName, DateTimeOffset seededAtUtc, CancellationToken ct)
    {
        await using var connection = this.Open();
        await connection.OpenAsync(ct);
        await ExecuteAsync(connection, this.CreateTableSql(), ct);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = this.UpsertSql();
        AddParameter(cmd, "@store_name", storeName);
        AddParameter(cmd, "@seeded_at_utc", seededAtUtc.UtcDateTime.ToString("O", CultureInfo.InvariantCulture));

        await cmd.ExecuteNonQueryAsync(ct);
    }

    DbConnection Open() => kind switch
    {
        DocumentProviderKind.Postgres or DocumentProviderKind.CockroachDb
            => new Npgsql.NpgsqlConnection(connectionString),
        DocumentProviderKind.SqlServer
            => new Microsoft.Data.SqlClient.SqlConnection(connectionString),
        DocumentProviderKind.MySql or DocumentProviderKind.MariaDb
            => new MySqlConnector.MySqlConnection(connectionString),
        DocumentProviderKind.Sqlite
            => new Microsoft.Data.Sqlite.SqliteConnection(connectionString),
        _ => throw new NotSupportedException($"DocumentProviderKind '{kind}' has no seed-marker connection.")
    };

    string CreateTableSql() => kind switch
    {
        DocumentProviderKind.Postgres or DocumentProviderKind.CockroachDb =>
            $"CREATE TABLE IF NOT EXISTS {TableName} (store_name varchar(200) NOT NULL PRIMARY KEY, seeded_at_utc varchar(40) NOT NULL)",
        DocumentProviderKind.SqlServer =>
            $"IF OBJECT_ID(N'{TableName}', N'U') IS NULL CREATE TABLE {TableName} (store_name nvarchar(200) NOT NULL PRIMARY KEY, seeded_at_utc nvarchar(40) NOT NULL)",
        DocumentProviderKind.MySql or DocumentProviderKind.MariaDb =>
            $"CREATE TABLE IF NOT EXISTS {TableName} (store_name varchar(200) NOT NULL PRIMARY KEY, seeded_at_utc varchar(40) NOT NULL)",
        DocumentProviderKind.Sqlite =>
            $"CREATE TABLE IF NOT EXISTS {TableName} (store_name TEXT NOT NULL PRIMARY KEY, seeded_at_utc TEXT NOT NULL)",
        _ => throw new NotSupportedException($"DocumentProviderKind '{kind}' has no seed-marker DDL.")
    };

    string UpsertSql() => kind switch
    {
        DocumentProviderKind.Postgres or DocumentProviderKind.CockroachDb or DocumentProviderKind.Sqlite =>
            $"INSERT INTO {TableName} (store_name, seeded_at_utc) VALUES (@store_name, @seeded_at_utc) " +
            "ON CONFLICT (store_name) DO UPDATE SET seeded_at_utc = EXCLUDED.seeded_at_utc",
        DocumentProviderKind.MySql or DocumentProviderKind.MariaDb =>
            $"INSERT INTO {TableName} (store_name, seeded_at_utc) VALUES (@store_name, @seeded_at_utc) " +
            "ON DUPLICATE KEY UPDATE seeded_at_utc = VALUES(seeded_at_utc)",
        DocumentProviderKind.SqlServer =>
            $"UPDATE {TableName} SET seeded_at_utc = @seeded_at_utc WHERE store_name = @store_name; " +
            $"IF @@ROWCOUNT = 0 INSERT INTO {TableName} (store_name, seeded_at_utc) VALUES (@store_name, @seeded_at_utc)",
        _ => throw new NotSupportedException($"DocumentProviderKind '{kind}' has no seed-marker upsert.")
    };

    static async Task ExecuteAsync(DbConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    static void AddParameter(DbCommand cmd, string name, string value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }
}
