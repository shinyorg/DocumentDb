using Microsoft.Data.Sqlite;

namespace Shiny.DocumentDb.Sqlite;

public class SqliteDocumentStore : DocumentStore
{
    readonly string connectionString;

    public SqliteDocumentStore(string connectionString) : base(new DocumentStoreOptions
    {
        DatabaseProvider = new SqliteDatabaseProvider(connectionString)
    })
    {
        this.connectionString = connectionString;
    }

    public SqliteDocumentStore(DocumentStoreOptions options) : base(options)
    {
        this.connectionString = options.DatabaseProvider is SqliteDatabaseProvider sqlite
            ? sqlite.ConnectionString
            : throw new ArgumentException("DatabaseProvider must be a SqliteDatabaseProvider.", nameof(options));
    }

    /// <summary>
    /// Creates a backup of the SQLite database to the specified file path.
    /// Not supported in WebAssembly environments.
    /// </summary>
    [System.Runtime.Versioning.UnsupportedOSPlatform("browser")]
    public async Task Backup(string destinationPath, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);

        await using var source = new SqliteConnection(this.connectionString);
        await source.OpenAsync(cancellationToken).ConfigureAwait(false);

        var destConnStr = new SqliteConnectionStringBuilder { DataSource = destinationPath }.ToString();
        await using var destination = new SqliteConnection(destConnStr);
        await destination.OpenAsync(cancellationToken).ConfigureAwait(false);

        source.BackupDatabase(destination);
    }

    /// <summary>
    /// Deletes all documents across all tables in the SQLite database.
    /// This includes document data and spatial sidecar tables.
    /// </summary>
    public async Task ClearAllAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new SqliteConnection(this.connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        var tables = new List<string>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';";
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                tables.Add(reader.GetString(0));
        }

        foreach (var table in tables)
        {
            await using var deleteCmd = conn.CreateCommand();
            deleteCmd.CommandText = $"DELETE FROM {table};";
            await deleteCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
