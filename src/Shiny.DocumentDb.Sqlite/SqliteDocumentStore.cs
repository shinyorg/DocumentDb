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
    /// Deletes all documents across all tables in the SQLite database, including spatial, vector, and
    /// temporal-history sidecars. Equivalent to <see cref="IDocumentMaintenance.ClearAll"/> — retained
    /// for back-compat.
    /// </summary>
    public Task ClearAllAsync(CancellationToken cancellationToken = default)
        => this.ClearAll(cancellationToken);
}
