using Microsoft.Data.Sqlite;

namespace Shiny.DocumentDb.Sqlite.SqlCipher;

public class SqlCipherDocumentStore : DocumentStore
{
    readonly string connectionString;
    readonly string password;

    public SqlCipherDocumentStore(string filePath, string password)
        : this(new DocumentStoreOptions { DatabaseProvider = new SqlCipherDatabaseProvider(filePath, password) })
    {
    }

    public SqlCipherDocumentStore(DocumentStoreOptions options) : base(options)
    {
        if (options.DatabaseProvider is not SqlCipherDatabaseProvider cipher)
            throw new ArgumentException("DatabaseProvider must be a SqlCipherDatabaseProvider.", nameof(options));

        this.connectionString = cipher.ConnectionString;
        this.password = cipher.Password;
    }

    /// <summary>
    /// Creates an encrypted backup of the SQLite database to the specified file path.
    /// </summary>
    public async Task Backup(string destinationPath, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);

        await using var source = new SqliteConnection(this.connectionString);
        await source.OpenAsync(cancellationToken).ConfigureAwait(false);

        var destConnStr = SqlCipherDatabaseProvider.BuildConnectionString(destinationPath, this.password);
        await using var destination = new SqliteConnection(destConnStr);
        await destination.OpenAsync(cancellationToken).ConfigureAwait(false);

        source.BackupDatabase(destination);
    }
}
