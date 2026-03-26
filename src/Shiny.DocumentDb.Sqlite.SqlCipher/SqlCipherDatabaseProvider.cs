using System.Data.Common;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Sqlite.SqlCipher;

public class SqlCipherDatabaseProvider : SqliteDatabaseProvider
{
    readonly string password;

    public SqlCipherDatabaseProvider(string filePath, string password) : base(
        new SqliteConnectionStringBuilder
        {
            DataSource = filePath,
            Password = password
        }.ToString()
    )
    {
        this.password = password;
    }

    public override async Task BackupAsync(DbConnection connection, string destinationPath, CancellationToken ct)
    {
        var destinationBuilder = new SqliteConnectionStringBuilder
        {
            DataSource = destinationPath,
            Password = this.password
        };

        await using var destination = new SqliteConnection(destinationBuilder.ToString());
        await destination.OpenAsync(ct).ConfigureAwait(false);
        ((SqliteConnection)connection).BackupDatabase(destination);
    }
}
