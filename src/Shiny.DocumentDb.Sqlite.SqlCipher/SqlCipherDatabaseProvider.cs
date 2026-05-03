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

    internal string Password => this.password;
}
