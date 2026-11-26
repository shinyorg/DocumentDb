using System.Text;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Sqlite.SqlCipher;

/// <summary>
/// An encrypted SQLite database in the SQLCipher 4 file format, on the SQLite3 Multiple Ciphers engine.
/// </summary>
public class SqlCipherDatabaseProvider : SqliteDatabaseProvider
{
    readonly string password;

    public SqlCipherDatabaseProvider(string filePath, string password) : base(BuildConnectionString(filePath, password))
    {
        this.password = password;
    }

    internal string Password => this.password;

    /// <summary>
    /// The connection string for an encrypted file: the path as a SQLite URI carrying
    /// <c>cipher=sqlcipher&amp;legacy=4</c>, so SQLite3 Multiple Ciphers reads and writes the SQLCipher 4
    /// format (its own default is ChaCha20-Poly1305) and databases created by the SQLCipher engine keep
    /// opening. The cipher has to be chosen before <c>PRAGMA key</c> runs, which is why it rides on the
    /// file name rather than being a pragma.
    /// </summary>
    internal static string BuildConnectionString(string filePath, string password)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        SqlCipherEngine.Init();

        return new SqliteConnectionStringBuilder
        {
            DataSource = ToCipherUri(filePath),
            Password = password
        }.ToString();
    }

    // SQLite URI filenames: '?', '#' and '%' are reserved in the path and must be percent-encoded, '\' is
    // not a separator, and a Windows drive path needs a leading '/' ("file:/C:/data/app.db").
    static string ToCipherUri(string filePath)
    {
        var path = filePath == ":memory:" ? filePath : filePath.Replace('\\', '/');
        if (path.Length > 1 && path[1] == ':' && Char.IsLetter(path[0]))
            path = "/" + path;

        var sb = new StringBuilder("file:", path.Length + 32);
        foreach (var c in path)
        {
            sb.Append(c switch
            {
                '%' => "%25",
                '?' => "%3f",
                '#' => "%23",
                _ => c.ToString()
            });
        }
        sb.Append("?cipher=sqlcipher&legacy=4");
        return sb.ToString();
    }
}
