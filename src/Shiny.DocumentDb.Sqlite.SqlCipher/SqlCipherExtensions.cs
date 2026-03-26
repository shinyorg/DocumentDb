using Microsoft.Data.Sqlite;

namespace Shiny.DocumentDb.Sqlite.SqlCipher;

public static class SqlCipherExtensions
{
    public static async Task RekeyAsync(this IDocumentStore store, string newPassword, CancellationToken ct = default)
    {
        var provider = (store as DocumentStore)?.DatabaseProvider as SqlCipherDatabaseProvider
            ?? throw new InvalidOperationException("RekeyAsync is only supported when using SqlCipherDatabaseProvider.");

        await using var connection = (SqliteConnection)provider.CreateConnection();
        await connection.OpenAsync(ct).ConfigureAwait(false);

        await using var quoteCmd = connection.CreateCommand();
        quoteCmd.CommandText = "SELECT quote($newPassword);";
        quoteCmd.Parameters.AddWithValue("$newPassword", newPassword);
        var quotedNewPassword = (string)(await quoteCmd.ExecuteScalarAsync(ct).ConfigureAwait(false))!;

        await using var rekeyCmd = connection.CreateCommand();
        rekeyCmd.CommandText = "PRAGMA rekey = " + quotedNewPassword;
        await rekeyCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}
