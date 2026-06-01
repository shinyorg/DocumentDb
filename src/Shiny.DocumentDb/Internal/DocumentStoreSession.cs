using System.Data.Common;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// A bound (connection, optional-transaction) pair handed to every operation lambda. Lets the
/// store hand callers a fresh per-operation connection (server SQL pooling) or the long-lived
/// shared connection (SQLite) through a single API.
/// </summary>
internal sealed class DocumentStoreSession
{
    public DbConnection Connection { get; }
    public DbTransaction? Transaction { get; }

    public DocumentStoreSession(DbConnection connection, DbTransaction? transaction = null)
    {
        this.Connection = connection;
        this.Transaction = transaction;
    }

    public DbCommand CreateCommand()
    {
        var cmd = this.Connection.CreateCommand();
        if (this.Transaction != null)
            cmd.Transaction = this.Transaction;
        return cmd;
    }
}
