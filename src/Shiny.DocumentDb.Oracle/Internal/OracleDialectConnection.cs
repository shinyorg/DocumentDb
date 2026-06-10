using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Oracle.ManagedDataAccess.Client;

namespace Shiny.DocumentDb.Oracle.Internal;

/// <summary>
/// Delegating connection that adapts the DocumentDb core SQL conventions to Oracle. The core
/// authors SQL with @-prefixed named parameters, trailing semicolons, and FROM-less SELECTs —
/// all of which Oracle rejects — so every command created from this connection rewrites its
/// command text and parameter names just before execution (see <see cref="OracleDialectCommand"/>).
/// </summary>
sealed class OracleDialectConnection(OracleConnection inner) : DbConnection
{
    internal OracleConnection Inner => inner;

    [AllowNull]
    public override string ConnectionString
    {
        get => inner.ConnectionString;
        set => inner.ConnectionString = value;
    }

    public override string Database => inner.Database;
    public override string DataSource => inner.DataSource;
    public override string ServerVersion => inner.ServerVersion;
    public override ConnectionState State => inner.State;

    public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);
    public override void Close() => inner.Close();
    public override Task CloseAsync() => inner.CloseAsync();
    public override void Open() => inner.Open();
    public override Task OpenAsync(CancellationToken cancellationToken) => inner.OpenAsync(cancellationToken);

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        => inner.BeginTransaction(Map(isolationLevel));

    protected override async ValueTask<DbTransaction> BeginDbTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken)
        => await inner.BeginTransactionAsync(Map(isolationLevel), cancellationToken).ConfigureAwait(false);

    // ODP.NET only supports ReadCommitted and Serializable; Unspecified throws
    static IsolationLevel Map(IsolationLevel level)
        => level == IsolationLevel.Unspecified ? IsolationLevel.ReadCommitted : level;

    protected override DbCommand CreateDbCommand() => new OracleDialectCommand(inner.CreateCommand());

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            inner.Dispose();
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await inner.DisposeAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }
}
