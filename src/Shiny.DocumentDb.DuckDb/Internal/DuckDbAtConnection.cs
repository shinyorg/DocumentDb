using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;
using DuckDB.NET.Data;

namespace Shiny.DocumentDb.DuckDb.Internal;

/// <summary>
/// Wraps <see cref="DuckDBConnection"/> so SQL using @param placeholders gets rewritten to
/// DuckDB's native $param syntax just before execution. The rest of the Shiny.DocumentDb
/// codebase hardcodes @id/@typeName, so the rewrite happens here rather than at every call site.
/// </summary>
internal sealed class DuckDbAtConnection : DbConnection
{
    readonly DuckDBConnection inner;

    public DuckDbAtConnection(string connectionString)
    {
        this.inner = new DuckDBConnection(connectionString);
    }

    /// <summary>The wrapped native connection — used by the provider's appender-based bulk copy path,
    /// which needs the real <see cref="DuckDBConnection"/> rather than this @param-rewriting wrapper.</summary>
    internal DuckDBConnection Inner => this.inner;

    public override string ConnectionString
    {
        get => this.inner.ConnectionString;
        set => this.inner.ConnectionString = value;
    }

    public override string Database => this.inner.Database;
    public override string DataSource => this.inner.DataSource;
    public override string ServerVersion => this.inner.ServerVersion;
    public override ConnectionState State => this.inner.State;

    public override void ChangeDatabase(string databaseName) => this.inner.ChangeDatabase(databaseName);
    public override void Close() => this.inner.Close();
    public override void Open() => this.inner.Open();
    public override Task OpenAsync(CancellationToken cancellationToken) => this.inner.OpenAsync(cancellationToken);

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        => this.inner.BeginTransaction(isolationLevel);

    protected override DbCommand CreateDbCommand() => new DuckDbAtCommand(this.inner.CreateCommand(), this);

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            this.inner.Dispose();
        base.Dispose(disposing);
    }
}

internal sealed class DuckDbAtCommand : DbCommand
{
    static readonly Regex AtParamRegex = new(@"@([A-Za-z_][A-Za-z0-9_]*)", RegexOptions.Compiled);

    readonly DuckDBCommand inner;
    readonly DuckDbAtConnection owner;
    string commandText = string.Empty;

    public DuckDbAtCommand(DuckDBCommand inner, DuckDbAtConnection owner)
    {
        this.inner = inner;
        this.owner = owner;
    }

    public override string CommandText
    {
        get => this.commandText;
        set => this.commandText = value ?? string.Empty;
    }

    public override int CommandTimeout
    {
        get => this.inner.CommandTimeout;
        set => this.inner.CommandTimeout = value;
    }

    public override CommandType CommandType
    {
        get => this.inner.CommandType;
        set => this.inner.CommandType = value;
    }

    public override bool DesignTimeVisible
    {
        get => this.inner.DesignTimeVisible;
        set => this.inner.DesignTimeVisible = value;
    }

    public override UpdateRowSource UpdatedRowSource
    {
        get => this.inner.UpdatedRowSource;
        set => this.inner.UpdatedRowSource = value;
    }

    protected override DbConnection? DbConnection
    {
        get => this.owner;
        set { /* fixed to owning connection */ }
    }

    protected override DbParameterCollection DbParameterCollection => this.inner.Parameters;

    protected override DbTransaction? DbTransaction
    {
        get => this.inner.Transaction;
        set => this.inner.Transaction = value as DuckDBTransaction;
    }

    protected override DbParameter CreateDbParameter() => this.inner.CreateParameter();

    public override void Cancel() => this.inner.Cancel();
    public override void Prepare() => this.inner.Prepare();

    public override int ExecuteNonQuery()
    {
        this.SyncToInner();
        return this.inner.ExecuteNonQuery();
    }

    public override object? ExecuteScalar()
    {
        this.SyncToInner();
        return this.inner.ExecuteScalar();
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        this.SyncToInner();
        return this.inner.ExecuteReader(behavior);
    }

    void SyncToInner()
    {
        this.inner.CommandText = AtParamRegex.Replace(this.commandText, "$$$1");
        foreach (DbParameter p in this.inner.Parameters)
        {
            if (p.ParameterName.StartsWith('@'))
                p.ParameterName = p.ParameterName[1..];

            // DuckDB.NET has no native binding for DateTimeOffset — left as-is it is ToString()'d with the
            // current culture (e.g. "2026-06-12 9:34:12 PM +00:00"), which DuckDB cannot parse as a
            // TIMESTAMPTZ. Bind an invariant ISO-8601 UTC string instead and let DuckDB cast it.
            if (p.Value is DateTimeOffset dto)
                p.Value = dto.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "+00";
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            this.inner.Dispose();
        base.Dispose(disposing);
    }
}
