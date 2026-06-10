using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;
using Oracle.ManagedDataAccess.Client;

namespace Shiny.DocumentDb.Oracle.Internal;

/// <summary>
/// Delegating command that translates the core's provider-agnostic SQL into Oracle dialect at
/// execution time: rewrites @name placeholders to :name, strips trailing semicolons (illegal in
/// plain SQL through ODP.NET, but kept for PL/SQL blocks), removes the MSSQL-style
/// "ORDER BY (SELECT NULL)" pagination idiom (Oracle's OFFSET/FETCH needs no ORDER BY), and
/// appends FROM DUAL to FROM-less SELECT CASE expressions. Parameters are bound by name
/// (ODP.NET defaults to positional) with the @ prefix stripped, and large strings are bound
/// as CLOB to clear the VARCHAR2 bind size limit.
/// </summary>
sealed partial class OracleDialectCommand(OracleCommand inner) : DbCommand
{
    string rawCommandText = "";

    [AllowNull]
    public override string CommandText
    {
        get => rawCommandText;
        set
        {
            rawCommandText = value ?? "";
            inner.CommandText = Rewrite(rawCommandText);
        }
    }

    public override int CommandTimeout
    {
        get => inner.CommandTimeout;
        set => inner.CommandTimeout = value;
    }

    public override CommandType CommandType
    {
        get => inner.CommandType;
        set => inner.CommandType = value;
    }

    public override bool DesignTimeVisible
    {
        get => inner.DesignTimeVisible;
        set => inner.DesignTimeVisible = value;
    }

    public override UpdateRowSource UpdatedRowSource
    {
        get => inner.UpdatedRowSource;
        set => inner.UpdatedRowSource = value;
    }

    protected override DbConnection? DbConnection
    {
        get => inner.Connection;
        set => inner.Connection = value switch
        {
            null => null,
            OracleDialectConnection wrapped => wrapped.Inner,
            OracleConnection oracle => oracle,
            _ => throw new ArgumentException($"Expected an Oracle connection, got {value.GetType()}.")
        };
    }

    protected override DbTransaction? DbTransaction
    {
        get => inner.Transaction;
        set => inner.Transaction = (OracleTransaction?)value;
    }

    protected override DbParameterCollection DbParameterCollection => inner.Parameters;

    protected override DbParameter CreateDbParameter() => inner.CreateParameter();

    public override void Cancel() => inner.Cancel();

    public override void Prepare()
    {
        this.PrepareExecution();
        inner.Prepare();
    }

    public override int ExecuteNonQuery()
    {
        this.PrepareExecution();
        return inner.ExecuteNonQuery();
    }

    public override object? ExecuteScalar()
    {
        this.PrepareExecution();
        return inner.ExecuteScalar();
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        this.PrepareExecution();
        return inner.ExecuteReader(behavior);
    }

    public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        this.PrepareExecution();
        return inner.ExecuteNonQueryAsync(cancellationToken);
    }

    public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        this.PrepareExecution();
        return inner.ExecuteScalarAsync(cancellationToken);
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        this.PrepareExecution();
        return await inner.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);
    }

    void PrepareExecution()
    {
        // ODP.NET binds by position unless told otherwise — the core's SQL repeats placeholders
        // (e.g. @now twice in an insert), which only works with name-based binding.
        inner.BindByName = true;

        foreach (OracleParameter p in inner.Parameters)
        {
            if (p.ParameterName.StartsWith('@'))
                p.ParameterName = p.ParameterName[1..];

            // Past the VARCHAR2 bind limit the string must go over as a CLOB
            if (p.Value is string s && s.Length > 4000)
                p.OracleDbType = OracleDbType.Clob;
        }
    }

    internal static string Rewrite(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return sql;

        var text = sql.Trim();
        var isPlSqlBlock = text.StartsWith("BEGIN", StringComparison.OrdinalIgnoreCase)
            || text.StartsWith("DECLARE", StringComparison.OrdinalIgnoreCase);

        if (!isPlSqlBlock)
        {
            while (text.EndsWith(';'))
                text = text[..^1].TrimEnd();

            // MSSQL idiom for "OFFSET/FETCH without an order" — Oracle neither supports
            // subqueries in ORDER BY nor requires an ORDER BY for OFFSET/FETCH.
            text = text.Replace(" ORDER BY (SELECT NULL)", "");

            if (text.StartsWith("SELECT CASE WHEN EXISTS", StringComparison.OrdinalIgnoreCase) &&
                text.EndsWith("END", StringComparison.OrdinalIgnoreCase))
                text += " FROM DUAL";
        }

        return ParameterPlaceholder().Replace(text, ":$1");
    }

    [GeneratedRegex("@([A-Za-z_][A-Za-z0-9_]*)")]
    private static partial Regex ParameterPlaceholder();

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
