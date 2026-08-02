using System.Data.Common;
using System.Globalization;
using System.Text;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Reader/parameter helpers that paper over the nine ADO.NET drivers. Nothing here is clever - it
/// exists because each driver returns a different CLR type for the same envelope column
/// (SQLite stores timestamps as TEXT, Oracle hands back CLOBs and decimals, and so on).
/// </summary>
public static class Ado
{
    public static DbCommand Command(DbConnection connection, string sql)
    {
        var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return cmd;
    }

    /// <summary>
    /// Binds a parameter. Always use the <c>@name</c> form: every provider's connection either accepts
    /// it natively or wraps the command to rewrite it (Oracle to <c>:name</c>, DuckDB to <c>$name</c>).
    /// </summary>
    public static void Bind(DbCommand cmd, string name, object? value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(p);
    }

    public static string Text(DbDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            return "";

        try
        {
            return reader.GetString(ordinal);
        }
        catch (InvalidCastException)
        {
            return Stringify(reader.GetValue(ordinal)) ?? "";
        }
    }

    public static string? NullableText(DbDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : Text(reader, ordinal);

    public static DateTimeOffset? Timestamp(DbDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            return null;

        var value = reader.GetValue(ordinal);
        return value switch
        {
            DateTimeOffset dto => dto,
            DateTime dt => new DateTimeOffset(dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt),
            string s when DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed) => parsed,
            _ => null
        };
    }

    public static long Count(object? scalar) => scalar switch
    {
        null or DBNull => 0,
        long l => l,
        int i => i,
        decimal d => (long)d,
        double db => (long)db,
        string s when long.TryParse(s, CultureInfo.InvariantCulture, out var parsed) => parsed,
        _ => Convert.ToInt64(scalar, CultureInfo.InvariantCulture)
    };

    /// <summary>Renders any driver value as display text for the SQL console grid.</summary>
    public static string? Stringify(object? value) => value switch
    {
        null or DBNull => null,
        string s => s,
        char[] c => new string(c),
        byte[] b => IsProbablyText(b) ? Encoding.UTF8.GetString(b) : $"0x{Convert.ToHexString(b[..Math.Min(b.Length, 32)])}{(b.Length > 32 ? "..." : "")}",
        DateTimeOffset dto => dto.ToString("O", CultureInfo.InvariantCulture),
        DateTime dt => dt.ToString("O", CultureInfo.InvariantCulture),
        bool bo => bo ? "true" : "false",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString()
    };

    static bool IsProbablyText(byte[] bytes)
        => bytes.Length > 0 && bytes.Length < 4096 && bytes.All(b => b >= 0x09 && b != 0x7F);

    /// <summary>
    /// The tool refuses to interpolate a table name it has not first read back from the database's own
    /// catalog, but this belt-and-braces check keeps anything odd out of generated SQL.
    /// </summary>
    public static string SafeIdentifier(string name)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Any(c => !char.IsLetterOrDigit(c) && c != '_' && c != '$' && c != '#'))
            throw new ArgumentException($"'{name}' is not a usable table name.", nameof(name));

        return name;
    }
}
