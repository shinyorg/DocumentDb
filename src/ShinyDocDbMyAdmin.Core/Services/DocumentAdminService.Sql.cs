using System.Diagnostics;
using System.Text.Json;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

public sealed partial class DocumentAdminService
{
    /// <summary>Rows the console will materialise before it stops and says so.</summary>
    public const int SqlRowLimit = 1000;

    /// <summary>
    /// Runs a statement typed by the user and returns either a grid or an affected-row count.
    /// </summary>
    /// <remarks>
    /// This is the phpMyAdmin SQL tab: whatever you type is what runs. There is no statement parsing and
    /// no allow-list - the only guard is a read-only profile, which refuses anything that is not a SELECT.
    /// The dialect is the target database's own, except that <c>@name</c> placeholders work everywhere
    /// because each provider's connection rewrites them.
    /// </remarks>
    public async Task<SqlResult> ExecuteSql(
        string profileId,
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException("Nothing to run.", nameof(sql));

        var connection = await this.Connect(profileId, ct);
        if (connection.Profile.ReadOnly && !LooksReadOnly(sql))
            throw new InvalidOperationException($"Connection '{connection.Profile.Name}' is read-only; only SELECT statements are allowed.");

        var stopwatch = Stopwatch.StartNew();
        var result = await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, sql.Trim());
            foreach (var (name, value) in parameters ?? new Dictionary<string, object?>())
                Ado.Bind(cmd, name.StartsWith('@') ? name : "@" + name, connection.Provider.NormalizeParameterValue(value));

            await using var reader = await cmd.ExecuteReaderAsync(token);

            if (reader.FieldCount == 0)
                return new SqlResult([], [], reader.RecordsAffected, TimeSpan.Zero, false);

            var columns = new List<string>(reader.FieldCount);
            for (var i = 0; i < reader.FieldCount; i++)
                columns.Add(reader.GetName(i));

            var rows = new List<IReadOnlyList<string?>>();
            var truncated = false;
            while (await reader.ReadAsync(token))
            {
                if (rows.Count >= SqlRowLimit)
                {
                    truncated = true;
                    break;
                }

                var row = new string?[reader.FieldCount];
                for (var i = 0; i < reader.FieldCount; i++)
                    row[i] = Ado.Stringify(reader.IsDBNull(i) ? null : reader.GetValue(i));

                rows.Add(row);
            }

            return new SqlResult(columns, rows, reader.RecordsAffected, TimeSpan.Zero, truncated);
        }, ct);

        stopwatch.Stop();

        // DDL invalidates the cached table list, and there is no cheap way to know whether it did.
        this.InvalidateSchema(profileId);

        return result with { Elapsed = stopwatch.Elapsed };
    }

    /// <summary>
    /// A conservative "is this a read?" test used only to enforce the read-only flag. It errs towards
    /// refusing: anything that is not a lone SELECT/WITH/SHOW/EXPLAIN, or that contains a second
    /// statement, is treated as a write.
    /// </summary>
    /// <summary>
    /// Parses the console's parameter box - a JSON object - into bound values, so a statement can use
    /// <c>@name</c> placeholders instead of hand-quoting literals. JSON keeps the types honest:
    /// <c>{"min": 450}</c> binds a number, <c>{"min": "450"}</c> binds text, and on SQLite that is the
    /// difference between a numeric and a lexical comparison.
    /// </summary>
    public static IReadOnlyDictionary<string, object?> ParseParameters(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return new Dictionary<string, object?>();

        using var document = JsonDocument.Parse(json);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
            throw new JsonException("Parameters must be a JSON object, e.g. {\"status\": \"Shipped\"}.");

        var result = new Dictionary<string, object?>();
        foreach (var property in document.RootElement.EnumerateObject())
        {
            result[property.Name] = property.Value.ValueKind switch
            {
                JsonValueKind.String => property.Value.GetString(),
                JsonValueKind.Number => property.Value.TryGetInt64(out var l) ? l : property.Value.GetDouble(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                // Arrays and objects have no single bindable ADO.NET representation; pass the raw JSON
                // text so a statement can still hand it to a json function.
                _ => property.Value.GetRawText()
            };
        }

        return result;
    }

    static bool LooksReadOnly(string sql)
    {
        var trimmed = sql.Trim().TrimEnd(';');
        if (trimmed.Contains(';'))
            return false;

        var firstWord = trimmed.Split([' ', '\t', '\r', '\n', '('], StringSplitOptions.RemoveEmptyEntries).FirstOrDefault() ?? "";
        return firstWord.ToLowerInvariant() is "select" or "with" or "show" or "explain" or "describe" or "desc" or "pragma";
    }
}
