using System.Text.RegularExpressions;
using Shiny.DocumentDb;

namespace ShinyDocDbMcp;

/// <summary>
/// The tool has no compiled document classes, so what it exposes has to come from the data: the distinct
/// <c>TypeName</c> discriminators in the documents table, each published as a schema-free collection.
/// </summary>
static partial class TypeDiscovery
{
    public static async Task<IReadOnlyList<string>> ListTypeNames(
        IDatabaseProvider provider,
        string table,
        CancellationToken cancellationToken)
    {
        if (!Identifier().IsMatch(table))
            throw new UsageException($"'{table}' is not a valid table name.");

        await using var connection = provider.CreateConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await provider.InitializeConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT DISTINCT TypeName FROM {table} ORDER BY TypeName";

        var names = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!await reader.IsDBNullAsync(0, cancellationToken).ConfigureAwait(false))
                names.Add(reader.GetString(0));
        }
        return names;
    }

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]{0,127}$")]
    private static partial Regex Identifier();
}
