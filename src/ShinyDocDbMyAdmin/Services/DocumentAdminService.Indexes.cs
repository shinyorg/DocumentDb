using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

public sealed partial class DocumentAdminService
{
    /// <summary>The prefix DocumentDb gives every JSON property index it creates.</summary>
    const string IndexPrefix = "idx_json_";

    /// <summary>
    /// Lists the JSON property indexes on a table. Index names follow DocumentDb's own
    /// <c>idx_json_{type}_{path}</c> convention, and both halves can contain underscores, so the known
    /// type names are used to split the name rather than guessing at the first separator.
    /// </summary>
    public async Task<IReadOnlyList<JsonIndexInfo>> ListIndexes(string profileId, string table, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var safeTable = Ado.SafeIdentifier(table);
        var pattern = IndexPrefix + "%";

        var names = await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, connection.Provider.BuildListJsonIndexesSql(safeTable, pattern));
            Ado.Bind(cmd, "@prefix", pattern);

            await using var reader = await cmd.ExecuteReaderAsync(token);
            var results = new List<string>();
            while (await reader.ReadAsync(token))
                results.Add(Ado.Text(reader, 0));

            return results;
        }, ct);

        var types = await this.ListTypes(profileId, safeTable, ct);
        return [.. names.Select(name => Describe(name, types)).OrderBy(x => x.TypeName).ThenBy(x => x.Path)];
    }

    static JsonIndexInfo Describe(string indexName, IReadOnlyList<DocumentTypeInfo> types)
    {
        var body = indexName.StartsWith(IndexPrefix, StringComparison.OrdinalIgnoreCase)
            ? indexName[IndexPrefix.Length..]
            : indexName;

        // Longest match first, so "OrderLine" is not mistaken for "Order".
        foreach (var type in types.OrderByDescending(t => t.TypeName.Length))
        {
            var sanitized = Sanitize(type.TypeName);
            if (body.StartsWith(sanitized + "_", StringComparison.OrdinalIgnoreCase))
                return new JsonIndexInfo(indexName, type.TypeName, body[(sanitized.Length + 1)..]);
        }

        // An index for a type that currently has no rows - report the raw name rather than inventing one.
        return new JsonIndexInfo(indexName, "?", body);
    }

    public async Task CreateIndex(string profileId, string table, string typeName, string jsonPath, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        if (string.IsNullOrWhiteSpace(jsonPath))
            throw new ArgumentException("A JSON path is required.", nameof(jsonPath));

        // The path is interpolated into DDL by the library's own builders and cannot be parameterised,
        // so anything that is not a plain dotted property path is refused outright.
        foreach (var segment in jsonPath.Split('.'))
        {
            if (segment.Length == 0 || segment.Any(c => !char.IsLetterOrDigit(c) && c != '_'))
                throw new ArgumentException($"'{jsonPath}' is not a plain dotted property path.", nameof(jsonPath));
        }

        var safeTable = Ado.SafeIdentifier(table);
        var indexName = BuildIndexName(typeName, jsonPath);

        await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, connection.Provider.BuildCreateJsonIndexSql(indexName, safeTable, jsonPath, typeName));
            await cmd.ExecuteNonQueryAsync(token);
        }, ct);

        logger.LogInformation("Created index {Index} on {Table}", indexName, table);
    }

    public async Task DropIndex(string profileId, string table, string indexName, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        var safeTable = Ado.SafeIdentifier(table);
        await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, connection.Provider.BuildDropIndexSql(Ado.SafeIdentifier(indexName), safeTable));
            await cmd.ExecuteNonQueryAsync(token);
        }, ct);

        logger.LogInformation("Dropped index {Index} on {Table}", indexName, table);
    }

    /// <summary>Mirrors <c>IndexExpressionHelper.BuildIndexName</c> so names line up with library-created indexes.</summary>
    public static string BuildIndexName(string typeName, string jsonPath)
        => $"{IndexPrefix}{Sanitize(typeName)}_{jsonPath.Replace('.', '_')}";

    static string Sanitize(string typeName) => typeName.Replace('.', '_');
}
