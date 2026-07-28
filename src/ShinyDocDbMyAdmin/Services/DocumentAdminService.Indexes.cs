using System.Data.Common;
using System.Diagnostics;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

public sealed partial class DocumentAdminService
{
    /// <summary>The prefix DocumentDb gives every JSON property index it creates.</summary>
    const string IndexPrefix = "idx_json_";

    /// <summary>
    /// What the library joins the paths of a composite index with. Two underscores rather than one so a
    /// composite name cannot collide with a single path whose JSON name contains an underscore.
    /// </summary>
    const string CompositeSeparator = "__";

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

    /// <summary>
    /// Every index on the table, not only the ones DocumentDb created — because an index someone added
    /// by hand is exactly the thing that explains why a query is fast, and hiding it makes the tool
    /// lie by omission. Falls back to the DocumentDb-only list on a provider with no catalog view.
    /// </summary>
    public async Task<IReadOnlyList<IndexInfo>> ListAllIndexes(string profileId, string table, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var safeTable = Ado.SafeIdentifier(table);
        var sql = connection.Provider.BuildListAllIndexesSql(safeTable);

        if (sql is null)
        {
            var known = await this.ListIndexes(profileId, table, ct);
            return [.. known.Select(i => new IndexInfo(i.Name, null, null, null, i))];
        }

        var types = await this.ListTypes(profileId, safeTable, ct);
        var rows = await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, sql);
            await using var reader = await cmd.ExecuteReaderAsync(token);

            var results = new List<IndexInfo>();
            while (await reader.ReadAsync(token))
            {
                var name = Ado.Text(reader, 0);
                var isOurs = name.StartsWith(IndexPrefix, StringComparison.OrdinalIgnoreCase);

                results.Add(new IndexInfo(
                    name,
                    Ado.NullableText(reader, 1),
                    reader.IsDBNull(2) ? null : Ado.Count(reader.GetValue(2)),
                    reader.IsDBNull(3) ? null : Ado.Count(reader.GetValue(3)),
                    isOurs ? Describe(name, types) : null));
            }

            return (IReadOnlyList<IndexInfo>)results;
        }, ct);

        return rows;
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
                return new JsonIndexInfo(indexName, type.TypeName, SplitPaths(body[(sanitized.Length + 1)..]));
        }

        // An index for a type that currently has no rows - report the raw name rather than inventing one.
        return new JsonIndexInfo(indexName, "?", [body]);
    }

    /// <summary>
    /// Recovers the paths a composite index covers from its name. The library joins them with
    /// <c>__</c> and replaces each path's dots with <c>_</c>, so this is a best-effort inverse: a JSON
    /// property whose own name contains a double underscore is ambiguous and is accepted as such, which
    /// is the same trade the library documents when it builds the name.
    /// </summary>
    static IReadOnlyList<string> SplitPaths(string body)
        => body.Contains(CompositeSeparator, StringComparison.Ordinal)
            ? [.. body.Split(CompositeSeparator, StringSplitOptions.RemoveEmptyEntries)]
            : [body];

    /// <summary>Creates a single-path JSON index.</summary>
    public Task CreateIndex(string profileId, string table, string typeName, string jsonPath, CancellationToken ct = default)
        => this.CreateIndex(profileId, table, typeName, [jsonPath], ct);

    /// <summary>
    /// Creates a JSON property index over one or more paths, named exactly as DocumentDb names its own
    /// so the library recognises it and does not build a duplicate.
    /// </summary>
    public async Task CreateIndex(
        string profileId,
        string table,
        string typeName,
        IReadOnlyList<string> jsonPaths,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        var paths = jsonPaths
            .Select(p => p.Trim())
            .Where(p => p.Length > 0)
            .ToList();

        if (paths.Count == 0)
            throw new ArgumentException("At least one JSON path is required.", nameof(jsonPaths));

        // Paths are interpolated into DDL by the library's own builders and cannot be parameterised,
        // so anything that is not a plain dotted property path is refused outright.
        foreach (var segment in paths.SelectMany(p => p.Split('.')))
        {
            if (segment.Length == 0 || segment.Any(c => !char.IsLetterOrDigit(c) && c != '_'))
                throw new ArgumentException($"'{string.Join(", ", paths)}' is not a plain dotted property path.", nameof(jsonPaths));
        }

        var safeTable = Ado.SafeIdentifier(table);
        var indexName = BuildIndexName(typeName, paths);
        var provider = connection.Provider;

        await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, provider.BuildCreateJsonIndexSql(indexName, safeTable, paths, typeName));
            await cmd.ExecuteNonQueryAsync(token);
        }, ct);

        logger.LogInformation("Created index {Index} on {Table} over {Paths}", indexName, table, string.Join(", ", paths));
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
        => BuildIndexName(typeName, [jsonPath]);

    /// <inheritdoc cref="BuildIndexName(string, string)"/>
    public static string BuildIndexName(string typeName, IReadOnlyList<string> jsonPaths)
    {
        if (jsonPaths.Count == 0)
            throw new ArgumentException("At least one JSON path is required.", nameof(jsonPaths));

        var sanitizedType = Sanitize(typeName);
        return jsonPaths.Count == 1
            ? $"{IndexPrefix}{sanitizedType}_{jsonPaths[0].Replace('.', '_')}"
            : $"{IndexPrefix}{sanitizedType}_{string.Join(CompositeSeparator, jsonPaths.Select(p => p.Replace('.', '_')))}";
    }

    static string Sanitize(string typeName) => typeName.Replace('.', '_');

    // ── Query plans ─────────────────────────────────────────────────────

    /// <summary>
    /// Runs the provider's own EXPLAIN over a statement and returns the plan as text.
    /// </summary>
    /// <remarks>
    /// The statement is <b>planned, not executed</b> on every provider that supports this, so it is safe
    /// on a read-only connection — which is the whole point, since "why is this slow" is a question you
    /// most want to ask about production. Two providers need more than one statement to get there
    /// (Oracle populates a plan table then formats it; SQL Server toggles a session setting around the
    /// statement), so the plan is taken from the last result set that returns rows.
    /// </remarks>
    public async Task<QueryPlan> Explain(
        string profileId,
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException("Nothing to explain.", nameof(sql));

        var connection = await this.Connect(profileId, ct);
        var statements = connection.Provider.BuildExplainSql(sql.Trim().TrimEnd(';'));

        if (statements.Count == 0)
            return new QueryPlan([], TimeSpan.Zero, Supported: false);

        var stopwatch = Stopwatch.StartNew();
        var lines = await connection.Execute(async (db, token) =>
        {
            IReadOnlyList<string> plan = [];
            foreach (var statement in statements)
            {
                var rows = await ReadPlanRows(db, connection, statement, parameters, token);

                // The last result set with rows is the plan: SQL Server's SET statements return
                // nothing, and Oracle's EXPLAIN PLAN FOR only populates the table.
                if (rows.Count > 0)
                    plan = rows;
            }

            return plan;
        }, ct);

        stopwatch.Stop();
        return new QueryPlan(lines, stopwatch.Elapsed, Supported: true);
    }

    static async Task<IReadOnlyList<string>> ReadPlanRows(
        DbConnection db,
        AdminConnection connection,
        string statement,
        IReadOnlyDictionary<string, object?>? parameters,
        CancellationToken ct)
    {
        await using var cmd = Ado.Command(db, statement);

        // The plan of a parameterised query needs its parameters bound even though nothing runs -
        // SQL Server will not compile a batch referencing an undeclared variable.
        foreach (var (name, value) in parameters ?? new Dictionary<string, object?>())
        {
            var bound = name.StartsWith('@') ? name : "@" + name;
            if (statement.Contains(bound, StringComparison.OrdinalIgnoreCase))
                Ado.Bind(cmd, bound, connection.Provider.NormalizeParameterValue(value));
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (reader.FieldCount == 0)
            return [];

        var rows = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            // Plans come back with a different column layout per engine - SQLite's four-column
            // EXPLAIN QUERY PLAN, PostgreSQL's single text column - so the row is flattened rather
            // than gridded, which is also how every one of these reads naturally.
            var cells = new List<string>(reader.FieldCount);
            for (var i = 0; i < reader.FieldCount; i++)
                cells.Add(Ado.Stringify(reader.GetValue(i)) ?? "");

            var line = string.Join("  ", cells.Where(c => c.Length > 0));
            if (line.Length > 0)
                rows.Add(line);
        }

        return rows;
    }
}
