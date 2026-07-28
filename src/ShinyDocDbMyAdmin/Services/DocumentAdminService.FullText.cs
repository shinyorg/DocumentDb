using System.Data.Common;
using System.Diagnostics;
using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Internal;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Full-text search from the admin tool.
/// </summary>
/// <remarks>
/// <para>
/// Unlike the vector sidecar, there is <b>nothing to keep in sync here</b>. Every relational provider's
/// full-text index is maintained by the engine itself — FTS5 triggers on the documents table, a
/// generated or computed column, an on-commit CONTEXT index — so a write issued by this tool updates
/// the index the same way a write from the library does. The one exception is DuckDB, whose index is a
/// snapshot the library rebuilds before each query rather than something the engine maintains, and that
/// is reported rather than papered over.
/// </para>
/// <para>
/// Searching needs no registered mapping: every provider's <c>BuildFullTextSearchSql</c> derives the
/// index table or column from the table and type name alone. Only PostgreSQL reads the mapped language,
/// for its <c>to_tsquery</c> regconfig — which is why the caller can pick one.
/// </para>
/// </remarks>
public sealed partial class DocumentAdminService
{
    /// <summary>Rows a full-text search returns before it stops.</summary>
    public const int FullTextResultLimit = 100;

    /// <summary>
    /// Establishes whether a type has a full-text index, without needing the mapping that created it.
    /// </summary>
    public async Task<FullTextIndexInfo> DescribeFullTextIndex(
        string profileId,
        string table,
        string typeName,
        CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        var provider = connection.Provider;
        var safeTable = Ado.SafeIdentifier(table);

        if (!provider.SupportsFullText)
            return new FullTextIndexInfo(false, false, false, null);

        var probe = provider.BuildFullTextProbeSql(safeTable, typeName);
        if (probe is null)
            return new FullTextIndexInfo(true, false, provider.FullTextIndexRequiresRebuild, "This provider does not expose a way to detect its full-text index.");

        try
        {
            var present = await connection.Execute(async (db, token) =>
            {
                await using var cmd = Ado.Command(db, probe);
                await using var reader = await cmd.ExecuteReaderAsync(token);
                return await reader.ReadAsync(token);
            }, ct);

            return new FullTextIndexInfo(true, present, provider.FullTextIndexRequiresRebuild, null);
        }
        catch (DbException ex)
        {
            return new FullTextIndexInfo(true, false, provider.FullTextIndexRequiresRebuild, ex.Message);
        }
    }

    /// <summary>
    /// Runs the provider's ranked full-text query and returns the matching documents with their scores.
    /// </summary>
    /// <param name="language">
    /// Only PostgreSQL reads this (it selects the <c>to_tsquery</c> regconfig, which must match the one
    /// the index was built with or the query silently matches nothing). Every other provider ignores it.
    /// </param>
    public async Task<FullTextResults> SearchFullText(
        string profileId,
        string table,
        string typeName,
        string searchText,
        FullTextLanguage language = FullTextLanguage.English,
        int maxResults = FullTextResultLimit,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(searchText))
            throw new ArgumentException("Enter something to search for.", nameof(searchText));

        var connection = await this.Connect(profileId, ct);
        var provider = connection.Provider;
        var safeTable = Ado.SafeIdentifier(table);
        var mapping = SyntheticFullTextMapping(language);

        var (sql, parameters) = provider.BuildFullTextSearchSql(
            safeTable, typeName, mapping, searchText, Math.Clamp(maxResults, 1, FullTextResultLimit), additionalWhere: null);

        var stopwatch = Stopwatch.StartNew();
        var hits = await connection.Execute(async (db, token) =>
        {
            await using var cmd = Ado.Command(db, sql);
            Ado.Bind(cmd, "@typeName", typeName);
            foreach (var (name, value) in parameters)
                Ado.Bind(cmd, name, value);

            await using var reader = await cmd.ExecuteReaderAsync(token);
            var results = new List<FullTextHit>();
            while (await reader.ReadAsync(token))
            {
                // The contract is two columns: the document body, then a numeric score where higher is
                // more relevant.
                var json = Ado.Text(reader, 0);
                var score = reader.FieldCount > 1 && !reader.IsDBNull(1)
                    ? Convert.ToDouble(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture)
                    : 0d;

                results.Add(new FullTextHit(IdOf(json), score, json));
            }

            return (IReadOnlyList<FullTextHit>)results;
        }, ct);

        stopwatch.Stop();
        return new FullTextResults(hits, stopwatch.Elapsed, sql);
    }

    static string IdOf(string json)
    {
        try
        {
            if (JsonNode.Parse(json) is JsonObject obj)
            {
                var key = obj.ContainsKey("Id") ? "Id" : "id";
                if (obj.TryGetPropertyValue(key, out var value) && value is not null)
                    return value.ToString();
            }
        }
        catch (System.Text.Json.JsonException)
        {
            // A body that will not parse still has a score worth showing; it just has no id to label it.
        }

        return "";
    }

    /// <summary>
    /// Stands in for the <c>FullTextMapping</c> that <c>MapFullTextProperty&lt;T&gt;</c> would have
    /// produced.
    /// </summary>
    /// <remarks>
    /// Safe for searching and <b>only</b> for searching. Every provider's search SQL derives the index
    /// identifier from the table and type name and reads nothing else off the mapping except
    /// PostgreSQL's language — which is supplied. <c>JsonPaths</c> is deliberately empty because nothing
    /// on the search path reads it; anything that builds or rebuilds an index does, which is exactly why
    /// this tool cannot do that.
    /// </remarks>
    static FullTextMapping SyntheticFullTextMapping(FullTextLanguage language) => new()
    {
        DocumentType = typeof(object),
        PropertyNames = [],
        JsonPaths = [],
        Language = language,
        GetTexts = _ => []
    };
}
