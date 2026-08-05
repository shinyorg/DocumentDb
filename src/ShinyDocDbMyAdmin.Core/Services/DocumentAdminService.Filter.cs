using System.Diagnostics;
using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// The other console: DocumentDb's own string query grammar instead of the target database's SQL.
/// </summary>
/// <remarks>
/// <para>
/// This is the one place the tool goes <b>up</b> to <c>IDocumentStore</c> rather than down to
/// <c>IDatabaseProvider</c>. Everywhere else it has no CLR type to bind to and so cannot use the
/// library's API — but <c>store.Collection(name)</c> is the schema-free lane, which has no CLR type
/// either. The row keying lines up exactly: a collection's name <i>is</i> the <c>TypeName</c> column
/// this tool already browses by, so "the Order type in the documents table" and
/// <c>Collection("Order")</c> are the same set of rows.
/// </para>
/// <para>
/// The payoff is that the grammar is not reimplemented here. <c>Where("total:number &gt; 100")</c>
/// parses, lowers and translates through the same code the application does, so what you see in this
/// console is what your code would get — including the generated SQL, which
/// <see cref="IJsonDocumentQuery.ToQueryString"/> hands back and the console shows.
/// </para>
/// </remarks>
public sealed partial class DocumentAdminService
{
    /// <summary>Rows the filter console will materialise before it stops and says so.</summary>
    public const int FilterRowLimit = 500;

    /// <summary>
    /// Runs a string-grammar query and returns the documents plus the SQL it compiled to.
    /// </summary>
    public async Task<FilterResult> ExecuteFilter(
        string profileId,
        string table,
        string typeName,
        FilterQuery query,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        var take = Math.Clamp(query.Take, 1, FilterRowLimit);
        var stopwatch = Stopwatch.StartNew();

        // Disposed per run rather than cached: SQLite and DuckDB hold the database file open for the
        // life of a store, and an admin tool has no business keeping that lock between clicks - the
        // same reason AdminConnection opens and closes around every operation.
        using var store = await this.OpenStore(profileId, table, ct);

        var collection = OpenCollection(store, typeName);
        var built = Build(collection.Query(), query);

        string sql;
        IReadOnlyDictionary<string, object?> parameters;
        try
        {
            var text = built.ToQueryString();
            sql = text.Sql;
            parameters = text.Parameters;
        }
        catch (Exception ex)
        {
            // Showing the SQL is a convenience; failing to render it must not lose the results.
            sql = $"-- the provider could not render this query as text: {ex.Message}";
            parameters = new Dictionary<string, object?>();
        }

        IReadOnlyList<JsonObject> rows = query.Project is { Length: > 0 }
            ? await built.Project(query.Project).ToList(ct)
            : await built.ToList(ct);

        // Counted without the page so the footer can say "20 of 4,312" rather than "20".
        var total = await Build(collection.Query(), query with { Offset = 0, Take = 0 }).Count(ct);

        stopwatch.Stop();
        return new FilterResult(
            Columns(rows),
            rows,
            sql,
            parameters,
            stopwatch.Elapsed,
            total,
            rows.Count >= take);
    }

    /// <summary>
    /// Applies the console's boxes to a query. Empty boxes are left off entirely rather than passed as
    /// empty strings, so an unfiltered run produces the plain <c>WHERE TypeName = @typeName</c> the
    /// browse grid would.
    /// </summary>
    static IJsonDocumentQuery Build(IJsonDocumentQuery query, FilterQuery filter)
    {
        if (!string.IsNullOrWhiteSpace(filter.Where))
            query = query.Where(filter.Where);

        if (!string.IsNullOrWhiteSpace(filter.OrderBy))
            query = query.OrderBy(filter.OrderBy);

        if (filter.Take > 0)
            query = query.Paginate(Math.Max(0, filter.Offset), Math.Clamp(filter.Take, 1, FilterRowLimit));

        return query;
    }

    /// <summary>
    /// Opens the schema-free collection for a type name, translating the two ways it can legitimately
    /// fail into something a user can act on.
    /// </summary>
    static IJsonDocumentCollection OpenCollection(IDocumentStore store, string typeName)
    {
        try
        {
            return store.Collection(typeName);
        }
        catch (ArgumentException ex)
        {
            // Collection names are validated to ^[A-Za-z_][A-Za-z0-9_]{0,127}$ because they are
            // interpolated into DDL. A store configured with TypeNameResolution.FullName writes dotted
            // type names, which this tool can browse but the collection lane will not address.
            throw new InvalidOperationException(
                $"'{typeName}' is not a usable collection name, so the filter console cannot address it: " +
                $"{ex.Message} Type names containing dots come from TypeNameResolution.FullName. " +
                $"Use the SQL console for this type.", ex);
        }
        catch (NotSupportedException ex)
        {
            throw new InvalidOperationException(
                $"This backend does not support JSON collections, so the filter console is unavailable: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// A store over one table that will not create anything. <c>SkipTableInitialization</c> matters
    /// here: every store operation, reads included, otherwise runs <c>CREATE TABLE IF NOT EXISTS</c>
    /// once per table — which a read-only connection has promised not to do and a restricted database
    /// account would refuse outright.
    /// </summary>
    async Task<DocumentStore> OpenStore(string profileId, string table, CancellationToken ct)
    {
        var connection = await this.Connect(profileId, ct);

        return new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = connection.Provider,
            TableName = Ado.SafeIdentifier(table),
            SkipTableInitialization = true
        });
    }

    /// <summary>
    /// The field paths a filter touches that have no index, so the console can offer to create one.
    /// </summary>
    /// <remarks>
    /// Read from the query text rather than the plan: engines name the object they scanned, not the
    /// JSON path inside it, so the plan can say "this was a full scan" but not "…because
    /// <c>customer.name</c> is unindexed". Pairing the two — the plan for whether it matters, the
    /// filter text for what to do about it — is more useful than either alone, and the plan is always
    /// shown so the suggestion is never the only evidence.
    /// </remarks>
    public async Task<IReadOnlyList<string>> SuggestIndexPaths(
        string profileId,
        string table,
        string typeName,
        FilterQuery query,
        CancellationToken ct = default)
    {
        var referenced = FilterPaths.Extract(query.Where).Concat(FilterPaths.Extract(query.OrderBy)).Distinct(StringComparer.Ordinal);
        if (!referenced.Any())
            return [];

        var indexed = (await this.ListIndexes(profileId, table, ct))
            .Where(i => i.TypeName.Equals(typeName, StringComparison.Ordinal))
            .SelectMany(i => i.Paths)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return [.. referenced.Where(p => !indexed.Contains(p)).OrderBy(p => p, StringComparer.Ordinal)];
    }

    /// <summary>
    /// Warnings for encrypted paths a filter expression references — one per path, worded for the mode the
    /// sample could prove.
    /// </summary>
    /// <remarks>
    /// This console compiles to SQL directly, so the library's <c>EncryptedPredicateRewriter</c> never runs
    /// and the constant you type is never encrypted on the way to the database. That is invisible from the
    /// outside: the query succeeds, returns nothing, and reads as "there is no such document". Saying so is
    /// the whole point - the console cannot make the predicate work, only stop it from lying.
    /// </remarks>
    public async Task<IReadOnlyList<string>> EncryptionWarnings(
        string profileId,
        string table,
        string typeName,
        FilterQuery query,
        CancellationToken ct = default)
    {
        var referenced = FilterPaths.Extract(query.Where)
            .Concat(FilterPaths.Extract(query.OrderBy))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (referenced.Count == 0)
            return [];

        var schema = await this.InferSchema(profileId, table, typeName, ct: ct);

        return
        [
            .. schema.Fields
                .Where(f => f.Encryption is not null && referenced.Contains(f.Path, StringComparer.OrdinalIgnoreCase))
                .Select(f => f.Encryption!.DeterministicObserved
                    ? $"'{f.Path}' is deterministically encrypted: only exact-ciphertext equality can match. " +
                      "Copy the ciphertext from a row."
                    : $"'{f.Path}' is encrypted; a predicate over it cannot match. The console compiles to SQL " +
                      "directly, so it does not encrypt your constant the way the library's LINQ path does.")
        ];
    }

    /// <summary>
    /// The union of top-level keys across the rows, in first-seen order. The result of a filter is
    /// whatever the documents happen to hold, so the grid's columns are discovered the same way the
    /// browse grid's are.
    /// </summary>
    static IReadOnlyList<string> Columns(IReadOnlyList<JsonObject> rows)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var columns = new List<string>();

        foreach (var row in rows)
        {
            foreach (var (key, _) in row)
            {
                if (seen.Add(key))
                    columns.Add(key);
            }
        }

        return columns;
    }
}
