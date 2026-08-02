using System.ComponentModel;
using System.Text.Json;
using Microsoft.Extensions.AI;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// The tools the assistant is given. Every one is a <b>read</b> that this tool already performs
/// elsewhere in the UI.
/// </summary>
/// <remarks>
/// <para>
/// Read-only is a property of this surface, not a rule the model is asked to follow. The write
/// paths - <c>SaveDocument</c>, <c>DeleteDocuments</c>, <c>ClearType</c>, <c>RestoreVersion</c>,
/// <c>DeleteBlob</c>, <c>ResyncVectorSidecar</c> - are simply never registered, so there is nothing
/// for the model to call and no prompt that could talk it into calling one. <c>ExecuteSql</c> is
/// left out for the same reason: its guard is a read-only <i>profile</i> flag plus a string check on
/// statement text, which is a weaker thing to stand on than "the function does not exist".
/// </para>
/// <para>
/// <see cref="ToolNames"/> is the contract a test asserts against, so adding a write tool here fails
/// the build rather than quietly widening what the assistant can do.
/// </para>
/// <para>
/// Reach is deliberately every connection, not just the one the chat was opened from: "is this order
/// id in staging too?" is exactly the question worth asking, and scoping it away would make the
/// assistant less useful without making it safer - it can only ever read. That reach is what the
/// warning in the UI is about.
/// </para>
/// </remarks>
public sealed class AiToolSurface(DocumentAdminService admin, ProfileStore profiles)
{
    /// <summary>Rows any one tool call will return, however the model asks.</summary>
    public const int MaxRows = 50;

    /// <summary>
    /// Characters of document JSON returned per row. A single 400 KB document would otherwise fill
    /// the context window on its own and push out the conversation that asked for it.
    /// </summary>
    public const int MaxJsonChars = 4_000;

    /// <summary>
    /// The exact set of tools the assistant gets. Every entry reads; none writes. A test pins this
    /// list, so a write tool cannot be added without someone deciding to.
    /// </summary>
    public static readonly IReadOnlyList<string> ToolNames =
    [
        "list_connections",
        "list_tables",
        "list_types",
        "describe_type",
        "table_stats",
        "browse_documents",
        "get_document",
        "list_indexes",
        "search_full_text"
    ];

    /// <summary>Builds the tools. Cheap - the delegates close over the two services and hold no state.</summary>
    public IReadOnlyList<AITool> Build()
    {
        var tools = new List<AITool>
        {
            AIFunctionFactory.Create(this.ListConnections, "list_connections"),
            AIFunctionFactory.Create(this.ListTables, "list_tables"),
            AIFunctionFactory.Create(this.ListTypes, "list_types"),
            AIFunctionFactory.Create(this.DescribeType, "describe_type"),
            AIFunctionFactory.Create(this.TableStats, "table_stats"),
            AIFunctionFactory.Create(this.BrowseDocuments, "browse_documents"),
            AIFunctionFactory.Create(this.GetDocument, "get_document"),
            AIFunctionFactory.Create(this.ListIndexes, "list_indexes"),
            AIFunctionFactory.Create(this.SearchFullText, "search_full_text")
        };

        return tools;
    }

    // ── Tools ───────────────────────────────────────────────────────────

    [Description("Lists the database connections configured in this tool. Start here when you do not " +
                 "already know which connection to work against. Connection strings are never returned.")]
    async Task<IReadOnlyList<ConnectionSummary>> ListConnections(CancellationToken ct)
    {
        var all = await profiles.List(ct);

        // Name, provider and read-only status only. The connection string is a credential, and a
        // credential that reaches the model reaches the model's provider too.
        return [.. all.Select(p => new ConnectionSummary(p.Id, p.Name, p.Provider.ToString(), p.ReadOnly))];
    }

    [Description("Lists the tables on a connection, marking which carry documents and which are " +
                 "history/blob/spatial/vector sidecars.")]
    async Task<IReadOnlyList<TableSummary>> ListTables(
        [Description("Connection id from list_connections.")] string connectionId,
        CancellationToken ct)
    {
        var tables = await admin.ListTables(connectionId, refresh: false, ct);
        return [.. tables.Select(t => new TableSummary(t.Name, t.Role.ToString(), t.IsBrowsable))];
    }

    [Description("Lists the document types stored in a table, with how many documents each has. " +
                 "A 'type' is the CLR type name the application stored under.")]
    async Task<IReadOnlyList<TypeSummary>> ListTypes(
        [Description("Connection id from list_connections.")] string connectionId,
        [Description("Table name from list_tables.")] string table,
        CancellationToken ct)
    {
        var types = await admin.ListTypes(connectionId, table, ct);
        return [.. types.Select(t => new TypeSummary(t.TypeName, t.Count))];
    }

    [Description("Infers the shape of a document type by sampling documents: field paths, their JSON " +
                 "types, how often each is present, and an example value. The store is schema-free, so " +
                 "this describes what the sampled documents contain - it is not a contract.")]
    async Task<TypeSchema> DescribeType(
        [Description("Connection id from list_connections.")] string connectionId,
        [Description("Table name from list_tables.")] string table,
        [Description("Type name from list_types.")] string typeName,
        CancellationToken ct)
    {
        var schema = await admin.InferSchema(connectionId, table, typeName, ct: ct);
        return new TypeSchema(
            schema.TypeName,
            schema.SampleSize,
            [.. schema.Fields.Select(f => new SchemaFieldSummary(f.Path, f.Types, f.PercentPresent, f.Example))]);
    }

    [Description("Document and type counts plus JSON byte sizes for a table, optionally narrowed to one type.")]
    Task<TableStats> TableStats(
        [Description("Connection id from list_connections.")] string connectionId,
        [Description("Table name from list_tables.")] string table,
        [Description("Optional type name to narrow the counts to.")] string? typeName = null,
        CancellationToken ct = default)
        => admin.GetStats(connectionId, table, string.IsNullOrWhiteSpace(typeName) ? null : typeName, ct);

    [Description("Reads a page of documents, optionally filtered and sorted. Use describe_type first " +
                 "so the field paths you filter on actually exist. Results are capped - check the " +
                 "'truncated' and 'totalCount' fields before stating a total.")]
    async Task<DocumentResults> BrowseDocuments(
        [Description("Connection id from list_connections.")] string connectionId,
        [Description("Table name from list_tables.")] string table,
        [Description("Type name from list_types.")] string typeName,
        // Every optional argument carries a C# default. Without one, AIFunctionFactory marks a
        // nullable parameter as *required* in the generated schema, and the model would have to
        // supply a filter and a sort just to read a page.
        [Description("Optional field path to filter on: an envelope column (Id, CreatedAt, UpdatedAt) " +
                     "or a dotted path into the document body such as 'customer.tier'.")] string? filterPath = null,
        [Description("Filter comparison. One of: Equals, NotEquals, Contains, StartsWith, EndsWith, " +
                     "GreaterThan, GreaterOrEqual, LessThan, LessOrEqual, IsNull, IsNotNull.")] string? filterOperator = null,
        [Description("Value to compare against. Ignored for IsNull/IsNotNull.")] string? filterValue = null,
        [Description("Optional free-text search across the type's string fields.")] string? search = null,
        [Description("Optional field path to sort by. Defaults to UpdatedAt.")] string? sortPath = null,
        [Description("Sort descending. Defaults to true.")] bool? sortDescending = null,
        [Description("Rows to return, 1 to 50. Defaults to 25.")] int? limit = null,
        CancellationToken ct = default)
    {
        var take = Math.Clamp(limit ?? 25, 1, MaxRows);

        var filters = new List<BrowseFilter>();
        if (!string.IsNullOrWhiteSpace(filterPath))
        {
            if (!Enum.TryParse<FilterOperator>(filterOperator, ignoreCase: true, out var op))
                op = FilterOperator.Equals;

            filters.Add(new BrowseFilter(filterPath, op, filterValue ?? ""));
        }

        var sort = string.IsNullOrWhiteSpace(sortPath)
            ? new BrowseSort("UpdatedAt", true, true)
            : new BrowseSort(sortPath, sortDescending ?? true, DocumentAdminService.IsEnvelopeColumn(sortPath));

        // Searching needs to know which paths are text, and that comes from the inferred schema -
        // the same call the Browse grid makes before it offers a search box.
        IReadOnlyList<string> searchPaths = [];
        if (!string.IsNullOrWhiteSpace(search))
        {
            var schema = await admin.InferSchema(connectionId, table, typeName, ct: ct);
            searchPaths = DocumentAdminService.SearchableColumns(schema);
        }

        var page = await admin.Browse(connectionId, table, typeName, new BrowseQuery
        {
            Page = 1,
            PageSize = take,
            Sort = sort,
            Filters = filters,
            Search = search,
            SearchPaths = searchPaths
        }, ct);

        return new DocumentResults(
            [.. page.Rows.Select(Summarize)],
            page.TotalCount,
            page.TotalCount > page.Rows.Count);
    }

    [Description("Reads one document by its id.")]
    async Task<DocumentSummary?> GetDocument(
        [Description("Connection id from list_connections.")] string connectionId,
        [Description("Table name from list_tables.")] string table,
        [Description("Type name from list_types.")] string typeName,
        [Description("The document id.")] string documentId,
        CancellationToken ct)
    {
        var row = await admin.GetDocument(connectionId, table, typeName, documentId, ct);
        return row is null ? null : Summarize(row);
    }

    [Description("Lists the JSON property indexes DocumentDb has created on a table. Useful for " +
                 "explaining why a query would be fast or slow.")]
    async Task<IReadOnlyList<IndexSummary>> ListIndexes(
        [Description("Connection id from list_connections.")] string connectionId,
        [Description("Table name from list_tables.")] string table,
        CancellationToken ct)
    {
        var indexes = await admin.ListIndexes(connectionId, table, ct);
        return [.. indexes.Select(i => new IndexSummary(i.Name, i.TypeName, [.. i.Paths]))];
    }

    [Description("Runs a ranked full-text search over a type. Only works where the type actually has " +
                 "a full-text index; if it does not, say so rather than guessing.")]
    async Task<IReadOnlyList<SearchHit>> SearchFullText(
        [Description("Connection id from list_connections.")] string connectionId,
        [Description("Table name from list_tables.")] string table,
        [Description("Type name from list_types.")] string typeName,
        [Description("What to search for.")] string searchText,
        CancellationToken ct)
    {
        var results = await admin.SearchFullText(connectionId, table, typeName, searchText, maxResults: MaxRows, ct: ct);
        return [.. results.Hits.Select(h => new SearchHit(h.DocumentId, h.Score, Clip(h.Json)))];
    }

    // ── Shaping ─────────────────────────────────────────────────────────

    static DocumentSummary Summarize(DocumentRow row)
        => new(row.Id, row.CreatedAt, row.UpdatedAt, Clip(row.Json));

    /// <summary>
    /// Trims a body to the per-row budget, and says so in the text. A silently truncated document
    /// reads as a complete one, and the model would go on to describe fields that were cut.
    /// </summary>
    static string Clip(string json)
        => json.Length <= MaxJsonChars
            ? json
            : json[..MaxJsonChars] + $"\n… truncated at {MaxJsonChars} characters of {json.Length}.";

    // ── Result shapes ───────────────────────────────────────────────────
    // Deliberately narrow records rather than the internal models: what goes in here goes to the
    // model provider, so each field is one someone chose to send.

    public sealed record ConnectionSummary(string ConnectionId, string Name, string Provider, bool ReadOnly);
    public sealed record TableSummary(string Name, string Role, bool CarriesDocuments);
    public sealed record TypeSummary(string TypeName, long DocumentCount);
    public sealed record SchemaFieldSummary(string Path, string JsonTypes, int PercentPresent, string? Example);
    public sealed record TypeSchema(string TypeName, int SampleSize, IReadOnlyList<SchemaFieldSummary> Fields);
    public sealed record DocumentSummary(string Id, DateTimeOffset? CreatedAt, DateTimeOffset? UpdatedAt, string Json);
    public sealed record DocumentResults(IReadOnlyList<DocumentSummary> Documents, long TotalCount, bool Truncated);
    public sealed record IndexSummary(string Name, string TypeName, IReadOnlyList<string> Paths);
    public sealed record SearchHit(string DocumentId, double Score, string Json);
}
