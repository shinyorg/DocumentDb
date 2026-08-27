using System.ComponentModel;
using System.Text.Json;
using Microsoft.Extensions.AI;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// The tools the assistant is given. Every read tool is always registered; write tools
/// (<c>insert_document</c>, <c>update_document</c>, <c>delete_document</c>) are only registered when
/// the connection's <see cref="AiConnectionSettings"/> has explicitly opted them in.
/// </summary>
/// <remarks>
/// <para>
/// Read-only is a property of this surface, not a rule the model is asked to follow. The write
/// paths that stay excluded even when writes are opted in - <c>ClearType</c>, <c>RestoreVersion</c>,
/// <c>DeleteBlob</c>, <c>ResyncVectorSidecar</c> - are simply never registered, so there is nothing
/// for the model to call and no prompt that could talk it into calling one. <c>ExecuteSql</c> is
/// left out for the same reason: its guard is a read-only <i>profile</i> flag plus a string check on
/// statement text, which is a weaker thing to stand on than "the function does not exist".
/// </para>
/// <para>
/// Writes are per-connection and opt-in on <see cref="AiConnectionSettings.AllowInsert"/>,
/// <see cref="AiConnectionSettings.AllowUpdate"/> and <see cref="AiConnectionSettings.AllowDelete"/>.
/// The underlying <see cref="DocumentAdminService"/> methods still enforce the profile's read-only
/// flag, so a connection marked read-only cannot be written to whatever the assistant is allowed.
/// </para>
/// <para>
/// <see cref="ReadOnlyToolNames"/> is the contract a test asserts against for the default surface, so
/// adding a write tool to the read-only path fails the build rather than quietly widening what the
/// assistant can do.
/// </para>
/// <para>
/// Reach is deliberately every connection, not just the one the chat was opened from: "is this order
/// id in staging too?" is exactly the question worth asking, and scoping it away would make the
/// assistant less useful without making it safer. Read tools reach every connection; write tools are
/// checked against the settings of the connection the write is targeted at, so opting writes in for
/// staging does not open production too.
/// </para>
/// <para>
/// <b>This surface never decrypts</b>, whatever encryption keys a connection profile carries. Field-level
/// encryption envelopes reach the model exactly as they are stored - as ciphertext - and that is the
/// point: a document body goes to a third-party model endpoint, so a decrypted SSN in it is a breach
/// while the base64 it was stored as is harmless. Every tool here goes through
/// <c>DocumentAdminService</c>'s raw read paths, none of which touch <c>EncryptionKeyRing</c>, so this is
/// a property of the code rather than a rule the model is asked to follow. Do not add a decrypting tool.
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
    /// The read-only tool set - what a connection with no write opt-ins is given. A test pins this
    /// list, so a write tool cannot be added to the read path without someone deciding to.
    /// </summary>
    public static readonly IReadOnlyList<string> ReadOnlyToolNames =
    [
        "list_connections",
        "list_tables",
        "list_types",
        "describe_type",
        "table_stats",
        "browse_documents",
        "get_document",
        "list_indexes",
        "search_full_text",
        "outbox_status"
    ];

    /// <summary>Backwards-compatible alias for <see cref="ReadOnlyToolNames"/>.</summary>
    [Obsolete("Use ReadOnlyToolNames.")]
    public static readonly IReadOnlyList<string> ToolNames = ReadOnlyToolNames;

    /// <summary>The tool name for insert, registered only when opted in.</summary>
    public const string InsertToolName = "insert_document";

    /// <summary>The tool name for update, registered only when opted in.</summary>
    public const string UpdateToolName = "update_document";

    /// <summary>The tool name for delete, registered only when opted in.</summary>
    public const string DeleteToolName = "delete_document";

    /// <summary>
    /// Builds the tools for a chat scope. <paramref name="writeScope"/> is the settings of the
    /// connection the conversation is opened against; its opt-in flags gate which write tools are
    /// registered. Reads always work against every connection.
    /// </summary>
    public IReadOnlyList<AITool> Build(AiConnectionSettings? writeScope = null)
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
            AIFunctionFactory.Create(this.SearchFullText, "search_full_text"),
            AIFunctionFactory.Create(this.OutboxStatus, "outbox_status")
        };

        if (writeScope is not null)
        {
            // A write tool is scoped to the connection its opt-in came from; a call for any other
            // connection is refused, which is what stops a chat opened against staging from
            // reaching over to production.
            var scopedProfile = writeScope.ProfileId;

            if (writeScope.AllowInsert)
                tools.Add(AIFunctionFactory.Create(
                    (string connectionId, string table, string typeName, string documentId, string json, CancellationToken ct)
                        => this.InsertDocument(scopedProfile, connectionId, table, typeName, documentId, json, ct),
                    InsertToolName,
                    "Creates one new document. The connectionId must be the connection this " +
                    "assistant conversation was opened against. The documentId is the id you want " +
                    "the new document to have. The json is the full document body as a JSON object. " +
                    "Fails if a document with that id already exists - use update_document then."));

            if (writeScope.AllowUpdate)
                tools.Add(AIFunctionFactory.Create(
                    (string connectionId, string table, string typeName, string documentId, string json, CancellationToken ct)
                        => this.UpdateDocument(scopedProfile, connectionId, table, typeName, documentId, json, ct),
                    UpdateToolName,
                    "Replaces the body of one existing document with the given JSON. The " +
                    "connectionId must be the connection this assistant conversation was opened " +
                    "against. Reads the current body first so you can confirm you are updating the " +
                    "right document. Fails if no document with that id exists - use insert_document " +
                    "then."));

            if (writeScope.AllowDelete)
                tools.Add(AIFunctionFactory.Create(
                    (string connectionId, string table, string typeName, string documentId, CancellationToken ct)
                        => this.DeleteDocument(scopedProfile, connectionId, table, typeName, documentId, ct),
                    DeleteToolName,
                    "Deletes one document by id. The connectionId must be the connection this " +
                    "assistant conversation was opened against. Only one id per call, on purpose - a " +
                    "bulk delete is a decision worth being explicit about."));
        }

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

    [Description("Reports the transactional outbox on a connection: how many messages are pending, " +
                 "waiting out a retry backoff, dead-lettered or delivered, how long the oldest pending " +
                 "message has been waiting, and the most common dead-letter failures. Answers 'is anything " +
                 "stuck?' and 'what is failing?' in one call. Returns null when the database has no outbox. " +
                 "A large pending count is normal on a busy system; an OLD oldest-pending is the signal that " +
                 "the application's outbox processor has stopped. You cannot requeue or deliver anything - " +
                 "that is a deliberate human action on the Outbox screen.")]
    async Task<OutboxStatusSummary?> OutboxStatus(
        [Description("Connection id from list_connections.")] string connectionId,
        [Description("Optional table name; defaults to the first outbox found on the connection.")] string? table = null,
        CancellationToken ct = default)
    {
        var outboxes = await admin.FindOutboxes(connectionId, ct);
        var located = string.IsNullOrWhiteSpace(table)
            ? outboxes.FirstOrDefault()
            : outboxes.FirstOrDefault(o => o.Table.Equals(table, StringComparison.OrdinalIgnoreCase));

        if (located is not { Addressable: true })
            return null;

        var health = await admin.GetOutboxHealth(connectionId, located, ct);
        var failures = health.DeadLettered > 0
            ? (await admin.GroupOutboxFailures(connectionId, located, ct)).Groups.Take(5).ToList()
            : [];

        return new OutboxStatusSummary(
            located.Table,
            health.Pending,
            health.Scheduled,
            health.DeadLettered,
            health.Processed,
            health.OldestPendingAt,
            [.. failures.Select(f => new OutboxFailureSummary(f.MessageType, f.ErrorSummary, f.Count))]);
    }

    // ── Writes (only registered when opted in on the connection's AiConnectionSettings) ─────────

    /// <summary>
    /// Refuses a write targeted at anything other than the connection the chat was opened against.
    /// The write opt-ins are a per-connection decision, and there is no meaningful way to opt them in
    /// for one connection and have the assistant carry them to another mid-chat.
    /// </summary>
    static void AssertScoped(string scopedProfileId, string requestedConnectionId)
    {
        if (!string.Equals(scopedProfileId, requestedConnectionId, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Writes are opted in for '{scopedProfileId}' only. Open a chat against '{requestedConnectionId}' to write there.");
    }

    async Task<string> InsertDocument(
        string scopedProfileId,
        string connectionId,
        string table,
        string typeName,
        string documentId,
        string json,
        CancellationToken ct)
    {
        AssertScoped(scopedProfileId, connectionId);
        if (string.IsNullOrWhiteSpace(documentId))
            throw new ArgumentException("A documentId is required.", nameof(documentId));

        var existing = await admin.GetDocument(connectionId, table, typeName, documentId, ct);
        if (existing is not null)
            throw new InvalidOperationException($"A '{typeName}' document with id '{documentId}' already exists. Use {UpdateToolName} instead.");

        await admin.SaveDocument(connectionId, table, typeName, documentId, json, isNew: true, ct: ct);
        return $"Inserted {typeName}/{documentId} in {table}.";
    }

    async Task<string> UpdateDocument(
        string scopedProfileId,
        string connectionId,
        string table,
        string typeName,
        string documentId,
        string json,
        CancellationToken ct)
    {
        AssertScoped(scopedProfileId, connectionId);
        if (string.IsNullOrWhiteSpace(documentId))
            throw new ArgumentException("A documentId is required.", nameof(documentId));

        var existing = await admin.GetDocument(connectionId, table, typeName, documentId, ct)
            ?? throw new InvalidOperationException($"No '{typeName}' document with id '{documentId}' exists. Use {InsertToolName} instead.");

        await admin.SaveDocument(connectionId, table, typeName, documentId, json, isNew: false, ct: ct);
        return $"Updated {typeName}/{documentId} in {table}.";
    }

    async Task<string> DeleteDocument(
        string scopedProfileId,
        string connectionId,
        string table,
        string typeName,
        string documentId,
        CancellationToken ct)
    {
        AssertScoped(scopedProfileId, connectionId);
        if (string.IsNullOrWhiteSpace(documentId))
            throw new ArgumentException("A documentId is required.", nameof(documentId));

        var removed = await admin.DeleteDocuments(connectionId, table, typeName, [documentId], ct);
        return removed == 0
            ? $"No '{typeName}' document with id '{documentId}' existed in {table}."
            : $"Deleted {typeName}/{documentId} from {table}.";
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

    public sealed record OutboxFailureSummary(string MessageType, string Error, int Count);

    public sealed record OutboxStatusSummary(
        string Table,
        long Pending,
        long Scheduled,
        long DeadLettered,
        long Processed,
        DateTimeOffset? OldestPendingAt,
        IReadOnlyList<OutboxFailureSummary> TopFailures);
}
