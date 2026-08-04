using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// The outbox queue: how deep it is, whether it is draining, what is dead-lettered and why — and the one thing
/// an operator can actually do about it, which is requeue.
/// </summary>
/// <remarks>
/// <para>
/// Outbox messages are ordinary documents, so this needs no new SQL, no provider code and no schema knowledge:
/// every operation below is a <c>Count</c>, an <c>OrderBy</c>/<c>Paginate</c>, an <c>ExecuteUpdate</c> or an
/// <c>ExecuteDelete</c> on the same schema-free JSON-collection lane the filter console uses. Relational only,
/// for the same reason the filter console is.
/// </para>
/// <para>
/// <b>This tool cannot dispatch.</b> Delivery is <c>IOutboxDispatcher</c>, an application-side implementation
/// over a transport this process has no access to. Requeue makes a message <i>eligible</i>; the application's
/// running processor delivers it. A "dispatch now" button would be a lie of the same shape as the tool
/// pretending it can run interceptors.
/// </para>
/// <para>
/// Field paths are the pinned wire names from <see cref="OutboxFields"/> — <b>not</b> <c>nameof</c>, which
/// would produce "ProcessedAt" where the stored key is "processedAt". The library pins them with
/// <c>[JsonPropertyName]</c> and a guard test holds them there precisely so this screen can address them.
/// </para>
/// </remarks>
public sealed partial class DocumentAdminService
{
    /// <summary>Rows a dead-letter grouping will materialise before it stops and says so.</summary>
    public const int OutboxFailureScanLimit = 500;

    // ── Discovery ───────────────────────────────────────────────────────

    /// <summary>
    /// Every outbox in this database, found by scanning each browsable table's type list rather than assuming a
    /// table name. The library defaults to a dedicated <c>outbox</c> table, but the name is configurable and an
    /// older store may have the messages in a shared one — so nothing here is coupled to that default.
    /// </summary>
    public async Task<IReadOnlyList<OutboxLocation>> FindOutboxes(string profileId, CancellationToken ct = default)
    {
        var tables = await this.ListTables(profileId, refresh: false, ct);
        var found = new List<OutboxLocation>();

        foreach (var table in tables.Where(t => t.IsBrowsable))
        {
            // ListTypes is one GROUP BY and the table list is cached per profile, so this stays cheap enough
            // to run on every page that wants to know whether to show an outbox link.
            foreach (var type in await this.ListTypes(profileId, table.Name, ct))
            {
                if (IsOutboxTypeName(type.TypeName))
                    found.Add(new OutboxLocation(table.Name, type.TypeName, type.Count, IsAddressable(type.TypeName)));
            }
        }

        return found;
    }

    /// <summary>
    /// The stored type name is written through <c>TypeNameResolution</c>, so it is either the short name or the
    /// full one. Matching both — and carrying the <i>stored</i> name forward — keeps this working either way.
    /// </summary>
    static bool IsOutboxTypeName(string typeName)
        => typeName.Equals(nameof(OutboxMessage), StringComparison.Ordinal)
        || typeName.EndsWith("." + nameof(OutboxMessage), StringComparison.Ordinal);

    // Collection names are interpolated into DDL and validated to ^[A-Za-z_][A-Za-z0-9_]{0,127}$, so a dotted
    // type name (TypeNameResolution.FullName) can be browsed but never addressed through the collection lane.
    static bool IsAddressable(string typeName) => !typeName.Contains('.', StringComparison.Ordinal);

    // ── Reads ───────────────────────────────────────────────────────────

    /// <summary>The four bucket counts plus the age signal.</summary>
    public async Task<OutboxHealth> GetOutboxHealth(string profileId, OutboxLocation outbox, CancellationToken ct = default)
    {
        using var store = await this.OpenStore(profileId, outbox.Table, ct);
        var collection = OpenCollection(store, outbox.TypeName);
        var now = DateTimeOffset.UtcNow;

        var pending = await State(collection, OutboxState.Pending, now).Count(ct);
        var scheduled = await State(collection, OutboxState.Scheduled, now).Count(ct);
        var dead = await State(collection, OutboxState.DeadLettered, now).Count(ct);
        var processed = await State(collection, OutboxState.Processed, now).Count(ct);

        var oldest = await State(collection, OutboxState.Pending, now)
            .OrderBy($"{OutboxFields.CreatedAt}:datetimeoffset")
            .Paginate(0, 1)
            .FirstOrDefault(ct);

        return new OutboxHealth(pending, scheduled, dead, processed, ReadDate(oldest, OutboxFields.CreatedAt));
    }

    /// <summary>One page of the grid.</summary>
    public async Task<IReadOnlyList<OutboxEntry>> ListOutbox(
        string profileId,
        OutboxLocation outbox,
        OutboxFilter filter,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(filter);

        using var store = await this.OpenStore(profileId, outbox.Table, ct);
        var collection = OpenCollection(store, outbox.TypeName);
        var now = DateTimeOffset.UtcNow;

        var query = Filtered(collection, filter, now)
            // Newest first: during an incident the interesting rows are the recent ones, and the ids are
            // time-ordered so this needs no extra column.
            .OrderBy($"{OutboxFields.Id} desc")
            .Paginate(Math.Max(0, filter.Offset), Math.Clamp(filter.Take, 1, OutboxFailureScanLimit));

        var rows = await query.ToList(ct);
        return [.. rows.Select(row => ToEntry(row, now))];
    }

    /// <summary>How many rows the current filter matches, ignoring the page window.</summary>
    public async Task<long> CountOutbox(string profileId, OutboxLocation outbox, OutboxFilter filter, CancellationToken ct = default)
    {
        using var store = await this.OpenStore(profileId, outbox.Table, ct);
        var collection = OpenCollection(store, outbox.TypeName);
        return await Filtered(collection, filter, DateTimeOffset.UtcNow).Count(ct);
    }

    /// <summary>
    /// Dead letters collapsed by message type and error, which is the view that finds the one poison consumer.
    /// Grouped client-side: <c>IJsonDocumentQuery</c> has no <c>GroupBy</c> (that lives on the typed lane), so
    /// the scan is capped and the cap is reported rather than silently truncating.
    /// </summary>
    public async Task<OutboxFailureReport> GroupOutboxFailures(string profileId, OutboxLocation outbox, CancellationToken ct = default)
    {
        using var store = await this.OpenStore(profileId, outbox.Table, ct);
        var collection = OpenCollection(store, outbox.TypeName);

        var rows = await State(collection, OutboxState.DeadLettered, DateTimeOffset.UtcNow)
            .OrderBy($"{OutboxFields.Id} desc")
            .Paginate(0, OutboxFailureScanLimit)
            .ToList(ct);

        var groups = rows
            .Select(row => new
            {
                MessageType = ReadString(row, OutboxFields.MessageType) ?? "(unknown)",
                Summary = OutboxErrors.Summarize(ReadString(row, OutboxFields.Error)),
                Id = ReadString(row, OutboxFields.Id) ?? ""
            })
            .GroupBy(x => (x.MessageType, x.Summary))
            .Select(g => new OutboxFailureGroup(g.Key.MessageType, g.Key.Summary, g.Count(), g.First().Id))
            .OrderByDescending(g => g.Count)
            .ThenBy(g => g.MessageType, StringComparer.Ordinal)
            .ToList();

        return new OutboxFailureReport(groups, rows.Count, rows.Count >= OutboxFailureScanLimit);
    }

    // ── Writes ──────────────────────────────────────────────────────────

    /// <summary>
    /// Makes the named dead letters eligible again. Returns how many were requeued.
    /// </summary>
    /// <remarks>
    /// The dead-letter guard stays in the predicate even though the caller named ids explicitly: requeueing a
    /// merely <i>scheduled</i> message would reset its backoff, and requeueing a <i>processed</i> one would
    /// redeliver a business event that already happened. Neither is something the UI should be the only thing
    /// preventing.
    /// </remarks>
    public Task<int> RequeueOutbox(string profileId, OutboxLocation outbox, IEnumerable<string> ids, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(ids);
        var list = ids.Where(id => !string.IsNullOrWhiteSpace(id)).Distinct(StringComparer.Ordinal).ToList();
        return list.Count == 0
            ? Task.FromResult(0)
            : this.RequeueCore(profileId, outbox, list, messageType: null, ct);
    }

    /// <summary>Requeues every dead letter, optionally narrowed to one message type.</summary>
    public Task<int> RequeueAllDeadLettered(string profileId, OutboxLocation outbox, string? messageType = null, CancellationToken ct = default)
        => this.RequeueCore(profileId, outbox, ids: null, messageType, ct);

    async Task<int> RequeueCore(string profileId, OutboxLocation outbox, IReadOnlyList<string>? ids, string? messageType, CancellationToken ct)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        using var store = await this.OpenStore(profileId, outbox.Table, ct);
        var collection = OpenCollection(store, outbox.TypeName);
        var now = DateTimeOffset.UtcNow;

        var query = State(collection, OutboxState.DeadLettered, now);
        if (ids != null)
            query = query.Where(IdListFilter(ids));
        if (!string.IsNullOrWhiteSpace(messageType))
            query = WhereValue(query, MessageTypePrefix, messageType);

        // Set-based, so no interceptors run and no temporal version is written — which is correct and needs no
        // RecordVersion call: the library's own ExecuteUpdate behaves exactly the same way. Do not "fix" this.
        var affected = await query.ExecuteUpdate(new Dictionary<string, object?>
        {
            [OutboxFields.DeadLetteredAt] = null,
            [OutboxFields.Error] = null,
            [OutboxFields.Attempts] = 0,
            [OutboxFields.AvailableAt] = now
        }, ct);

        logger.LogInformation("Requeued {Count} outbox message(s) in {Table}", affected, outbox.Table);
        return affected;
    }

    /// <summary>
    /// Deletes acknowledged messages older than the cut-off. Structurally incapable of touching a pending or
    /// dead-lettered row — only a delivered message has a <c>processedAt</c> at all.
    /// </summary>
    public async Task<int> PurgeProcessedOutbox(string profileId, OutboxLocation outbox, DateTimeOffset olderThan, CancellationToken ct = default)
    {
        var connection = await this.Connect(profileId, ct);
        connection.AssertWritable();

        using var store = await this.OpenStore(profileId, outbox.Table, ct);
        var collection = OpenCollection(store, outbox.TypeName);

        var affected = await WhereValue(collection.Query(), ProcessedBeforePrefix, olderThan).ExecuteDelete(ct);

        logger.LogInformation("Purged {Count} processed outbox message(s) from {Table}", affected, outbox.Table);
        return affected;
    }

    // ── Predicates ──────────────────────────────────────────────────────

    // The four filter texts. Written as constant interpolations of OutboxFields so the pinned wire names stay
    // the single source of truth, and held as constants so they are plain strings by the time a query sees
    // them. The :datetimeoffset hint is required: the collection lane is schema-free, so an unhinted path compares as
    // text and every date predicate would quietly return the wrong rows.
    const string UndeliveredPrefix = $"{OutboxFields.ProcessedAt} is null and {OutboxFields.DeadLetteredAt} is null and {OutboxFields.AvailableAt}:datetimeoffset ";
    const string PendingPrefix = UndeliveredPrefix + "<= ";
    const string ScheduledPrefix = UndeliveredPrefix + "> ";
    const string DeadLetteredFilter = $"{OutboxFields.DeadLetteredAt} is not null";
    const string ProcessedFilter = $"{OutboxFields.ProcessedAt} is not null";
    const string ProcessedBeforePrefix = $"{ProcessedFilter} and {OutboxFields.ProcessedAt}:datetimeoffset < ";
    const string MessageTypePrefix = $"{OutboxFields.MessageType} == ";
    const string PartitionKeyPrefix = $"{OutboxFields.PartitionKey} == ";

    /// <summary>
    /// Applies a filter whose path text is fixed and whose value is bound as a parameter.
    /// </summary>
    /// <remarks>
    /// The handler is built by hand rather than written as <c>Where($"…")</c> because an interpolated string
    /// literal would capture <i>every</i> hole as a parameter — including the <see cref="OutboxFields"/>
    /// constants, which have to reach the parser as literal path text. Values still never touch the filter
    /// text, which is the property that matters.
    /// </remarks>
    static IJsonDocumentQuery WhereValue(IJsonDocumentQuery query, string prefix, object? value)
    {
        var handler = new FilterInterpolatedStringHandler(prefix.Length, 1);
        handler.AppendLiteral(prefix);
        handler.AppendFormatted(value);
        return query.Where(handler);
    }

    // State is derived, not stored: three canned filters plus a residual.
    static IJsonDocumentQuery State(IJsonDocumentCollection collection, OutboxState state, DateTimeOffset now) => state switch
    {
        OutboxState.Pending => WhereValue(collection.Query(), PendingPrefix, now),
        OutboxState.Scheduled => WhereValue(collection.Query(), ScheduledPrefix, now),
        OutboxState.DeadLettered => collection.Query().Where(DeadLetteredFilter),
        _ => collection.Query().Where(ProcessedFilter)
    };

    static IJsonDocumentQuery Filtered(IJsonDocumentCollection collection, OutboxFilter filter, DateTimeOffset now)
    {
        var query = filter.State is { } state ? State(collection, state, now) : collection.Query();

        if (!string.IsNullOrWhiteSpace(filter.MessageType))
            query = WhereValue(query, MessageTypePrefix, filter.MessageType);
        if (!string.IsNullOrWhiteSpace(filter.PartitionKey))
            query = WhereValue(query, PartitionKeyPrefix, filter.PartitionKey);

        return query;
    }

    /// <summary>
    /// An <c>in (…)</c> list of ids as literal filter text. The grammar's IN list needs a comma-separated set,
    /// which an interpolation hole cannot produce — so the ids are written in directly, and every one is
    /// checked to be a plain identifier first. They come from rows this screen just read, so a value that
    /// fails the check means something is wrong and saying so beats silently requeueing a subset.
    /// </summary>
    static string IdListFilter(IReadOnlyList<string> ids)
    {
        foreach (var id in ids)
        {
            foreach (var c in id)
            {
                if (!char.IsAsciiLetterOrDigit(c) && c != '-' && c != '_')
                    throw new InvalidOperationException(
                        $"Outbox id '{id}' is not a plain identifier, so it cannot be requeued from this screen. Use the SQL console.");
            }
        }

        return $"{OutboxFields.Id} in ({string.Join(", ", ids.Select(id => $"'{id}'"))})";
    }

    // ── Row shaping ─────────────────────────────────────────────────────

    static OutboxEntry ToEntry(JsonObject row, DateTimeOffset now)
    {
        var processedAt = ReadDate(row, OutboxFields.ProcessedAt);
        var deadLetteredAt = ReadDate(row, OutboxFields.DeadLetteredAt);
        var availableAt = ReadDate(row, OutboxFields.AvailableAt);

        var state = deadLetteredAt != null ? OutboxState.DeadLettered
            : processedAt != null ? OutboxState.Processed
            : availableAt > now ? OutboxState.Scheduled
            : OutboxState.Pending;

        return new OutboxEntry(
            ReadString(row, OutboxFields.Id) ?? "",
            ReadString(row, OutboxFields.MessageType) ?? "(unknown)",
            ReadString(row, OutboxFields.PartitionKey),
            ReadDate(row, OutboxFields.CreatedAt),
            availableAt,
            ReadInt(row, OutboxFields.Attempts),
            state,
            ReadString(row, OutboxFields.Error),
            ReadHeader(row, OutboxFields.TraceParentHeader),
            ReadPayload(row));
    }

    static string? ReadString(JsonObject? row, string field)
        => row?[field] is JsonValue value && value.TryGetValue<string>(out var text) ? text : null;

    static int ReadInt(JsonObject row, string field)
        => row[field] is JsonValue value && value.TryGetValue<int>(out var number) ? number : 0;

    static DateTimeOffset? ReadDate(JsonObject? row, string field)
        => ReadString(row, field) is { } text && DateTimeOffset.TryParse(text, out var parsed) ? parsed : null;

    static string? ReadHeader(JsonObject row, string key)
        => row[OutboxFields.Headers] is JsonObject headers ? ReadString(headers, key) : null;

    // The payload is stored as a JSON string, so the grid gets it back as text and has to parse it to show
    // anything structured. A payload the application wrote as something other than JSON is shown verbatim.
    static JsonNode? ReadPayload(JsonObject row)
    {
        var payload = ReadString(row, OutboxFields.Payload);
        if (payload == null)
            return null;

        try
        {
            return JsonNode.Parse(payload);
        }
        catch (System.Text.Json.JsonException)
        {
            return JsonValue.Create(payload);
        }
    }
}
