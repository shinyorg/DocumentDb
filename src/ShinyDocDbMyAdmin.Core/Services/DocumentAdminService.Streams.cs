using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Orleans stream queues: how deep they are, whether they are draining, and which streams are stuck.
/// </summary>
/// <remarks>
/// <para>
/// Read-only, and deliberately so. The outbox screen can requeue because an outbox message is a unit of work
/// someone owns; a stream event is a <i>position in a sequence</i> that every subscriber holds a cursor into.
/// Deleting one would tear a hole in a gap-free sequence, and re-dating one would reorder delivery — so there
/// is no safe operator action here beyond looking. A stuck stream is fixed on the consumer side.
/// </para>
/// <para>
/// Like the outbox screen this needs no new SQL and no schema knowledge: everything below is a <c>Count</c>,
/// an <c>OrderBy</c>/<c>Paginate</c> on the schema-free JSON-collection lane. Relational only, for the same
/// reason the filter console is.
/// </para>
/// <para>
/// Field paths come from <see cref="StreamWireFields"/>, which mirrors the library's <c>StreamFields</c>
/// rather than referencing it — see the note there for why, and the guard test that keeps the two in step.
/// </para>
/// </remarks>
public sealed partial class DocumentAdminService
{
    /// <summary>Rows a stuck-stream grouping will materialise before it stops.</summary>
    public const int StreamStuckScanLimit = 2000;

    // ── Discovery ───────────────────────────────────────────────────────

    /// <summary>
    /// Every Orleans stream table in this database, found by scanning each browsable table's type list. The
    /// provider defaults to an <c>orleans_streams</c> table but the name is configurable, so nothing here is
    /// coupled to that default.
    /// </summary>
    public async Task<IReadOnlyList<StreamLocation>> FindStreamProviders(string profileId, CancellationToken ct = default)
    {
        var tables = await this.ListTables(profileId, refresh: false, ct);
        var found = new List<StreamLocation>();

        foreach (var table in tables.Where(t => t.IsBrowsable))
        {
            foreach (var type in await this.ListTypes(profileId, table.Name, ct))
            {
                if (IsStreamEventTypeName(type.TypeName))
                    found.Add(new StreamLocation(table.Name, type.TypeName, type.Count, IsAddressable(type.TypeName)));
            }
        }

        return found;
    }

    // The stored type name is written through TypeNameResolution, so match the short and the dotted form and
    // carry the stored one forward — the same rule the outbox discovery uses.
    static bool IsStreamEventTypeName(string typeName)
        => typeName.Equals(StreamWireFields.EventTypeName, StringComparison.Ordinal)
        || typeName.EndsWith("." + StreamWireFields.EventTypeName, StringComparison.Ordinal);

    // ── Reads ───────────────────────────────────────────────────────────

    /// <summary>Totals across every queue in this provider's table.</summary>
    public async Task<StreamHealth> GetStreamHealth(string profileId, StreamLocation location, CancellationToken ct = default)
    {
        using var store = await this.OpenStore(profileId, location.Table, ct);
        var collection = OpenCollection(store, location.TypeName);

        var depth = await collection.Query().Where(UndeliveredFilter).Count(ct);
        var retained = await collection.Query().Where(DeliveredFilter).Count(ct);

        var oldest = await collection.Query()
            .Where(UndeliveredFilter)
            .OrderBy($"{StreamWireFields.Seq}:long")
            .Paginate(0, 1)
            .FirstOrDefault(ct);

        var queues = await this.ListStreamQueues(profileId, location, ct);
        return new StreamHealth(depth, retained, queues.Count, ReadDate(oldest, StreamWireFields.EnqueuedAt));
    }

    /// <summary>
    /// Per-queue depth, retained history and age.
    /// </summary>
    /// <remarks>
    /// The queue list comes from the counter rows rather than from the events, so a queue that is drained (or
    /// has never carried traffic) still appears at depth zero instead of vanishing from the view — which is
    /// exactly the queue you want to see when you are asking why nothing is arriving.
    /// </remarks>
    public async Task<IReadOnlyList<StreamQueueRow>> ListStreamQueues(string profileId, StreamLocation location, CancellationToken ct = default)
    {
        using var store = await this.OpenStore(profileId, location.Table, ct);

        var queueIds = await this.ReadQueueIds(store, location, ct);
        var collection = OpenCollection(store, location.TypeName);
        var rows = new List<StreamQueueRow>(queueIds.Count);

        foreach (var queueId in queueIds)
        {
            var undelivered = WhereValue(collection.Query().Where(UndeliveredFilter), QueueIdPrefix, queueId);
            var depth = await undelivered.Count(ct);
            var retained = await WhereValue(collection.Query().Where(DeliveredFilter), QueueIdPrefix, queueId).Count(ct);

            var oldest = await WhereValue(collection.Query().Where(UndeliveredFilter), QueueIdPrefix, queueId)
                .OrderBy($"{StreamWireFields.Seq}:long")
                .Paginate(0, 1)
                .FirstOrDefault(ct);

            rows.Add(new StreamQueueRow(queueId, depth, retained, ReadDate(oldest, StreamWireFields.EnqueuedAt)));
        }

        return [.. rows.OrderBy(r => r.QueueId, StringComparer.Ordinal)];
    }

    /// <summary>
    /// Queue ids, taken from the counter rows when they are addressable and otherwise recovered from the
    /// events themselves.
    /// </summary>
    /// <remarks>
    /// The counter rows are the authoritative list, but they are a different <c>TypeName</c> and a store
    /// written with <c>TypeNameResolution.FullName</c> makes that name unaddressable through the collection
    /// lane. Falling back to the distinct queue ids present in the events keeps the screen useful there, at
    /// the cost of not showing an idle queue.
    /// </remarks>
    async Task<IReadOnlyList<string>> ReadQueueIds(IDocumentStore store, StreamLocation location, CancellationToken ct)
    {
        var counterType = location.TypeName.Contains('.', StringComparison.Ordinal)
            ? location.TypeName[..(location.TypeName.LastIndexOf('.') + 1)] + StreamWireFields.SequenceTypeName
            : StreamWireFields.SequenceTypeName;

        if (IsAddressable(counterType))
        {
            try
            {
                var counters = await store.Collection(counterType).Query().Paginate(0, 512).ToList(ct);
                var ids = counters
                    .Select(c => ReadString(c, "id"))
                    .Where(id => !string.IsNullOrEmpty(id))
                    // "{serviceId}|{providerName}|{queueId}" — the queue id is the tail.
                    .Select(id => id![(id.LastIndexOf('|') + 1)..])
                    .Where(id => id.Length > 0)
                    .Distinct(StringComparer.Ordinal)
                    .ToList();

                if (ids.Count > 0)
                    return ids;
            }
            catch (Exception ex) when (ex is InvalidOperationException or ArgumentException or NotSupportedException)
            {
                // No counter rows addressable here — fall through to deriving the list from the events.
            }
        }

        var collection = OpenCollection(store, location.TypeName);
        var sample = await collection.Query()
            .OrderBy($"{StreamWireFields.Seq}:long desc")
            .Paginate(0, StreamStuckScanLimit)
            .ToList(ct);

        return [.. sample
            .Select(row => ReadString(row, StreamWireFields.QueueId))
            .Where(id => !string.IsNullOrEmpty(id))
            .Select(id => id!)
            .Distinct(StringComparer.Ordinal)];
    }

    /// <summary>
    /// Streams with undelivered events older than the cut-off, worst first.
    /// </summary>
    /// <remarks>
    /// Grouped client-side: <c>IJsonDocumentQuery</c> has no <c>GroupBy</c> (that lives on the typed lane), so
    /// the scan is bounded by <see cref="StreamStuckScanLimit"/> and the caller is told when it was truncated
    /// rather than shown a total that quietly is not one.
    /// </remarks>
    public async Task<(IReadOnlyList<StuckStreamRow> Streams, bool Truncated)> FindStuckStreams(
        string profileId,
        StreamLocation location,
        TimeSpan olderThan,
        int take = 50,
        CancellationToken ct = default)
    {
        using var store = await this.OpenStore(profileId, location.Table, ct);
        var collection = OpenCollection(store, location.TypeName);
        var cutoff = DateTimeOffset.UtcNow - olderThan;

        var rows = await WhereValue(collection.Query().Where(UndeliveredFilter), EnqueuedBeforePrefix, cutoff)
            .OrderBy($"{StreamWireFields.Seq}:long")
            .Paginate(0, StreamStuckScanLimit)
            .ToList(ct);

        var grouped = rows
            .Select(ToEventRow)
            .Where(e => e.EnqueuedAt != null)
            .GroupBy(e => e.StreamId, StringComparer.Ordinal)
            .Select(g => new StuckStreamRow(
                g.Key,
                g.First().StreamNamespace,
                g.First().QueueId,
                g.Count(),
                g.Min(e => e.EnqueuedAt!.Value)))
            .OrderBy(s => s.OldestUndeliveredAt)
            .Take(take)
            .ToList();

        return (grouped, rows.Count >= StreamStuckScanLimit);
    }

    /// <summary>One page of a queue's backlog, oldest first — what will not drain.</summary>
    public async Task<IReadOnlyList<StreamEventRow>> ListStreamBacklog(
        string profileId,
        StreamLocation location,
        string queueId,
        int take = 100,
        CancellationToken ct = default)
    {
        using var store = await this.OpenStore(profileId, location.Table, ct);
        var collection = OpenCollection(store, location.TypeName);

        var query = collection.Query().Where(UndeliveredFilter);
        if (!string.IsNullOrWhiteSpace(queueId))
            query = WhereValue(query, QueueIdPrefix, queueId);

        var rows = await query
            .OrderBy($"{StreamWireFields.Seq}:long")
            .Paginate(0, Math.Clamp(take, 1, StreamStuckScanLimit))
            .ToList(ct);

        return [.. rows.Select(ToEventRow)];
    }

    // ── Predicates ──────────────────────────────────────────────────────

    // Constant interpolations of the pinned wire names, so those stay the single source of truth. The
    // :datetimeoffset / :long hints are required — the collection lane is schema-free, so an unhinted path
    // compares as text and both the date predicates and the sequence ordering would silently be wrong
    // (lexicographic ordering puts sequence 10 before 9).
    const string UndeliveredFilter = $"{StreamWireFields.DeliveredAt} is null";
    const string DeliveredFilter = $"{StreamWireFields.DeliveredAt} is not null";
    // No leading "and": successive Where calls are AND-ed by the query builder, exactly as on the outbox screen.
    const string QueueIdPrefix = $"{StreamWireFields.QueueId} == ";
    const string EnqueuedBeforePrefix = $"{StreamWireFields.EnqueuedAt}:datetimeoffset < ";

    static StreamEventRow ToEventRow(JsonObject row) => new(
        ReadString(row, StreamWireFields.Id) ?? "",
        ReadString(row, StreamWireFields.QueueId) ?? "",
        ReadLong(row, StreamWireFields.Seq),
        ReadString(row, StreamWireFields.StreamId) ?? "",
        ReadString(row, StreamWireFields.StreamNamespace),
        ReadDate(row, StreamWireFields.EnqueuedAt),
        ReadDate(row, StreamWireFields.DeliveredAt),
        ReadString(row, StreamWireFields.Payload)?.Length ?? 0);

    static long ReadLong(JsonObject row, string field)
        => row[field] is JsonValue value && value.TryGetValue<long>(out var number) ? number : 0;
}
