namespace ShinyDocDbMyAdmin.Models;

/// <summary>
/// The pinned wire names of the Orleans stream provider's event document.
/// </summary>
/// <remarks>
/// Duplicated from <c>Shiny.DocumentDb.Orleans.StreamFields</c> rather than referenced. Taking a project
/// reference would pull the whole Orleans runtime into a tool that already ships ~152 MB of native database
/// providers, to power a screen that only counts rows and reads two timestamps. A guard test in the admin
/// test project — which <i>can</i> reference the package, because tests are never packed — asserts these
/// strings still match the library's, so the duplication cannot drift silently.
/// </remarks>
public static class StreamWireFields
{
    public const string EventTypeName = "StreamEventDocument";
    public const string SequenceTypeName = "StreamSequenceDocument";
    public const string CheckpointTypeName = "StreamCheckpointDocument";

    public const string Id = "id";
    public const string QueueId = "queueId";
    public const string Seq = "seq";
    public const string StreamId = "streamId";
    public const string StreamNamespace = "streamNamespace";
    public const string Payload = "payload";
    public const string EnqueuedAt = "enqueuedAt";
    public const string DeliveredAt = "deliveredAt";
}

/// <summary>Where an Orleans stream provider's events live in this database.</summary>
/// <param name="Table">The documents table holding the events.</param>
/// <param name="TypeName">The <c>TypeName</c> value as stored — short or dotted, depending on the
/// application's <c>TypeNameResolution</c>.</param>
/// <param name="Count">How many event rows the table holds, delivered or not.</param>
/// <param name="Addressable">
/// False when <paramref name="TypeName"/> is dotted: collection names are interpolated into DDL and validated,
/// so a dotted name can be browsed but not queried through the JSON-collection lane.
/// </param>
public sealed record StreamLocation(string Table, string TypeName, long Count, bool Addressable);

/// <summary>One queue's operational state, as the screen shows it.</summary>
/// <param name="Depth">Undelivered events — the number to alert on.</param>
/// <param name="Retained">Delivered events still present, i.e. how far back a rewind can reach.</param>
/// <param name="OldestUndeliveredAt">
/// When the oldest undelivered event was enqueued. This, not the depth, is what separates "busy" from
/// "the pulling agent is dead" — a draining queue's oldest keeps moving, a stalled one's does not.
/// </param>
public sealed record StreamQueueRow(
    string QueueId,
    long Depth,
    long Retained,
    DateTimeOffset? OldestUndeliveredAt);

/// <summary>A stream whose events are not draining.</summary>
public sealed record StuckStreamRow(
    string StreamId,
    string? StreamNamespace,
    string QueueId,
    int UndeliveredCount,
    DateTimeOffset OldestUndeliveredAt);

/// <summary>One event row, flattened out of the JSON body for the grid.</summary>
public sealed record StreamEventRow(
    string Id,
    string QueueId,
    long Seq,
    string StreamId,
    string? StreamNamespace,
    DateTimeOffset? EnqueuedAt,
    DateTimeOffset? DeliveredAt,
    int PayloadBytes)
{
    public bool Delivered => this.DeliveredAt != null;
}

/// <summary>Totals across every queue in one provider's table.</summary>
public sealed record StreamHealth(
    long Depth,
    long Retained,
    int QueueCount,
    DateTimeOffset? OldestUndeliveredAt);
