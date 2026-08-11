using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// The per-queue sequence counter. One row per (service, provider, queue), and the only row an enqueue
/// contends on.
/// </summary>
/// <remarks>
/// This exists instead of a native identity column because engine sequences hand out values at <b>insert</b>
/// time while rows become visible at <b>commit</b> time, and those orders differ. A receiver reading by a
/// <c>Seq &gt; cursor</c> watermark would step over a lower sequence that committed later and never deliver
/// it. Reserving the range under a row lock inside the enqueue transaction makes assignment order and commit
/// order the same order — a second enqueuer blocks until the first commits — and produces a gap-free
/// sequence, which is what makes the cursor exact.
/// </remarks>
public sealed class StreamSequenceDocument
{
    /// <summary><c>{serviceId}|{providerName}|{queueId}</c>.</summary>
    public string Id { get; set; } = null!;

    /// <summary>Document CAS. Unused on the locked path — the row lock is the concurrency control — but kept
    /// so an operator editing the row from the admin UI cannot silently clobber a live enqueue.</summary>
    public int Version { get; set; }

    /// <summary>The next sequence number to hand out.</summary>
    public long Next { get; set; }
}

/// <summary>
/// One enqueued batch. The Orleans-serialized <see cref="DocumentDbBatchContainer"/> rides in
/// <see cref="Payload"/>; the columns beside it exist to be queried — by the receiver (<c>QueueId</c> +
/// <c>Seq</c>) and by a human staring at a queue that will not drain (<c>StreamNamespace</c>,
/// <c>EnqueuedAt</c>).
/// </summary>
public sealed class StreamEventDocument
{
    /// <summary>Storage identity only (a v7 GUID). Ordering is <see cref="Seq"/>'s job — a time-based id
    /// would reorder under clock skew, which is the failure this design exists to avoid.</summary>
    [JsonPropertyName(StreamFields.Id)]
    public string Id { get; set; } = null!;

    [JsonPropertyName(StreamFields.QueueId)]
    public string QueueId { get; set; } = null!;

    /// <summary>Gap-free, commit-ordered position within <see cref="QueueId"/>. Indexed with it.</summary>
    [JsonPropertyName(StreamFields.Seq)]
    public long Seq { get; set; }

    /// <summary>The stream's namespace, denormalized out of the payload purely so the backlog is legible.</summary>
    [JsonPropertyName(StreamFields.StreamNamespace)]
    public string? StreamNamespace { get; set; }

    /// <summary>
    /// The full stream identity, denormalized so history can be replayed <b>per stream</b> without
    /// deserializing every batch on the queue. This is what makes rewind affordable, and it is why the
    /// events table carries a second index.
    /// </summary>
    [JsonPropertyName(StreamFields.StreamId)]
    public string StreamId { get; set; } = null!;

    /// <summary>Base64 of the Orleans-serialized batch container.</summary>
    [JsonPropertyName(StreamFields.Payload)]
    public string Payload { get; set; } = null!;

    [JsonPropertyName(StreamFields.EnqueuedAt)]
    public DateTimeOffset EnqueuedAt { get; set; }

    /// <summary>Set once the pulling agent confirms delivery. Drives retention and failover replay; it is a
    /// watermark, not a per-message state machine.</summary>
    [JsonPropertyName(StreamFields.DeliveredAt)]
    public DateTimeOffset? DeliveredAt { get; set; }
}

/// <summary>
/// The pinned wire names of <see cref="StreamEventDocument"/> — the stored JSON keys, which are <b>not</b> the
/// CLR property names. Anything addressing stream rows without a CLR type uses these rather than
/// <c>nameof</c>, which would produce <c>"QueueId"</c> where the stored key is <c>"queueId"</c>.
/// </summary>
/// <remarks>
/// ShinyDocDbMyAdmin's streams screen is the reason these are pinned and public. The admin tool deliberately
/// does <b>not</b> reference this package — pulling the Orleans runtime into a tool that already ships 152 MB
/// of native providers would be a poor trade for a screen that only needs to count rows — so it addresses the
/// same fields through the schema-free JSON-collection lane, and a guard test holds the two in step.
/// </remarks>
public static class StreamFields
{
    /// <summary>The document type name the events are stored under.</summary>
    public const string EventTypeName = nameof(StreamEventDocument);

    /// <summary>The document type name the per-queue counters are stored under.</summary>
    public const string SequenceTypeName = nameof(StreamSequenceDocument);

    /// <summary>The document type name the delivery checkpoints are stored under.</summary>
    public const string CheckpointTypeName = nameof(StreamCheckpointDocument);

    public const string Id = "id";
    public const string QueueId = "queueId";
    public const string Seq = "seq";
    public const string StreamId = "streamId";
    public const string StreamNamespace = "streamNamespace";
    public const string Payload = "payload";
    public const string EnqueuedAt = "enqueuedAt";
    public const string DeliveredAt = "deliveredAt";

    /// <summary>Every pinned name, for the guard test and for schema-free readers that enumerate them.</summary>
    public static IReadOnlyList<string> All { get; } =
    [
        Id, QueueId, Seq, StreamId, StreamNamespace, Payload, EnqueuedAt, DeliveredAt
    ];
}

/// <summary>
/// The persisted delivery cursor for one queue.
/// </summary>
/// <remarks>
/// The acknowledgement watermark on the event rows can already rebuild a cursor, but only while those rows
/// still exist — once retention sweeps a fully-delivered queue there is nothing left to derive it from, and
/// the rebuild degrades to "start from zero". That is harmless today (only undelivered rows remain) and
/// stops being harmless the moment history is retained for rewind, because a restart would replay it. This
/// row is the durable answer, and it also spares the failover path an aggregate over the events table.
/// </remarks>
public sealed class StreamCheckpointDocument
{
    /// <summary><c>{serviceId}|{providerName}|{queueId}</c>.</summary>
    public string Id { get; set; } = null!;

    /// <summary>Highest sequence acknowledged as delivered on this queue.</summary>
    public long Cursor { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }
}
