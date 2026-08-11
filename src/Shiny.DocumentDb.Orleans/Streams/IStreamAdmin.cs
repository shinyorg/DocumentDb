namespace Shiny.DocumentDb.Orleans;

/// <summary>One queue's operational state.</summary>
/// <param name="QueueId">The Orleans queue.</param>
/// <param name="Depth">Undelivered events sitting on the queue — the number to alert on.</param>
/// <param name="Cursor">Highest sequence acknowledged as delivered.</param>
/// <param name="HighWaterMark">Highest sequence enqueued. <c>HighWaterMark - Cursor</c> is the lag.</param>
/// <param name="OldestUndeliveredAt">
/// When the oldest undelivered event was enqueued, or null when the queue is drained. This — not the depth —
/// is what separates "busy" from "the pulling agent is dead": a deep queue that is draining has a recent
/// timestamp here, a stalled one does not move.
/// </param>
/// <param name="RetainedCount">Delivered events still on the table, i.e. how much history rewind can reach.</param>
public record StreamQueueStatus(
    string QueueId,
    int Depth,
    long Cursor,
    long HighWaterMark,
    DateTimeOffset? OldestUndeliveredAt,
    int RetainedCount)
{
    /// <summary>How far behind the queue's consumer is, in sequence positions.</summary>
    public long Lag => Math.Max(0, this.HighWaterMark - this.Cursor);
}

/// <summary>One stream with undelivered events — the granularity an incident is actually about.</summary>
public record StuckStream(
    string StreamId,
    string? StreamNamespace,
    string QueueId,
    int UndeliveredCount,
    DateTimeOffset OldestUndeliveredAt);

/// <summary>
/// The in-process operational view of the DocumentDb stream provider: how deep are the queues, which streams
/// are not draining, how much history is retained.
/// </summary>
/// <remarks>
/// Read-only by design. The outbox's admin surface can requeue and purge because an outbox message is a unit
/// of work someone owns; a stream event is a position in a sequence that consumers hold cursors into, so
/// there is no safe "retry this one" — deleting or reordering it would silently break every subscriber's
/// token. Draining a stuck stream is a consumer-side fix, not a storage-side one.
/// </remarks>
public interface IStreamAdmin
{
    /// <summary>Per-queue depth, lag and retained history.</summary>
    Task<IReadOnlyList<StreamQueueStatus>> Queues(CancellationToken cancellationToken = default);

    /// <summary>Total undelivered events across every queue.</summary>
    Task<int> TotalDepth(CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams with undelivered events older than <paramref name="olderThan"/>, worst first. The default of
    /// five minutes is a starting point, not a rule — set it above your slowest legitimate consumer.
    /// </summary>
    Task<IReadOnlyList<StuckStream>> StuckStreams(TimeSpan? olderThan = null, int take = 50, CancellationToken cancellationToken = default);

    /// <summary>The undelivered events on one queue, oldest first — for looking at what will not drain.</summary>
    Task<IReadOnlyList<StreamEventDocument>> Backlog(string queueId, int take = 100, CancellationToken cancellationToken = default);
}
