using Microsoft.Extensions.Logging;
using global::Orleans.Serialization;
using global::Orleans.Streams;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// The read half of the provider. Orleans guarantees one pulling agent per queue per cluster, so this reads
/// by an in-memory cursor and takes <b>no locks and no per-message writes</b> — the single decision that
/// makes a database-backed stream affordable. The only writes on the read path are the delivery watermark
/// and the retention sweep.
/// </summary>
public sealed class DocumentDbQueueReceiver : IQueueAdapterReceiver
{
    readonly QueueId queueId;
    readonly string queueKey;
    readonly string checkpointId;
    readonly IDocumentStore store;
    readonly DocumentDbStreamOptions options;
    readonly Serializer<DocumentDbBatchContainer> serializer;
    readonly ILogger logger;
    readonly TimeProvider clock;
    readonly StreamChangeNudge? nudge;
    readonly Action onNudge;

    long cursor;
    DateTimeOffset nextQueryAt;
    TimeSpan backoff;
    DateTimeOffset nextPurgeAt;
    volatile bool nudged;
    bool shuttingDown;

    internal DocumentDbQueueReceiver(
        QueueId queueId,
        string checkpointId,
        IDocumentStore store,
        DocumentDbStreamOptions options,
        Serializer<DocumentDbBatchContainer> serializer,
        StreamChangeNudge? nudge,
        ILogger logger,
        TimeProvider clock)
    {
        this.queueId = queueId;
        this.queueKey = queueId.ToString();
        this.checkpointId = checkpointId;
        this.store = store;
        this.options = options;
        this.serializer = serializer;
        this.nudge = nudge;
        this.logger = logger;
        this.clock = clock;
        this.backoff = options.PollInterval;
        this.onNudge = () => this.nudged = true;
    }

    public async Task Initialize(TimeSpan timeout)
    {
        // Failover: a new agent picks up where the previous one's acknowledgements got to. Anything delivered
        // but not acknowledged is redelivered, which is Orleans' at-least-once contract, not a compromise.
        //
        // The persisted checkpoint is authoritative and the acknowledgement watermark is the fallback, in
        // that order. Deriving from the rows alone was correct only while retention swept everything
        // delivered; now that history is retained for rewind, a queue whose events are all delivered would
        // rebuild a cursor of zero and replay its whole retained history on restart.
        var checkpoint = await this.store.Get<StreamCheckpointDocument>(this.checkpointId).ConfigureAwait(false);
        if (checkpoint != null)
        {
            this.cursor = checkpoint.Cursor;
        }
        else
        {
            var lastAcked = await this.Query()
                .Where(e => e.DeliveredAt != null)
                .OrderByDescending(e => e.Seq)
                .FirstOrDefault()
                .ConfigureAwait(false);
            this.cursor = lastAcked?.Seq ?? 0;
        }

        this.nextPurgeAt = this.clock.GetUtcNow() + this.options.PurgeInterval;
        this.nudge?.Subscribe(this.onNudge);

        this.logger.LogInformation("Stream queue {QueueId} resuming from sequence {Cursor}", this.queueId, this.cursor);
    }

    /// <summary>
    /// Persists the delivery cursor. Best-effort: the acknowledgement watermark on the rows is written first
    /// and is what correctness rests on, so a failed checkpoint costs a longer replay on the next failover,
    /// never a lost or duplicated event.
    /// </summary>
    async Task Checkpoint(long cursor, DateTimeOffset now)
    {
        try
        {
            await this.store.Upsert(new StreamCheckpointDocument
            {
                Id = this.checkpointId,
                Cursor = cursor,
                UpdatedAt = now
            }).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.logger.LogWarning(ex, "Failed to persist the delivery checkpoint for stream queue {QueueId}", this.queueId);
        }
    }

    // The queue key is copied to a local in every query below rather than referenced through `this`: the
    // expression translator lowers captured locals, and reaching through a field is the kind of thing that
    // works on one provider's visitor and not another's.
    IDocumentQuery<StreamEventDocument> Query()
    {
        var key = this.queueKey;
        return this.store.Query<StreamEventDocument>().Where(e => e.QueueId == key);
    }

    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        if (this.shuttingDown)
            return [];

        var now = this.clock.GetUtcNow();

        // The pulling agent's timer fires on Orleans' schedule (100ms by default) whether or not there is
        // anything to read. Backing off here — rather than querying every time — is what keeps an idle
        // cluster from charging the database for its own silence.
        if (now < this.nextQueryAt && !this.nudged)
            return [];

        this.nudged = false;
        var take = maxCount <= 0 ? this.options.BatchSize : Math.Min(maxCount, this.options.BatchSize);
        var from = this.cursor;

        var rows = await this.Query()
            .Where(e => e.Seq > from)
            .OrderBy(e => e.Seq)
            .Paginate(0, take)
            .ToList()
            .ConfigureAwait(false);

        this.Reschedule(rows.Count, now);
        await this.PurgeIfDue(now).ConfigureAwait(false);

        if (rows.Count == 0)
            return [];

        var containers = new List<IBatchContainer>(rows.Count);
        foreach (var row in rows)
        {
            var container = this.serializer.Deserialize(Convert.FromBase64String(row.Payload));
            // The stored sequence is authoritative: it is what the cursor and the consumer's token both mean.
            container.SequenceNumber = row.Seq;
            containers.Add(container);
        }

        this.cursor = rows[^1].Seq;
        return containers;
    }

    void Reschedule(int rowCount, DateTimeOffset now)
    {
        this.backoff = rowCount == 0
            // Doubling, capped: an idle queue settles at MaxPollInterval instead of hammering.
            ? Min(this.backoff + this.backoff, this.options.MaxPollInterval)
            // Anything delivered means the queue is live — drop straight back to the floor rather than easing
            // down, or a burst arriving after a quiet spell would be metered out at idle latency.
            : this.options.PollInterval;

        this.nextQueryAt = now + this.backoff;
    }

    static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;

    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        if (messages == null || messages.Count == 0)
            return;

        var highest = messages
            .OfType<DocumentDbBatchContainer>()
            .Select(m => m.SequenceNumber)
            .DefaultIfEmpty(0L)
            .Max();

        if (highest == 0)
            return;

        // A watermark, not per-message bookkeeping: one range update per delivered set, which is why an
        // acknowledgement costs a single statement regardless of how many events it covers.
        var now = this.clock.GetUtcNow();
        await this.Query()
            .Where(e => e.Seq <= highest && e.DeliveredAt == null)
            .ExecuteUpdate(e => e.DeliveredAt!, now)
            .ConfigureAwait(false);

        // Watermark first, checkpoint second — the checkpoint is a shortcut for the failover path, not the
        // source of truth, so it must never be ahead of what the rows say was delivered.
        await this.Checkpoint(highest, now).ConfigureAwait(false);
    }

    /// <summary>
    /// Retention sweep for this queue only. No leader election and no coordination: each queue has exactly one
    /// receiver, so the receiver already owns every row it deletes.
    /// </summary>
    async Task PurgeIfDue(DateTimeOffset now)
    {
        if (this.options.Retention is not { } retention || now < this.nextPurgeAt)
            return;

        this.nextPurgeAt = now + this.options.PurgeInterval;
        var cutoff = now - retention;
        try
        {
            var deleted = await this.Query()
                .Where(e => e.DeliveredAt != null && e.DeliveredAt < cutoff)
                .ExecuteDelete()
                .ConfigureAwait(false);

            if (deleted > 0 && this.logger.IsEnabled(LogLevel.Debug))
                this.logger.LogDebug("Purged {Count} delivered events from stream queue {QueueId}", deleted, this.queueId);
        }
        catch (Exception ex)
        {
            // Retention is housekeeping. Failing it must never fail a poll, or a disk problem becomes a
            // stalled stream instead of a growing table.
            this.logger.LogWarning(ex, "Retention sweep failed for stream queue {QueueId}", this.queueId);
        }
    }

    public Task Shutdown(TimeSpan timeout)
    {
        this.shuttingDown = true;
        this.nudge?.Unsubscribe(this.onNudge);
        return Task.CompletedTask;
    }
}
