using Microsoft.Extensions.Logging;
using global::Orleans.Providers.Streams.Common;
using global::Orleans.Runtime;
using global::Orleans.Serialization;
using global::Orleans.Streams;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// A queue cache with <b>durable rewind</b>: the live window is an ordinary in-memory
/// <see cref="SimpleQueueCache"/>, and a cursor asking for a position older than that window is served by
/// replaying the events table instead of failing.
/// </summary>
/// <remarks>
/// <para>
/// This is the capability that separates a database-backed stream provider from a queue-backed one. Orleans'
/// own caches answer a too-old token with <see cref="QueueCacheMissException"/> because behind them sits a
/// queue that has already handed the message over; behind this one sits a table that still has the row. So
/// the cache miss is caught and turned into a read.
/// </para>
/// <para>
/// The rewind window is therefore <b>exactly</b> <c>DocumentDbStreamOptions.Retention</c> — the sweep that
/// bounds table growth is the same thing that bounds how far back a subscriber can resume. Beyond it the
/// original <see cref="QueueCacheMissException"/> is what the caller gets, which is the honest answer.
/// </para>
/// </remarks>
sealed class DocumentDbQueueCache : IQueueCache
{
    readonly SimpleQueueCache live;
    readonly QueueId queueId;
    readonly IDocumentStore store;
    readonly DocumentDbStreamOptions options;
    readonly Serializer<DocumentDbBatchContainer> serializer;
    readonly ILogger logger;

    public DocumentDbQueueCache(
        QueueId queueId,
        int cacheSize,
        IDocumentStore store,
        DocumentDbStreamOptions options,
        Serializer<DocumentDbBatchContainer> serializer,
        ILogger logger)
    {
        this.queueId = queueId;
        this.live = new SimpleQueueCache(cacheSize, logger);
        this.store = store;
        this.options = options;
        this.serializer = serializer;
        this.logger = logger;
    }

    public void AddToCache(IList<IBatchContainer> messages) => this.live.AddToCache(messages);

    public bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems) => this.live.TryPurgeFromCache(out purgedItems);

    public bool IsUnderPressure() => this.live.IsUnderPressure();

    public int GetMaxAddCount() => this.live.GetMaxAddCount();

    public void UpdateDeliveryProgress(StreamSequenceToken token, DateTime utcNow)
    {
        // Nothing to do: delivery progress is persisted by the receiver's acknowledgement watermark, which is
        // durable, rather than tracked in the cache, which is not.
    }

    /// <summary>
    /// A null token means "from now" and is served by the live cache. Any other token is a resume request,
    /// and those go through storage first.
    /// </summary>
    /// <remarks>
    /// Catching <see cref="QueueCacheMissException"/> from the live cache would be the tidier-looking hook and
    /// it is wrong: an <b>empty</b> cache — the state after every silo restart, which is exactly when a
    /// subscriber resumes — does not raise a miss, it hands back a cursor that yields nothing. The rewind
    /// would then silently deliver no history at all, which is worse than failing.
    /// <para>
    /// So a resume always starts in storage. When storage has nothing past the token the history cursor hands
    /// straight over to the live cache, so a consumer that is already up to date pays one bounded indexed read
    /// and nothing else.
    /// </para>
    /// </remarks>
    public IQueueCacheCursor GetCacheCursor(StreamId streamId, StreamSequenceToken token)
    {
        if (token == null)
            return this.live.GetCacheCursor(streamId, token);

        this.logger.LogInformation(
            "Stream {StreamId} resuming from sequence {Sequence} on queue {QueueId}; replaying from storage",
            streamId, token.SequenceNumber, this.queueId);

        return new DocumentDbHistoryCursor(
            streamId,
            token,
            this.queueId,
            this.store,
            this.options,
            this.serializer,
            this.live,
            this.logger);
    }
}

/// <summary>
/// Replays one stream's history out of the events table, then hands the caller back to the live in-memory
/// cache once it has caught up.
/// </summary>
/// <remarks>
/// <para>
/// The awkward part of this class is that <see cref="IQueueCacheCursor"/> is a synchronous interface over
/// what is now an I/O-backed sequence. Returning <c>false</c> from <see cref="MoveNext"/> while a page loads
/// is not safe: the pulling agent treats <c>false</c> as "nothing more for now" and will not necessarily come
/// back until new data is enqueued, which on a quiet stream may be never — the rewind would simply stall.
/// </para>
/// <para>
/// So the first page is fetched by blocking, on the thread pool (<c>Task.Run(...).GetAwaiter().GetResult()</c>
/// — off the Orleans scheduler, so a scheduler thread waits on a database read rather than deadlocking on
/// itself), and every page after that is <b>prefetched in the background</b> the moment the current one is
/// handed out. In the steady state the next page is already there when the cursor reaches it. This costs a
/// blocked thread on a rare, bounded operation, which is the right side of the trade against a rewind that
/// silently never finishes.
/// </para>
/// </remarks>
sealed class DocumentDbHistoryCursor : IQueueCacheCursor
{
    readonly StreamId streamId;
    readonly string streamKey;
    readonly string queueKey;
    readonly IDocumentStore store;
    readonly DocumentDbStreamOptions options;
    readonly Serializer<DocumentDbBatchContainer> serializer;
    readonly SimpleQueueCache live;
    readonly ILogger logger;

    long position;
    List<DocumentDbBatchContainer> page = [];
    int index = -1;
    Task<List<DocumentDbBatchContainer>>? prefetch;
    IQueueCacheCursor? handover;
    Exception? error;
    bool exhausted;

    public DocumentDbHistoryCursor(
        StreamId streamId,
        StreamSequenceToken token,
        QueueId queueId,
        IDocumentStore store,
        DocumentDbStreamOptions options,
        Serializer<DocumentDbBatchContainer> serializer,
        SimpleQueueCache live,
        ILogger logger)
    {
        this.streamId = streamId;
        this.streamKey = streamId.ToString();
        this.queueKey = queueId.ToString();
        this.store = store;
        this.options = options;
        this.serializer = serializer;
        this.live = live;
        this.logger = logger;
        // A rewind token means "resume after this position", matching the live cursor's semantics. A null
        // token would not have missed the cache, so it cannot reach here.
        this.position = token?.SequenceNumber ?? 0;
    }

    public bool MoveNext()
    {
        if (this.handover != null)
            return this.handover.MoveNext();

        if (this.exhausted)
            return false;

        if (this.index + 1 < this.page.Count)
        {
            this.index++;
            return true;
        }

        return this.LoadNextPage() && this.MoveNext();
    }

    bool LoadNextPage()
    {
        List<DocumentDbBatchContainer> next;
        try
        {
            next = this.prefetch != null
                ? this.prefetch.GetAwaiter().GetResult()
                : Task.Run(() => this.Fetch(this.position)).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            // Surfaced through GetCurrent, which is the only channel this interface has for an error.
            this.error = ex;
            this.exhausted = true;
            this.logger.LogError(ex, "Failed to replay history for stream {StreamId}", this.streamId);
            return false;
        }
        finally
        {
            this.prefetch = null;
        }

        if (next.Count == 0)
        {
            // Caught up with what is on disk — hand back to the live cache from the last replayed position.
            this.exhausted = true;
            this.TryHandover();
            return this.handover != null;
        }

        this.page = next;
        this.index = -1;
        this.position = next[^1].SequenceNumber;
        // Start the following page now, while the caller consumes this one.
        this.prefetch = Task.Run(() => this.Fetch(this.position));
        return true;
    }

    void TryHandover()
    {
        try
        {
            this.handover = this.live.GetCacheCursor(this.streamId, new EventSequenceTokenV2(this.position));
        }
        catch (QueueCacheMissException)
        {
            // Storage and the in-memory window do not overlap — everything between them was purged while the
            // replay was running. Nothing is recoverable, so the cursor simply ends rather than skipping ahead
            // and pretending the gap was delivered.
            this.logger.LogWarning(
                "Stream {StreamId} history replay could not rejoin the live cache at sequence {Position}; the gap was purged mid-replay",
                this.streamId, this.position);
        }
    }

    async Task<List<DocumentDbBatchContainer>> Fetch(long after)
    {
        var queue = this.queueKey;
        var stream = this.streamKey;
        var rows = await this.store.Query<StreamEventDocument>()
            .Where(e => e.QueueId == queue && e.StreamId == stream && e.Seq > after)
            .OrderBy(e => e.Seq)
            .Paginate(0, this.options.BatchSize)
            .ToList()
            .ConfigureAwait(false);

        var containers = new List<DocumentDbBatchContainer>(rows.Count);
        foreach (var row in rows)
        {
            var container = this.serializer.Deserialize(Convert.FromBase64String(row.Payload));
            container.SequenceNumber = row.Seq;
            containers.Add(container);
        }
        return containers;
    }

    public IBatchContainer? GetCurrent(out Exception exception)
    {
        if (this.handover != null)
            return this.handover.GetCurrent(out exception);

        exception = this.error!;
        return this.error != null || this.index < 0 || this.index >= this.page.Count
            ? null
            : this.page[this.index];
    }

    public void Refresh(StreamSequenceToken token)
    {
        this.handover?.Refresh(token);
    }

    public void RecordDeliveryFailure()
    {
        // Step back one so the failed batch is re-delivered rather than skipped — the same contract the live
        // cursor honours.
        if (this.handover != null)
            this.handover.RecordDeliveryFailure();
        else if (this.index >= 0)
            this.index--;
    }

    public void Dispose()
    {
        (this.handover as IDisposable)?.Dispose();
        this.page = [];
    }
}
