using Microsoft.Extensions.Logging;
using global::Orleans.Runtime;
using global::Orleans.Serialization;
using global::Orleans.Streams;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// The write half of the provider: turns an Orleans batch into one <see cref="StreamEventDocument"/> row,
/// sequenced under the queue's counter row.
/// </summary>
public sealed class DocumentDbQueueAdapter : IQueueAdapter
{
    readonly IDocumentStore store;
    readonly DocumentDbStreamOptions options;
    readonly IStreamQueueMapper queueMapper;
    readonly Serializer<DocumentDbBatchContainer> serializer;
    readonly ILoggerFactory loggerFactory;
    readonly ILogger logger;
    readonly string serviceId;
    readonly TimeProvider clock;
    readonly StreamChangeNudge? nudge;

    internal DocumentDbQueueAdapter(
        string name,
        string serviceId,
        IDocumentStore store,
        DocumentDbStreamOptions options,
        IStreamQueueMapper queueMapper,
        Serializer<DocumentDbBatchContainer> serializer,
        StreamChangeNudge? nudge,
        ILoggerFactory loggerFactory,
        TimeProvider clock)
    {
        this.Name = name;
        this.serviceId = serviceId;
        this.store = store;
        this.options = options;
        this.queueMapper = queueMapper;
        this.serializer = serializer;
        this.loggerFactory = loggerFactory;
        this.logger = loggerFactory.CreateLogger<DocumentDbQueueAdapter>();
        this.clock = clock;
        this.nudge = nudge;
    }

    public string Name { get; }

    /// <summary>
    /// True: a subscriber can resume from a <c>StreamSequenceToken</c> older than the in-memory cache,
    /// because <see cref="DocumentDbQueueCache"/> replays it out of the events table. The window is bounded
    /// by <c>DocumentDbStreamOptions.Retention</c> — beyond that the events are gone and Orleans' ordinary
    /// cache-miss is what the caller gets.
    /// </summary>
    public bool IsRewindable => true;

    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    internal string CounterId(QueueId queueId) => $"{this.serviceId}|{this.Name}|{queueId}";

    /// <summary>Checkpoint key. Same shape as the counter key but a different document type, so the two
    /// rows never collide despite sharing a table.</summary>
    internal string CheckpointId(QueueId queueId) => $"{this.serviceId}|{this.Name}|{queueId}";

    public async Task QueueMessageBatchAsync<T>(
        StreamId streamId,
        IEnumerable<T> events,
        StreamSequenceToken token,
        Dictionary<string, object> requestContext)
    {
        ArgumentNullException.ThrowIfNull(events);

        // A caller-supplied token would be a request to write at a specific position, which this provider
        // cannot honour — the queue's counter owns positions. Refusing is better than accepting and ignoring.
        if (token != null)
            throw new ArgumentException("DocumentDb streams assign sequence tokens at enqueue; a caller-supplied token is not supported.", nameof(token));

        var queueId = this.queueMapper.GetQueueForStream(streamId);
        var container = new DocumentDbBatchContainer
        {
            StreamId = streamId,
            Events = events.Cast<object>().ToList(),
            RequestContext = requestContext
        };

        await this.Enqueue(queueId, streamId, container).ConfigureAwait(false);

        if (this.logger.IsEnabled(LogLevel.Trace))
            this.logger.LogTrace("Enqueued batch on {QueueId} at sequence {Sequence} ({Count} events)", queueId, container.SequenceNumber, container.Events.Count);
    }

    /// <summary>
    /// One enqueue attempt, retried on a serialization failure.
    /// </summary>
    /// <remarks>
    /// The retry is not optional on CockroachDB. Its transactions are <c>SERIALIZABLE</c> by default and the
    /// counter row is, by design, the single hottest row on the queue — exactly the shape that produces
    /// retryable <c>40001</c> aborts under contention, which CockroachDB expects the application to retry.
    /// The other engines take the row lock and simply wait, so they never reach this path; leaving it out
    /// would mean streams "work in testing, drop writes under load" on one supported backend only.
    /// </remarks>
    async Task Enqueue(QueueId queueId, StreamId streamId, DocumentDbBatchContainer container)
    {
        const int maxAttempts = 5;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await this.EnqueueOnce(queueId, streamId, container).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts && IsRetryable(ex))
            {
                // Exponential-ish backoff with no jitter source available here; the lock queue does most of
                // the spacing for us and the contention window is sub-millisecond.
                this.logger.LogDebug(ex, "Retryable conflict enqueueing on {QueueId} (attempt {Attempt}/{Max})", queueId, attempt, maxAttempts);
                await Task.Delay(TimeSpan.FromMilliseconds(10 * attempt)).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Recognises a serialization/deadlock abort without taking a dependency on any provider's ADO.NET
    /// exception type — the Orleans package must not reference Npgsql, MySqlConnector, Oracle and the rest
    /// just to read an error code.
    /// </summary>
    static bool IsRetryable(Exception ex)
    {
        for (var e = ex; e != null; e = e.InnerException)
        {
            // ConcurrencyException reaches here when a competing writer bumped the counter's version, which
            // is the same "try again" answer by a different route.
            if (e is ConcurrencyException)
                return true;

            var sqlState = e.Data["SqlState"] as string;
            // 40001 serialization_failure, 40P01 deadlock_detected (PostgreSQL/CockroachDB SQLSTATE).
            if (sqlState is "40001" or "40P01")
                return true;

            var text = e.Message;
            if (text.Contains("40001", StringComparison.Ordinal)
                || text.Contains("restart transaction", StringComparison.OrdinalIgnoreCase)
                || text.Contains("deadlock", StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    async Task EnqueueOnce(QueueId queueId, StreamId streamId, DocumentDbBatchContainer container)
    {
        await using var session = this.store.OpenSession();
        await using var tx = await session.BeginTransaction().ConfigureAwait(false);

        // The lock is the whole design. Holding it across the insert means the next enqueuer cannot take a
        // sequence number until this transaction commits, so sequence order and commit order agree and the
        // receiver's `Seq > cursor` read can never step over a row that has not appeared yet.
        var counterId = this.CounterId(queueId);
        var counter = await session.Get<StreamSequenceDocument>(counterId, LockMode.Update).ConfigureAwait(false)
            ?? throw new InvalidOperationException(
                $"Sequence counter '{counterId}' is missing. It is created when the stream provider starts; " +
                "a missing counter means the provider was not initialized against this store.");

        container.SequenceNumber = counter.Next;
        counter.Next++;

        session.Update(counter);
        session.Add(new StreamEventDocument
        {
            Id = Guid.CreateVersion7().ToString(),
            QueueId = queueId.ToString(),
            Seq = container.SequenceNumber,
            StreamNamespace = streamId.GetNamespace(),
            StreamId = streamId.ToString(),
            Payload = Convert.ToBase64String(this.serializer.SerializeToArray(container)),
            EnqueuedAt = this.clock.GetUtcNow()
        });

        await session.SaveChanges().ConfigureAwait(false);
        await tx.Commit().ConfigureAwait(false);
    }

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        => new DocumentDbQueueReceiver(
            queueId,
            this.CheckpointId(queueId),
            this.store,
            this.options,
            this.serializer,
            this.nudge,
            this.loggerFactory.CreateLogger<DocumentDbQueueReceiver>(),
            this.clock);
}
