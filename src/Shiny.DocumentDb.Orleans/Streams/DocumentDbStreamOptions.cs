namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// Configuration for the DocumentDb persistent stream provider.
/// </summary>
public class DocumentDbStreamOptions : OrleansStoreOptions
{
    /// <summary>
    /// Number of queues streams hash across (Orleans' own default is 8). This is also the number of sequence
    /// counter rows, and therefore the enqueue-concurrency dial: each queue's enqueues serialize on its own
    /// counter row, so more queues means more concurrent producers, at the cost of more pulling agents polling.
    /// </summary>
    /// <remarks>
    /// Changing this on a cluster with a non-empty backlog re-hashes streams across queues, so events already
    /// sitting on a queue can end up on one nobody reads. Drain before changing it.
    /// </remarks>
    public int TotalQueueCount { get; set; } = 8;

    /// <summary>
    /// Floor of the adaptive poll backoff — the fastest this provider will query the database for one queue.
    /// The pulling agent's own timer still fires on Orleans' schedule; a poll that arrives before this has
    /// elapsed returns empty without touching the database.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Ceiling of the adaptive poll backoff on an idle queue. On a backend with no native change feed this is
    /// the worst-case delivery latency for the first event after an idle period, so it is a real
    /// latency-versus-idle-cost trade rather than a tuning detail.
    /// </summary>
    public TimeSpan MaxPollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Maximum event rows read per poll.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// How long delivered events are kept before the purge sweep removes them. <c>null</c> keeps every event
    /// forever — an audit trail, and a table that grows without bound.
    /// </summary>
    public TimeSpan? Retention { get; set; } = TimeSpan.FromHours(1);

    /// <summary>How often each receiver sweeps its own queue for expired delivered events.</summary>
    public TimeSpan PurgeInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Use the store's native change feed (PostgreSQL <c>LISTEN</c>/<c>NOTIFY</c>, SQL Server query
    /// notifications) to cut the poll backoff short when an event is enqueued — including by another silo.
    /// Backends without one fall back to the poll interval and behave identically, just later.
    /// </summary>
    public bool UseChangeFeedNudge { get; set; } = true;

    internal void Validate()
    {
        if (this.TotalQueueCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(this.TotalQueueCount), "TotalQueueCount must be at least 1.");
        if (this.BatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(this.BatchSize), "BatchSize must be at least 1.");
        if (this.PollInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(this.PollInterval), "PollInterval must be greater than zero.");
        if (this.MaxPollInterval < this.PollInterval)
            throw new ArgumentOutOfRangeException(nameof(this.MaxPollInterval), "MaxPollInterval must be at least PollInterval.");
    }
}
