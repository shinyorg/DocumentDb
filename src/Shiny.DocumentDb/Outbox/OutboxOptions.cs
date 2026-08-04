using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>How the <c>OutboxProcessor</c> drains the queue. Configured on <c>services.AddDocumentOutbox(…)</c>.</summary>
/// <remarks>
/// The table the messages live in is <b>not</b> here — that is a store mapping, chosen by
/// <c>options.AddOutbox(tableName)</c> when the store is configured, so there is exactly one place it can be
/// set and no way for the two halves to disagree.
/// </remarks>
public sealed class OutboxOptions
{
    /// <summary>How many messages one poll claims. Defaults to 50.</summary>
    public int BatchSize { get; set; } = 50;

    /// <summary>How many messages are dispatched concurrently. Defaults to 4.</summary>
    public int MaxParallelism { get; set; } = 4;

    /// <summary>Attempts before a message is dead-lettered. Defaults to 8.</summary>
    public int MaxAttempts { get; set; } = 8;

    /// <summary>How often the queue is polled when it is empty. Defaults to 5 seconds.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The delay before attempt <c>n</c> may be retried. Doubles as the claim's visibility timeout: claiming a
    /// message pushes <see cref="OutboxMessage.AvailableAt"/> out by this much, so a processor that dies
    /// mid-dispatch releases the message rather than stranding it.
    /// </summary>
    public Func<int, TimeSpan> Backoff { get; set; } = attempt => TimeSpan.FromSeconds(Math.Pow(2, Math.Min(attempt, 16)));

    /// <summary>Deletes acknowledged messages older than this. Null keeps them forever, as an audit trail.</summary>
    public TimeSpan? Retention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>How often the retention sweep runs. Defaults to 5 minutes; ignored when <see cref="Retention"/> is null.</summary>
    public TimeSpan RetentionInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Dispatches messages sharing a <see cref="OutboxMessage.PartitionKey"/> strictly in order, one at a time.
    /// Different partitions still run in parallel, and a message with no partition key is unordered.
    /// </summary>
    public bool OrderedPartitions { get; set; }

    /// <summary>
    /// AOT-safe metadata for <see cref="OutboxMessage"/>. Required only when the store is configured without
    /// reflection fallback — pass <c>OutboxJsonContext.Default.OutboxMessage</c>, or your own context's.
    /// </summary>
    public JsonTypeInfo<OutboxMessage>? MessageTypeInfo { get; set; }

    /// <summary>
    /// Serializer options for the <b>event payload</b> (not the envelope). Defaults to camelCase.
    /// </summary>
    public JsonSerializerOptions PayloadSerializerOptions { get; set; } = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    internal void Validate()
    {
        if (this.BatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(this.BatchSize), this.BatchSize, "OutboxOptions.BatchSize must be greater than zero.");
        if (this.MaxParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(this.MaxParallelism), this.MaxParallelism, "OutboxOptions.MaxParallelism must be greater than zero.");
        if (this.MaxAttempts <= 0)
            throw new ArgumentOutOfRangeException(nameof(this.MaxAttempts), this.MaxAttempts, "OutboxOptions.MaxAttempts must be greater than zero.");
        if (this.PollInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(this.PollInterval), this.PollInterval, "OutboxOptions.PollInterval must be greater than zero.");
    }
}

/// <summary>
/// Which write operations on a mapped type publish an event, for
/// <c>cfg.PublishToOutbox(…)</c>.
/// </summary>
/// <remarks>
/// A separate enum rather than <c>[Flags]</c> on <see cref="DocumentOperation"/>: that one is switched on
/// across the whole library and reshaping it to carry bit values would be a breaking change for a filter only
/// the outbox needs.
/// </remarks>
[Flags]
public enum OutboxOperations
{
    None = 0,
    Insert = 1,
    Update = 2,
    Upsert = 4,
    Delete = 8,
    All = Insert | Update | Upsert | Delete
}
