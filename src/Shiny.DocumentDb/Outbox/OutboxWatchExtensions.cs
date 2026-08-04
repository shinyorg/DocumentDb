using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// The observation half of the outbox — one <c>await foreach</c> for a dashboard, a health check, or a test.
/// </summary>
public static class OutboxWatchExtensions
{
    /// <summary>
    /// Streams outbox message revisions as they appear. <b>Read-only</b>: it never claims, acknowledges or
    /// mutates a message, so it cannot compete with the processor for work.
    /// <code>
    /// await foreach (var msg in store.WatchOutbox(o => o.States = OutboxStates.DeadLettered, ct))
    ///     logger.LogError("Outbox dead letter {Id}: {Error}", msg.Id, msg.Error);
    /// </code>
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is a <b>poll</b>, nudged early by change notification where the store supports it, and that order
    /// matters. A failed message becomes pending again purely because its <c>AvailableAt</c> elapsed — a clock
    /// event with no write behind it — so a notification-driven stream would never yield it. In-process
    /// notifications are also blind to other writers, and an outbox usually has several. Polling is the
    /// correctness floor; the nudge only shortens the latency, and a store with no notification capability
    /// behaves identically, just later.
    /// </para>
    /// <para>
    /// It deliberately does <b>not</b> promise every revision: a message enqueued, dispatched and acknowledged
    /// between two polls is never observed, which is correct for a monitor. If you need to see every message,
    /// you are the dispatcher — register one with <c>AddDocumentOutbox</c> rather than watching, or you will
    /// have built a second, competing consumer and duplicated a side effect in production.
    /// </para>
    /// </remarks>
    public static IAsyncEnumerable<OutboxMessage> WatchOutbox(
        this IDocumentStore store,
        Action<OutboxWatchOptions>? configure = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        var options = new OutboxWatchOptions();
        configure?.Invoke(options);
        return Watch(store, options, cancellationToken);
    }

    /// <summary>
    /// As <see cref="WatchOutbox(IDocumentStore, Action{OutboxWatchOptions}, CancellationToken)"/>, narrowed to
    /// one event type and with the payload deserialized.
    /// </summary>
    [RequiresUnreferencedCode("Deserializes the payload with reflection. Use the JsonTypeInfo overload in a trimmed or AOT application.")]
    [RequiresDynamicCode("Deserializes the payload with reflection. Use the JsonTypeInfo overload in a trimmed or AOT application.")]
    public static IAsyncEnumerable<OutboxEnvelope<TEvent>> WatchOutbox<TEvent>(
        this IDocumentStore store,
        Action<OutboxWatchOptions>? configure = null,
        CancellationToken cancellationToken = default) where TEvent : class
        => store.WatchOutbox<TEvent>(null, configure, cancellationToken);

    /// <summary>AOT-safe overload — deserializes the payload through supplied metadata rather than reflection.</summary>
    public static async IAsyncEnumerable<OutboxEnvelope<TEvent>> WatchOutbox<TEvent>(
        this IDocumentStore store,
        JsonTypeInfo<TEvent>? jsonTypeInfo,
        Action<OutboxWatchOptions>? configure = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) where TEvent : class
    {
        ArgumentNullException.ThrowIfNull(store);

        var options = new OutboxWatchOptions();
        configure?.Invoke(options);
        options.MessageType ??= OutboxSessionExtensions.MessageTypeOf(typeof(TEvent));

        await foreach (var message in Watch(store, options, cancellationToken).ConfigureAwait(false))
        {
            var evt = jsonTypeInfo != null
                ? JsonSerializer.Deserialize(message.Payload, jsonTypeInfo)
                : Deserialize<TEvent>(message.Payload, options);

            if (evt != null)
                yield return new OutboxEnvelope<TEvent>(message, evt);
        }
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "The reflection-based path is reached only from the overload already annotated RequiresUnreferencedCode.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "The reflection-based path is reached only from the overload already annotated RequiresDynamicCode.")]
    static TEvent? Deserialize<TEvent>(string payload, OutboxWatchOptions options) where TEvent : class
        => JsonSerializer.Deserialize<TEvent>(payload, options.PayloadSerializerOptions);

    static async IAsyncEnumerable<OutboxMessage> Watch(
        IDocumentStore store,
        OutboxWatchOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (options.States == 0)
            throw new ArgumentException("OutboxWatchOptions.States must name at least one state to watch.", nameof(options));

        // Dedupe on (Id, Version), never on Id alone: a message legitimately reappears — a retry bumps
        // Attempts, a requeue clears the dead-letter state — and each of those is a revision worth yielding.
        // Version is the concurrency counter already in the contract, and it moves on every one of them. An
        // id watermark would be wrong twice over: requeue walks a message backwards through the states, and
        // v7 ordering says nothing about revisions.
        var seen = new Dictionary<string, int>(StringComparer.Ordinal);
        var previousIds = new HashSet<string>(StringComparer.Ordinal);
        var yieldBatch = options.IncludeExisting;

        using var nudge = OutboxNudge.Attach(store, cancellationToken);

        var running = true;
        while (running && !cancellationToken.IsCancellationRequested)
        {
            IReadOnlyList<OutboxMessage> batch;
            try
            {
                batch = await Read(store, options, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Cancelling mid-poll ends the stream rather than throwing out of the caller's await foreach:
                // the token is how you stop a monitor, not an error condition.
                break;
            }

            var currentIds = new HashSet<string>(StringComparer.Ordinal);

            foreach (var message in batch)
            {
                currentIds.Add(message.Id);
                var isNewRevision = !seen.TryGetValue(message.Id, out var version) || version != message.Version;
                seen[message.Id] = message.Version;

                if (isNewRevision && yieldBatch)
                    yield return message;
            }

            // Two generations of ids are retained, so the dedupe set stays bounded on a long-lived watch
            // without a message that merely paged out of one poll re-yielding on the next.
            foreach (var stale in seen.Keys.Where(k => !currentIds.Contains(k) && !previousIds.Contains(k)).ToList())
                seen.Remove(stale);

            previousIds = currentIds;
            // IncludeExisting is nothing more than whether the first poll yields its results or only records
            // them as already-seen.
            yieldBatch = true;

            running = await nudge.Wait(options.PollInterval, cancellationToken).ConfigureAwait(false);
        }
    }

    // States are OR-ed by running one query per requested state and merging: at most four cheap, index-friendly
    // predicates, versus one OR-of-null-checks that every provider would translate differently.
    static async Task<IReadOnlyList<OutboxMessage>> Read(IDocumentStore store, OutboxWatchOptions options, CancellationToken cancellationToken)
    {
        var now = options.TimeProvider.GetUtcNow();
        var merged = new Dictionary<string, OutboxMessage>(StringComparer.Ordinal);

        foreach (var state in Enum.GetValues<OutboxStates>().Where(s => s != OutboxStates.All && options.States.HasFlag(s)))
        {
            var query = store.Query(options.MessageTypeInfo);
            query = state switch
            {
                OutboxStates.Pending => query.Where(m => m.ProcessedAt == null && m.DeadLetteredAt == null && m.AvailableAt <= now),
                OutboxStates.Scheduled => query.Where(m => m.ProcessedAt == null && m.DeadLetteredAt == null && m.AvailableAt > now),
                OutboxStates.DeadLettered => query.Where(m => m.DeadLetteredAt != null),
                _ => query.Where(m => m.ProcessedAt != null)
            };

            if (options.PartitionKey != null)
            {
                var partitionKey = options.PartitionKey;
                query = query.Where(m => m.PartitionKey == partitionKey);
            }
            if (options.MessageType != null)
            {
                var messageType = options.MessageType;
                query = query.Where(m => m.MessageType == messageType);
            }

            var rows = await query
                .OrderBy(m => m.Id)
                .Paginate(0, options.BatchSize)
                .ToList(cancellationToken)
                .ConfigureAwait(false);

            foreach (var row in rows)
                merged[row.Id] = row;
        }

        return [.. merged.Values.OrderBy(m => m.Id, StringComparer.Ordinal)];
    }
}

/// <summary>
/// Turns in-process change notification into an early wake-up for the watch loop. Best-effort by design: the
/// poll interval is the correctness floor, so a store with no <see cref="IObservableDocumentStore"/> simply
/// waits out the interval and behaves the same, just later.
/// </summary>
sealed class OutboxNudge : IDisposable
{
    readonly SemaphoreSlim signal = new(0, 1);
    readonly CancellationTokenSource? cts;

    OutboxNudge(IObservableDocumentStore? observable, CancellationToken cancellationToken)
    {
        if (observable == null)
            return;

        this.cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _ = this.Pump(observable, this.cts.Token);
    }

    public static OutboxNudge Attach(IDocumentStore store, CancellationToken cancellationToken)
        => new(store as IObservableDocumentStore, cancellationToken);

    async Task Pump(IObservableDocumentStore observable, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var _ in observable.NotifyOnChange<OutboxMessage>(cancellationToken).ConfigureAwait(false))
                this.Signal();
        }
        catch
        {
            // The nudge is an optimization; losing it costs latency, never correctness.
        }
    }

    void Signal()
    {
        try
        {
            if (this.signal.CurrentCount == 0)
                this.signal.Release();
        }
        catch (SemaphoreFullException)
        {
            // Already signalled by a concurrent notification — one wake-up is all the loop needs.
        }
    }

    /// <summary>Waits for a change notification or the poll interval, whichever lands first. Returns false when
    /// the caller cancelled, which ends the stream.</summary>
    public async Task<bool> Wait(TimeSpan interval, CancellationToken cancellationToken)
    {
        try
        {
            await this.signal.WaitAsync(interval, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    public void Dispose()
    {
        this.cts?.Cancel();
        this.cts?.Dispose();
        this.signal.Dispose();
    }
}
