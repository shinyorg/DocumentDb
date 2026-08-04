using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Shiny.DocumentDb.Diagnostics;

/// <summary>
/// The embedded telemetry engine: a process-wide <see cref="ActivitySource"/> + <see cref="Meter"/> (both named
/// <see cref="Name"/>) that every store emits through directly — no decorator, no opt-in. Tag names follow the
/// OpenTelemetry database client semantic conventions, so any OTel pipeline understands them without custom
/// mapping.
/// <para>
/// Both signals are <b>zero-cost when nobody is listening</b>: <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>
/// returns <c>null</c> with no registered <see cref="ActivityListener"/>, and the instruments no-op with no
/// subscribed meter — so instrumentation costs nothing until you opt in from your OTel pipeline with
/// <c>.AddMeter("Shiny.DocumentDb")</c> / <c>.AddSource("Shiny.DocumentDb")</c>.
/// </para>
/// </summary>
static class DocumentStoreMetrics
{
    /// <summary>The <see cref="Meter"/> / <see cref="ActivitySource"/> name. Subscribe with OTel via
    /// <c>.AddMeter(Name)</c> / <c>.AddSource(Name)</c>.</summary>
    public const string Name = "Shiny.DocumentDb";

    static readonly ActivitySource ActivitySource = new(Name);
    static readonly Meter Meter = new(Name);

    static readonly Histogram<double> Duration = Meter.CreateHistogram<double>(
        "db.client.operation.duration", "s", "Duration of document store operations.");
    static readonly Counter<long> Operations = Meter.CreateCounter<long>(
        "db.client.operations", "{operation}", "Number of document store operations executed.");
    static readonly Histogram<long> Rows = Meter.CreateHistogram<long>(
        "db.client.response.returned_rows", "{row}", "Documents returned or affected by an operation.");
    static readonly Histogram<long> UnitSize = Meter.CreateHistogram<long>(
        "db.client.unit_of_work.operations", "{operation}", "Buffered writes flushed per unit-of-work SaveChanges.");

    // ── Outbox ──────────────────────────────────────────────────────────
    // An outbox fails operationally rather than in the data, so it gets its own three signals: how many got
    // out, how many gave up, and — the one worth alerting on — how deep the queue is right now.
    static readonly Counter<long> OutboxDispatched = Meter.CreateCounter<long>(
        "db.client.outbox.dispatched", "{message}", "Outbox messages delivered successfully.");
    static readonly Counter<long> OutboxDeadLettered = Meter.CreateCounter<long>(
        "db.client.outbox.dead_lettered", "{message}", "Outbox messages that exhausted their delivery attempts.");
    static readonly Histogram<double> OutboxDispatchDuration = Meter.CreateHistogram<double>(
        "db.client.outbox.dispatch.duration", "s", "Duration of one outbox dispatch attempt.");

    // Read by an observable gauge, written by the processor after every poll. A gauge callback cannot await,
    // so the depth is sampled on the poll the processor is already making rather than by querying here.
    static long outboxPending = -1;
    static readonly ObservableGauge<long> OutboxPending = Meter.CreateObservableGauge(
        "db.client.outbox.pending", ObserveOutboxPending, "{message}", "Outbox messages awaiting delivery.");

    static IEnumerable<Measurement<long>> ObserveOutboxPending()
    {
        var depth = Interlocked.Read(ref outboxPending);
        // Negative means no processor has polled yet — report nothing rather than a fabricated zero.
        return depth < 0 ? [] : [new Measurement<long>(depth)];
    }

    /// <summary>Publishes the queue depth observed by the processor's latest poll.</summary>
    public static void RecordOutboxPending(long pending) => Interlocked.Exchange(ref outboxPending, pending);

    /// <summary>Starts the span covering one dispatch attempt, linked to the activity that enqueued the message.</summary>
    public static Activity? StartOutboxDispatch(string messageType, int attempt, string? partitionKey, string? traceParent)
    {
        var activity = ActivitySource.StartActivity("outbox.dispatch", ActivityKind.Producer, traceParent!);
        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag("messaging.operation.name", "publish");
            activity.SetTag("messaging.message.type", messageType);
            activity.SetTag("messaging.outbox.attempt", attempt);
            if (partitionKey != null)
                activity.SetTag("messaging.destination.partition.id", partitionKey);
        }
        return activity;
    }

    /// <summary>Records the outcome of one dispatch attempt.</summary>
    public static void RecordOutboxDispatch(string messageType, TimeSpan elapsed, string outcome, string? errorType)
    {
        var tags = new TagList
        {
            { "messaging.message.type", messageType },
            { "outcome", outcome }
        };
        if (errorType != null)
            tags.Add("error.type", errorType);

        OutboxDispatchDuration.Record(elapsed.TotalSeconds, tags);
        if (outcome == "success")
            OutboxDispatched.Add(1, tags);
    }

    /// <summary>Records a message that ran out of attempts.</summary>
    public static void RecordOutboxDeadLettered(string messageType)
        => OutboxDeadLettered.Add(1, new TagList { { "messaging.message.type", messageType } });

    /// <summary>Records the number of buffered write operations flushed by one <c>SaveChanges</c> (unit of work).</summary>
    public static void RecordUnitSize(string system, long operations, string? storeName = null)
    {
        var tags = new TagList { { "db.system.name", system } };
        if (storeName != null)
            tags.Add("db.namespace", storeName);
        UnitSize.Record(operations, tags);
    }

    /// <summary>Starts a client span for an operation, or returns null when no listener is attached.</summary>
    public static Activity? StartActivity(string system, string operation, string collection, string? storeName = null)
    {
        var activity = ActivitySource.StartActivity($"{system}.{operation}", ActivityKind.Client);
        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag("db.system.name", system);
            activity.SetTag("db.operation.name", operation);
            activity.SetTag("db.collection.name", collection);
            if (storeName != null)
                activity.SetTag("db.namespace", storeName);
        }
        return activity;
    }

    /// <summary>Records the duration, count, and (when known) row magnitude of a completed operation.</summary>
    public static void Record(string system, string operation, string collection, TimeSpan elapsed, string outcome, string? errorType, long? rowCount, string? storeName = null)
    {
        var tags = new TagList
        {
            { "db.system.name", system },
            { "db.operation.name", operation },
            { "db.collection.name", collection },
            { "outcome", outcome }
        };
        if (storeName != null)
            tags.Add("db.namespace", storeName);
        if (errorType != null)
            tags.Add("error.type", errorType);

        Duration.Record(elapsed.TotalSeconds, tags);
        Operations.Add(1, tags);
        if (rowCount is { } n)
            Rows.Record(n, tags);
    }
}
