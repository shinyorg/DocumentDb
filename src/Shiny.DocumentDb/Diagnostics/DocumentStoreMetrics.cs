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
