using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Shiny.DocumentDb.Diagnostics;

/// <summary>
/// Owns the <see cref="Meter"/> and <see cref="ActivitySource"/> that <see cref="InstrumentedDocumentStore"/>
/// emits through. Registered as a singleton; the <see cref="Meter"/> is created via the DI
/// <see cref="IMeterFactory"/> so it is container-scoped and testable. Tag names follow the
/// OpenTelemetry database client semantic conventions, so any OTel pipeline understands them without
/// custom mapping.
/// <para>
/// Both signals are <b>zero-cost when nobody is listening</b>: <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>
/// returns <c>null</c> with no registered listener, and the instruments no-op with no subscribed meter.
/// </para>
/// </summary>
public sealed class DocumentStoreMetrics : IDisposable
{
    /// <summary>The <see cref="Meter"/> / <see cref="ActivitySource"/> name. Subscribe with OTel via <c>.AddMeter(MeterName)</c> / <c>.AddSource(MeterName)</c>.</summary>
    public const string MeterName = "Shiny.DocumentDb";

    static readonly ActivitySource ActivitySource = new(MeterName);

    readonly Meter meter;
    readonly Histogram<double> duration;
    readonly Counter<long> operations;
    readonly Histogram<long> rows;

    public DocumentStoreMetrics(IMeterFactory meterFactory)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);
        this.meter = meterFactory.Create(MeterName);

        this.duration = this.meter.CreateHistogram<double>(
            "db.client.operation.duration", "s", "Duration of document store operations.");
        this.operations = this.meter.CreateCounter<long>(
            "db.client.operations", "{operation}", "Number of document store operations executed.");
        this.rows = this.meter.CreateHistogram<long>(
            "db.client.response.returned_rows", "{row}", "Documents returned or affected by an operation.");
    }

    /// <summary>Starts a client span for an operation, or returns null when no listener is attached.</summary>
    internal static Activity? StartActivity(string system, string operation, string collection, string? storeName = null)
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
    internal void Record(string system, string operation, string collection, TimeSpan elapsed, string outcome, string? errorType, long? rowCount, string? storeName = null)
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

        this.duration.Record(elapsed.TotalSeconds, tags);
        this.operations.Add(1, tags);
        if (rowCount is { } n)
            this.rows.Record(n, tags);
    }

    public void Dispose() => this.meter.Dispose();
}
