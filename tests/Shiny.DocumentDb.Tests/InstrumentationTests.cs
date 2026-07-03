using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Diagnostics;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class InstrumentationTests
{
    static InstrumentedDocumentStore CreateStore()
    {
        var metrics = new DocumentStoreMetrics(new TestMeterFactory());
        var inner = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        });
        return new InstrumentedDocumentStore(inner, metrics);
    }

    sealed class TestMeterFactory : IMeterFactory
    {
        readonly List<Meter> meters = new();
        public Meter Create(MeterOptions options)
        {
            var meter = new Meter(options);
            this.meters.Add(meter);
            return meter;
        }
        public void Dispose()
        {
            foreach (var meter in this.meters)
                meter.Dispose();
        }
    }

    [Fact]
    public async Task RecordsMetricAndSpanPerOperation()
    {
        using var telemetry = new TelemetryCollector();
        using var store = CreateStore();

        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        var fetched = await store.Get<VersionedUser>("u1");
        await store.Remove<VersionedUser>("u1");

        Assert.NotNull(fetched);

        // Durations recorded for each operation, tagged per OTel db client conventions.
        var durations = telemetry.Measurements.Where(m => m.Instrument == "db.client.operation.duration").ToList();
        var ops = durations.Select(m => m.Tag("db.operation.name")).ToList();
        Assert.Contains("insert", ops);
        Assert.Contains("get", ops);
        Assert.Contains("remove", ops);
        Assert.All(durations, m => Assert.Equal("sqlite", m.Tag("db.system.name")));
        Assert.All(durations, m => Assert.Equal("success", m.Tag("outcome")));
        Assert.All(durations, m => Assert.Equal(nameof(VersionedUser), m.Tag("db.collection.name")));

        // Spans emitted for the same operations.
        var spanNames = telemetry.Activities.Select(a => a.OperationName).ToList();
        Assert.Contains("sqlite.insert", spanNames);
        Assert.Contains("sqlite.get", spanNames);
        Assert.Contains("sqlite.remove", spanNames);
        Assert.All(telemetry.Activities, a => Assert.Equal(ActivityStatusCode.Unset, a.Status));
    }

    [Fact]
    public async Task RecordsErrorOutcomeAndSpanStatus_OnFailure()
    {
        using var telemetry = new TelemetryCollector();
        using var store = CreateStore();

        await store.Insert(new VersionedUser { Id = "dup", Name = "Alice", Age = 30 });
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.Insert(new VersionedUser { Id = "dup", Name = "Bob", Age = 40 }));

        var failed = telemetry.Measurements.Single(m =>
            m.Instrument == "db.client.operation.duration" &&
            m.Tag("db.operation.name") == "insert" &&
            m.Tag("outcome") == "error");
        Assert.Equal(typeof(InvalidOperationException).FullName, failed.Tag("error.type"));

        var span = telemetry.Activities.Last(a => a.OperationName == "sqlite.insert");
        Assert.Equal(ActivityStatusCode.Error, span.Status);
    }

    [Fact]
    public async Task FluentQueryTerminal_IsInstrumented()
    {
        using var telemetry = new TelemetryCollector();
        using var store = CreateStore();

        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        await store.Insert(new VersionedUser { Id = "u2", Name = "Bob", Age = 40 });

        var results = await store.Query<VersionedUser>().Where(u => u.Age >= 35).ToList();
        Assert.Single(results);

        var toList = telemetry.Measurements.Single(m =>
            m.Instrument == "db.client.operation.duration" && m.Tag("db.operation.name") == "query.to_list");
        Assert.Equal("sqlite", toList.Tag("db.system.name"));
        Assert.Equal("success", toList.Tag("outcome"));
        Assert.Contains(telemetry.Activities, a => a.OperationName == "sqlite.query.to_list");
    }

    [Fact]
    public async Task UnitOfWork_InnerOperations_AreChildSpans()
    {
        using var telemetry = new TelemetryCollector();
        using var store = CreateStore();

        // Two contiguous same-type adds coalesce into a single batch insert inside the unit.
        await store.CreateUnitOfWork()
            .Add(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 })
            .Add(new VersionedUser { Id = "u2", Name = "Bob", Age = 40 })
            .SaveChanges();

        var txSpan = telemetry.Activities.Single(a => a.OperationName == "sqlite.transaction");
        var batch = telemetry.Activities.Single(a => a.OperationName == "sqlite.batch_insert");
        Assert.Equal(txSpan.SpanId, batch.ParentSpanId);

        // Inner operations are also metered.
        var batchMetrics = telemetry.Measurements.Count(m =>
            m.Instrument == "db.client.operation.duration" && m.Tag("db.operation.name") == "batch_insert");
        Assert.Equal(1, batchMetrics);
    }

    [Fact]
    public void DiFlag_Instrumentation_DecoratesRegisteredStore()
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.Instrumentation = true;
        });

        using var sp = services.BuildServiceProvider();
        Assert.IsType<InstrumentedDocumentStore>(sp.GetRequiredService<IDocumentStore>());
    }

    [Fact]
    public void DiFlag_Default_LeavesStoreUndecorated()
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(o => o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"));

        using var sp = services.BuildServiceProvider();
        Assert.IsNotType<InstrumentedDocumentStore>(sp.GetRequiredService<IDocumentStore>());
    }

    [Fact]
    public void DiFlag_OnKeyedStore_Throws()
        => Assert.Throws<NotSupportedException>(() =>
            new ServiceCollection().AddDocumentStore("named", o =>
            {
                o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
                o.Instrumentation = true;
            }));

    // ── built-in MeterListener / ActivityListener collector (no extra test deps) ──

    sealed class TelemetryCollector : IDisposable
    {
        readonly MeterListener meterListener;
        readonly ActivityListener activityListener;
        public List<Measurement> Measurements { get; } = new();
        public List<Activity> Activities { get; } = new();

        public TelemetryCollector()
        {
            this.meterListener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (instrument.Meter.Name == DocumentStoreMetrics.MeterName)
                        listener.EnableMeasurementEvents(instrument);
                }
            };
            this.meterListener.SetMeasurementEventCallback<double>(this.OnMeasurement);
            this.meterListener.SetMeasurementEventCallback<long>(this.OnMeasurement);
            this.meterListener.Start();

            this.activityListener = new ActivityListener
            {
                ShouldListenTo = src => src.Name == DocumentStoreMetrics.MeterName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => this.Activities.Add(activity)
            };
            ActivitySource.AddActivityListener(this.activityListener);
        }

        void OnMeasurement<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
            where T : struct
        {
            var dict = new Dictionary<string, object?>();
            foreach (var tag in tags)
                dict[tag.Key] = tag.Value;
            this.Measurements.Add(new Measurement(instrument.Name, Convert.ToDouble(measurement), dict));
        }

        public void Dispose()
        {
            this.meterListener.Dispose();
            this.activityListener.Dispose();
        }
    }

    sealed record Measurement(string Instrument, double Value, Dictionary<string, object?> Tags)
    {
        public string? Tag(string key) => this.Tags.TryGetValue(key, out var v) ? v as string : null;
    }
}
