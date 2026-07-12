using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Diagnostics;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Instrumentation is embedded and always-on: a plain store emits OTel spans + metrics with no opt-in, zero-cost
// when unobserved. Because the ActivitySource/Meter are process-wide, each test registers a keyed store with a
// unique name and filters the collector by that db.namespace, isolating it from the rest of the parallel suite.
public class InstrumentationTests
{
    static (ServiceProvider Sp, IDocumentStore Store, string Name) KeyedStore()
    {
        var name = $"s{Guid.NewGuid():N}";
        var services = new ServiceCollection();
        services.AddDocumentStore(name, o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        var sp = services.BuildServiceProvider();
        return (sp, sp.GetRequiredKeyedService<IDocumentStore>(name), name);
    }

    [Fact]
    public async Task RecordsMetricAndSpanPerOperation()
    {
        var (sp, store, name) = KeyedStore();
        using (sp)
        using (var telemetry = new TelemetryCollector(name))
        {
            await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
            var fetched = await store.Get<VersionedUser>("u1");
            await store.Remove<VersionedUser>("u1");

            Assert.NotNull(fetched);

            var durations = telemetry.Measurements.Where(m => m.Instrument == "db.client.operation.duration").ToList();
            var ops = durations.Select(m => m.Tag("db.operation.name")).ToList();
            Assert.Contains("insert", ops);
            Assert.Contains("get", ops);
            Assert.Contains("remove", ops);
            Assert.All(durations, m => Assert.Equal("sqlite", m.Tag("db.system.name")));
            Assert.All(durations, m => Assert.Equal("success", m.Tag("outcome")));
            Assert.All(durations, m => Assert.Equal(nameof(VersionedUser), m.Tag("db.collection.name")));

            var spanNames = telemetry.Activities.Select(a => a.OperationName).ToList();
            Assert.Contains("sqlite.insert", spanNames);
            Assert.Contains("sqlite.get", spanNames);
            Assert.Contains("sqlite.remove", spanNames);
            Assert.All(telemetry.Activities, a => Assert.Equal(ActivityStatusCode.Unset, a.Status));
        }
    }

    [Fact]
    public async Task RecordsErrorOutcomeAndSpanStatus_OnFailure()
    {
        var (sp, store, name) = KeyedStore();
        using (sp)
        using (var telemetry = new TelemetryCollector(name))
        {
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
    }

    [Fact]
    public async Task FluentQueryTerminal_IsInstrumented()
    {
        var (sp, store, name) = KeyedStore();
        using (sp)
        using (var telemetry = new TelemetryCollector(name))
        {
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
    }

    // The re-entrancy guard: a user UnitOfWork emits ONE "transaction" span; the inner writes run span-free
    // (the decorator used to record them as child spans — that is the accepted trade-off of embedding).
    [Fact]
    public async Task UnitOfWork_EmitsSingleTransactionSpan_InnerOpsSpanFree()
    {
        var (sp, store, name) = KeyedStore();
        using (sp)
        using (var telemetry = new TelemetryCollector(name))
        {
            await store.CreateUnitOfWork()
                .Add(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 })
                .Add(new VersionedUser { Id = "u2", Name = "Bob", Age = 40 })
                .SaveChanges();

            Assert.Equal(1, telemetry.Activities.Count(a => a.OperationName == "sqlite.transaction"));
            Assert.Equal(0, telemetry.Activities.Count(a => a.OperationName == "sqlite.batch_insert"));
            Assert.Equal(1, telemetry.Measurements.Count(m =>
                m.Instrument == "db.client.operation.duration" && m.Tag("db.operation.name") == "transaction"));
            Assert.Equal(0, telemetry.Measurements.Count(m => m.Tag("db.operation.name") == "batch_insert"));
        }
    }

    [Fact]
    public async Task KeyedStore_TagsMeasurementsWithStoreName()
    {
        var (sp, store, name) = KeyedStore();
        using (sp)
        using (var telemetry = new TelemetryCollector(name))
        {
            await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });

            var insert = telemetry.Measurements.Single(m =>
                m.Instrument == "db.client.operation.duration" && m.Tag("db.operation.name") == "insert");
            Assert.Equal(name, insert.Tag("db.namespace"));
        }
    }

    [Fact]
    public async Task NonKeyedStore_OmitsStoreNameTag()
    {
        // A non-keyed store leaves StoreName null → no db.namespace. Filter by the unique table/collection to
        // isolate this store's own signals from the rest of the parallel suite.
        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        });
        using var telemetry = new TelemetryCollector(collection: nameof(NamespaceProbe));

        await store.Insert(new NamespaceProbe { Id = "u1" });

        var insert = telemetry.Measurements.Single(m =>
            m.Instrument == "db.client.operation.duration" && m.Tag("db.operation.name") == "insert");
        Assert.Null(insert.Tag("db.namespace"));
    }

    public class NamespaceProbe { public string Id { get; set; } = ""; }

    // ── built-in MeterListener / ActivityListener collector, filtered to one store ──

    sealed class TelemetryCollector : IDisposable
    {
        readonly MeterListener meterListener;
        readonly ActivityListener activityListener;
        readonly string? storeName;
        readonly string? collection;
        readonly List<Measurement> measurements = new();
        readonly List<Activity> activities = new();

        public IReadOnlyList<Measurement> Measurements { get { lock (this.measurements) return this.measurements.ToList(); } }
        public IReadOnlyList<Activity> Activities { get { lock (this.activities) return this.activities.ToList(); } }

        public TelemetryCollector(string? storeName = null, string? collection = null)
        {
            this.storeName = storeName;
            this.collection = collection;

            this.meterListener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (instrument.Meter.Name == DocumentStoreMetrics.Name)
                        listener.EnableMeasurementEvents(instrument);
                }
            };
            this.meterListener.SetMeasurementEventCallback<double>(this.OnMeasurement);
            this.meterListener.SetMeasurementEventCallback<long>(this.OnMeasurement);
            this.meterListener.Start();

            this.activityListener = new ActivityListener
            {
                ShouldListenTo = src => src.Name == DocumentStoreMetrics.Name,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = a =>
                {
                    if (this.Matches(a.GetTagItem("db.namespace") as string, a.GetTagItem("db.collection.name") as string))
                        lock (this.activities) this.activities.Add(a);
                }
            };
            ActivitySource.AddActivityListener(this.activityListener);
        }

        bool Matches(string? ns, string? coll)
            => (this.storeName == null || ns == this.storeName)
            && (this.collection == null || coll == this.collection);

        void OnMeasurement<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
            where T : struct
        {
            var dict = new Dictionary<string, object?>();
            foreach (var tag in tags)
                dict[tag.Key] = tag.Value;
            if (this.Matches(dict.GetValueOrDefault("db.namespace") as string, dict.GetValueOrDefault("db.collection.name") as string))
                lock (this.measurements) this.measurements.Add(new Measurement(instrument.Name, Convert.ToDouble(measurement), dict));
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
