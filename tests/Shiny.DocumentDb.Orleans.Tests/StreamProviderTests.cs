using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Orleans.Tests;

/// <summary>A clock the test drives, so the receiver's adaptive backoff is deterministic rather than slept through.</summary>
sealed class TestClock : TimeProvider
{
    DateTimeOffset now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow()
    {
        lock (this) return this.now;
    }

    public void Advance(TimeSpan by)
    {
        lock (this) this.now += by;
    }
}

[Collection("Postgres")]
public class StreamProviderTests(PostgresFixture fixture)
{
    readonly TestClock clock = new();

    IServiceProvider BuildServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSerializer(b => b.AddAssembly(typeof(DocumentDbBatchContainer).Assembly));
        services.Configure<ClusterOptions>(o =>
        {
            o.ServiceId = "svc";
            o.ClusterId = "cluster";
        });
        services.AddSingleton<TimeProvider>(this.clock);
        return services.BuildServiceProvider();
    }

    DocumentDbStreamOptions Options(int queues = 4) => new()
    {
        DatabaseProvider = new PostgreSqlDatabaseProvider(fixture.Container.GetConnectionString()),
        TableName = "streams_" + Guid.NewGuid().ToString("N"),
        TotalQueueCount = queues,
        // The change feed needs server-side provisioning that these unit-level tests don't set up, and the
        // point here is the poll path anyway — the nudge only ever shortens latency.
        UseChangeFeedNudge = false
    };

    async Task<(DocumentDbQueueAdapterFactory Factory, IQueueAdapter Adapter)> Create(DocumentDbStreamOptions? options = null)
    {
        var factory = new DocumentDbQueueAdapterFactory("test", options ?? this.Options(), this.BuildServices());
        var adapter = await factory.CreateAdapter();
        return (factory, adapter);
    }

    /// <summary>Reads everything currently available, stepping the clock past the backoff between polls.</summary>
    async Task<List<IBatchContainer>> Drain(IQueueAdapterReceiver receiver, DocumentDbStreamOptions options, int maxPolls = 50)
    {
        var all = new List<IBatchContainer>();
        var empties = 0;
        for (var i = 0; i < maxPolls && empties < 2; i++)
        {
            var batch = await receiver.GetQueueMessagesAsync(100);
            if (batch.Count == 0)
            {
                empties++;
            }
            else
            {
                empties = 0;
                all.AddRange(batch);
                await receiver.MessagesDeliveredAsync(batch);
            }
            this.clock.Advance(options.MaxPollInterval + TimeSpan.FromSeconds(1));
        }
        return all;
    }

    [Fact]
    public void Sqlite_IsRefusedAtRegistration()
    {
        // SQLite passes SupportsTransactions but has no row-level locking, so the sequence counter would not
        // serialize. The gate must reject it while the silo is starting, not on the first lost event.
        var options = new DocumentDbStreamOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = "streams_gate"
        };

        var ex = Assert.Throws<NotSupportedException>(
            () => new DocumentDbQueueAdapterFactory("test", options, this.BuildServices()));
        Assert.Contains("pessimistic locking", ex.Message);
    }

    [Fact]
    public async Task PostgreSql_PassesTheGate()
    {
        var (_, adapter) = await this.Create();
        Assert.Equal("test", adapter.Name);
        Assert.Equal(StreamProviderDirection.ReadWrite, adapter.Direction);
        // Rewindable because the cache replays from storage — a consumer branching on this must see the truth.
        Assert.True(adapter.IsRewindable);
    }

    [Fact]
    public async Task Enqueue_Then_Receive_InOrder()
    {
        var options = this.Options();
        var (factory, adapter) = await this.Create(options);
        var streamId = StreamId.Create("ns", Guid.NewGuid());
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        for (var i = 1; i <= 5; i++)
            await adapter.QueueMessageBatchAsync<string>(streamId, [$"e{i}"], null!, []);

        var receiver = adapter.CreateReceiver(queueId);
        await receiver.Initialize(TimeSpan.FromSeconds(30));
        var batches = await this.Drain(receiver, options);

        var events = batches.SelectMany(b => b.GetEvents<string>().Select(t => t.Item1)).ToList();
        Assert.Equal(["e1", "e2", "e3", "e4", "e5"], events);

        // Sequence tokens must be strictly increasing across batches — that is what a cursor and a rewind
        // token both rely on.
        var seqs = batches.Cast<DocumentDbBatchContainer>().Select(b => b.SequenceNumber).ToList();
        Assert.Equal(seqs.OrderBy(s => s), seqs);
        Assert.Equal(seqs.Distinct().Count(), seqs.Count);
    }

    [Fact]
    public async Task Enqueue_PreservesStreamIdAndRequestContext()
    {
        var options = this.Options();
        var (factory, adapter) = await this.Create(options);
        var streamId = StreamId.Create("orders", "customer-7");
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        await adapter.QueueMessageBatchAsync<string>(streamId, ["payload"], null!, new Dictionary<string, object> { ["trace"] = "abc" });

        var receiver = adapter.CreateReceiver(queueId);
        await receiver.Initialize(TimeSpan.FromSeconds(30));
        var batch = Assert.Single(await this.Drain(receiver, options));

        Assert.Equal(streamId, batch.StreamId);
        Assert.True(batch.ImportRequestContext());
    }

    [Fact]
    public async Task Sequencing_UnderConcurrency_IsGapFreeAndLosesNothing()
    {
        // The test the whole design exists for. Concurrent producers on ONE queue contend on one counter row;
        // if sequence numbers were handed out by a native identity column, assignment order and commit order
        // would diverge and the receiver's `Seq > cursor` read would step over an event that committed late.
        // That failure is silent and load-dependent, so it is asserted directly.
        var options = this.Options();
        var (factory, adapter) = await this.Create(options);
        var streamId = StreamId.Create("ns", Guid.NewGuid());   // one stream → one queue → one counter row
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        const int producers = 8;
        const int perProducer = 10;
        await Task.WhenAll(Enumerable.Range(0, producers).Select(p => Task.Run(async () =>
        {
            for (var i = 0; i < perProducer; i++)
                await adapter.QueueMessageBatchAsync<string>(streamId, [$"p{p}-e{i}"], null!, []);
        })));

        var receiver = adapter.CreateReceiver(queueId);
        await receiver.Initialize(TimeSpan.FromSeconds(30));
        var batches = await this.Drain(receiver, options);

        var events = batches.SelectMany(b => b.GetEvents<string>().Select(t => t.Item1)).ToList();
        Assert.Equal(producers * perProducer, events.Count);
        Assert.Equal(producers * perProducer, events.Distinct().Count());

        var seqs = batches.Cast<DocumentDbBatchContainer>().Select(b => b.SequenceNumber).ToList();
        Assert.Equal(seqs.OrderBy(s => s), seqs);
        // Gap-free is the strong claim: it proves every reservation committed, in the order it was taken.
        Assert.Equal(Enumerable.Range(1, seqs.Count).Select(i => (long)i), seqs);
    }

    [Fact]
    public async Task Failover_ResumesFromTheAcknowledgementWatermark()
    {
        var options = this.Options();
        var (factory, adapter) = await this.Create(options);
        var streamId = StreamId.Create("ns", Guid.NewGuid());
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        for (var i = 1; i <= 4; i++)
            await adapter.QueueMessageBatchAsync<string>(streamId, [$"e{i}"], null!, []);

        var first = adapter.CreateReceiver(queueId);
        await first.Initialize(TimeSpan.FromSeconds(30));
        var read = await first.GetQueueMessagesAsync(2);
        Assert.Equal(2, read.Count);
        await first.MessagesDeliveredAsync(read);       // acknowledge only the first two
        await first.Shutdown(TimeSpan.FromSeconds(5));

        // A new agent picks the queue up — it must resume at the watermark, not at zero and not at whatever
        // the dead agent had read into memory.
        var second = adapter.CreateReceiver(queueId);
        await second.Initialize(TimeSpan.FromSeconds(30));
        var rest = await this.Drain(second, options);

        var events = rest.SelectMany(b => b.GetEvents<string>().Select(t => t.Item1)).ToList();
        Assert.Equal(["e3", "e4"], events);
    }

    [Fact]
    public async Task UnacknowledgedEvents_AreRedelivered()
    {
        var options = this.Options();
        var (factory, adapter) = await this.Create(options);
        var streamId = StreamId.Create("ns", Guid.NewGuid());
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        await adapter.QueueMessageBatchAsync<string>(streamId, ["only"], null!, []);

        var first = adapter.CreateReceiver(queueId);
        await first.Initialize(TimeSpan.FromSeconds(30));
        var read = await first.GetQueueMessagesAsync(10);
        Assert.Single(read);
        // Deliberately no MessagesDeliveredAsync: the agent died mid-delivery.
        await first.Shutdown(TimeSpan.FromSeconds(5));

        var second = adapter.CreateReceiver(queueId);
        await second.Initialize(TimeSpan.FromSeconds(30));
        var again = await this.Drain(second, options);

        // At-least-once, as Orleans promises. Losing it here would be silent data loss on every silo restart.
        Assert.Single(again);
        Assert.Equal("only", again[0].GetEvents<string>().Single().Item1);
    }

    [Fact]
    public async Task IdleQueue_BacksOffInsteadOfQueryingEveryPoll()
    {
        // Guards the idle-cost design: Orleans' agent timer fires ~10x/second forever, and this provider must
        // not turn that into 10 database round-trips per queue per second when nothing is streaming.
        var options = this.Options();
        var (factory, adapter) = await this.Create(options);
        var streamId = StreamId.Create("ns", Guid.NewGuid());
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        var receiver = adapter.CreateReceiver(queueId);
        await receiver.Initialize(TimeSpan.FromSeconds(30));
        await receiver.GetQueueMessagesAsync(10);       // first poll queries and finds nothing → backs off

        // An event lands, but the clock has not moved past the backoff, so the poll must not see it yet.
        await adapter.QueueMessageBatchAsync<string>(streamId, ["late"], null!, []);
        Assert.Empty(await receiver.GetQueueMessagesAsync(10));

        // Past the backoff, the same event is delivered — the backoff delays, it never drops.
        this.clock.Advance(options.MaxPollInterval + TimeSpan.FromSeconds(1));
        var batch = await receiver.GetQueueMessagesAsync(10);
        Assert.Single(batch);
    }

    [Fact]
    public async Task Retention_PurgesDeliveredEventsOnly()
    {
        var options = this.Options();
        options.Retention = TimeSpan.FromMinutes(5);
        options.PurgeInterval = TimeSpan.FromMinutes(1);

        var (factory, adapter) = await this.Create(options);
        var streamId = StreamId.Create("ns", Guid.NewGuid());
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        await adapter.QueueMessageBatchAsync<string>(streamId, ["acked"], null!, []);

        var receiver = adapter.CreateReceiver(queueId);
        await receiver.Initialize(TimeSpan.FromSeconds(30));
        var read = await receiver.GetQueueMessagesAsync(10);
        await receiver.MessagesDeliveredAsync(read);

        // A second event that is never acknowledged — retention must not touch it however old it gets.
        await adapter.QueueMessageBatchAsync<string>(streamId, ["pending"], null!, []);

        this.clock.Advance(TimeSpan.FromHours(1));
        await receiver.GetQueueMessagesAsync(10);       // this poll runs the sweep

        var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new PostgreSqlDatabaseProvider(fixture.Container.GetConnectionString()),
            TableName = options.TableName!
        });
        using (store)
        {
            var remaining = await store.Query<StreamEventDocument>().ToList();
            Assert.Single(remaining);
            Assert.Null(remaining[0].DeliveredAt);
        }
    }
}

// ── Rewind, checkpoints and contention ──────────────────────────────────────────────────────────────

[Collection("Postgres")]
public class StreamRewindTests(PostgresFixture fixture)
{
    readonly TestClock clock = new();

    IServiceProvider BuildServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSerializer(b => b.AddAssembly(typeof(DocumentDbBatchContainer).Assembly));
        services.Configure<ClusterOptions>(o =>
        {
            o.ServiceId = "svc";
            o.ClusterId = "cluster";
        });
        services.AddSingleton<TimeProvider>(this.clock);
        return services.BuildServiceProvider();
    }

    DocumentDbStreamOptions Options() => new()
    {
        DatabaseProvider = new PostgreSqlDatabaseProvider(fixture.Container.GetConnectionString()),
        TableName = "streams_" + Guid.NewGuid().ToString("N"),
        TotalQueueCount = 4,
        UseChangeFeedNudge = false
    };

    async Task<(DocumentDbQueueAdapterFactory Factory, IQueueAdapter Adapter, DocumentDbStreamOptions Options)> Create()
    {
        var options = this.Options();
        var factory = new DocumentDbQueueAdapterFactory("test", options, this.BuildServices());
        return (factory, await factory.CreateAdapter(), options);
    }

    [Fact]
    public async Task Rewind_ReplaysHistoryOlderThanTheInMemoryCache()
    {
        // The Phase 2 headline, and the thing no queue-backed provider can do: a brand-new cache holds
        // nothing, so a cursor from sequence zero can only be served by reading the table back.
        var (factory, adapter, _) = await this.Create();
        var streamId = StreamId.Create("ns", Guid.NewGuid());
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        for (var i = 1; i <= 5; i++)
            await adapter.QueueMessageBatchAsync<string>(streamId, [$"e{i}"], null!, []);

        // A cache that has never seen these events — exactly the state after a silo restart.
        var cache = factory.GetQueueAdapterCache().CreateQueueCache(queueId);
        var cursor = cache.GetCacheCursor(streamId, new EventSequenceTokenV2(0));

        var replayed = new List<string>();
        while (cursor.MoveNext())
        {
            var batch = cursor.GetCurrent(out var ex);
            Assert.Null(ex);
            if (batch != null)
                replayed.AddRange(batch.GetEvents<string>().Select(t => t.Item1));
        }

        Assert.Equal(["e1", "e2", "e3", "e4", "e5"], replayed);
    }

    [Fact]
    public async Task Rewind_FromAMidpointTokenSkipsWhatWasAlreadySeen()
    {
        var (factory, adapter, _) = await this.Create();
        var streamId = StreamId.Create("ns", Guid.NewGuid());
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        for (var i = 1; i <= 5; i++)
            await adapter.QueueMessageBatchAsync<string>(streamId, [$"e{i}"], null!, []);

        var cache = factory.GetQueueAdapterCache().CreateQueueCache(queueId);
        // "Resume after position 3" — the same semantics a live cursor has.
        var cursor = cache.GetCacheCursor(streamId, new EventSequenceTokenV2(3));

        var replayed = new List<string>();
        while (cursor.MoveNext())
        {
            var batch = cursor.GetCurrent(out _);
            if (batch != null)
                replayed.AddRange(batch.GetEvents<string>().Select(t => t.Item1));
        }

        Assert.Equal(["e4", "e5"], replayed);
    }

    [Fact]
    public async Task Rewind_OnlyReplaysTheRequestedStream()
    {
        // Two streams that hash to the same queue would interleave in the table; a rewind must return one
        // stream's events, not the queue's.
        var (factory, adapter, _) = await this.Create();
        var mapper = factory.GetStreamQueueMapper();

        StreamId wanted = default, other = default;
        var queueId = default(QueueId);
        for (var i = 0; i < 200 && (other == default); i++)
        {
            var candidate = StreamId.Create("ns", Guid.NewGuid());
            if (wanted == default)
            {
                wanted = candidate;
                queueId = mapper.GetQueueForStream(candidate);
            }
            else if (mapper.GetQueueForStream(candidate).Equals(queueId))
            {
                other = candidate;
            }
        }
        Assert.NotEqual(default, other);   // 200 tries over 4 queues; a miss here means the mapper changed

        await adapter.QueueMessageBatchAsync<string>(wanted, ["mine-1"], null!, []);
        await adapter.QueueMessageBatchAsync<string>(other, ["theirs"], null!, []);
        await adapter.QueueMessageBatchAsync<string>(wanted, ["mine-2"], null!, []);

        var cache = factory.GetQueueAdapterCache().CreateQueueCache(queueId);
        var cursor = cache.GetCacheCursor(wanted, new EventSequenceTokenV2(0));

        var replayed = new List<string>();
        while (cursor.MoveNext())
        {
            var batch = cursor.GetCurrent(out _);
            if (batch != null)
                replayed.AddRange(batch.GetEvents<string>().Select(t => t.Item1));
        }

        Assert.Equal(["mine-1", "mine-2"], replayed);
    }

    [Fact]
    public async Task Checkpoint_SurvivesRetentionSweepingEveryDeliveredRow()
    {
        // The failure the checkpoint exists for. Derived from the rows alone, a queue whose delivered events
        // have all aged out rebuilds a cursor of zero — and with history now retained for rewind, that means
        // replaying everything still on the table after a restart.
        var (factory, adapter, options) = await this.Create();
        var streamId = StreamId.Create("ns", Guid.NewGuid());
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        await adapter.QueueMessageBatchAsync<string>(streamId, ["delivered"], null!, []);

        var first = adapter.CreateReceiver(queueId);
        await first.Initialize(TimeSpan.FromSeconds(30));
        var read = await first.GetQueueMessagesAsync(10);
        Assert.Single(read);
        await first.MessagesDeliveredAsync(read);
        await first.Shutdown(TimeSpan.FromSeconds(5));

        // Sweep every delivered row away, exactly as retention eventually does.
        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new PostgreSqlDatabaseProvider(fixture.Container.GetConnectionString()),
            TableName = options.TableName!
        });
        await store.Query<StreamEventDocument>().Where(e => e.DeliveredAt != null).ExecuteDelete();

        var second = adapter.CreateReceiver(queueId);
        await second.Initialize(TimeSpan.FromSeconds(30));
        this.clock.Advance(TimeSpan.FromSeconds(5));

        // Nothing to redeliver: the checkpoint remembers what the rows no longer can.
        Assert.Empty(await second.GetQueueMessagesAsync(10));
    }

    [Fact]
    public async Task ConcurrentProducersAcrossQueues_KeepEachQueueGapFree()
    {
        // Contention across several counter rows at once — the shape that surfaces a serialization failure on
        // an engine that aborts rather than waits (CockroachDB), and that the enqueue retry exists for.
        var (factory, adapter, options) = await this.Create();
        var streams = Enumerable.Range(0, 6).Select(_ => StreamId.Create("ns", Guid.NewGuid())).ToList();

        await Task.WhenAll(streams.SelectMany(s => Enumerable.Range(0, 8).Select(i => Task.Run(async () =>
            await adapter.QueueMessageBatchAsync<string>(s, [$"e{i}"], null!, [])))));

        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new PostgreSqlDatabaseProvider(fixture.Container.GetConnectionString()),
            TableName = options.TableName!
        });

        var all = await store.Query<StreamEventDocument>().ToList();
        Assert.Equal(48, all.Count);

        foreach (var byQueue in all.GroupBy(e => e.QueueId))
        {
            var seqs = byQueue.Select(e => e.Seq).OrderBy(s => s).ToList();
            // Gap-free per queue: every reservation committed, in the order it was taken.
            Assert.Equal(Enumerable.Range(1, seqs.Count).Select(i => (long)i), seqs);
        }
    }
}
