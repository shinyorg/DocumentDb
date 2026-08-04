using System.Diagnostics;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The outbox engine. SQLite unless a test needs real concurrency, in which case it needs a file database —
/// an in-memory SQLite store pins one connection and would serialize the very race being tested.
/// </summary>
public class OutboxTests : IDisposable
{
    readonly List<string> files = new();

    public void Dispose()
    {
        foreach (var file in this.files)
        {
            try
            {
                File.Delete(file);
            }
            catch (IOException)
            {
                // A held handle on a temp file is not a test failure.
            }
        }
    }

    // ── Fixtures ────────────────────────────────────────────────────────

    public class Order
    {
        public string Id { get; set; } = "";
        public string Customer { get; set; } = "";
        public decimal Total { get; set; }
    }

    public record OrderPlaced(string OrderId, decimal Total);

    public record OrderChanged(string OrderId, string Operation);

    /// <summary>A controllable clock. Only <c>GetUtcNow</c> is needed — every test drives
    /// <see cref="OutboxRunner"/> directly rather than waiting on a timer.</summary>
    sealed class TestClock(DateTimeOffset start) : TimeProvider
    {
        DateTimeOffset now = start;
        public override DateTimeOffset GetUtcNow() => this.now;
        public void Advance(TimeSpan by) => this.now = this.now.Add(by);
    }

    /// <summary>The smallest scope that carries a clock — <c>Enqueue</c> reads <see cref="TimeProvider"/> from
    /// the session's services, so a test with a controlled clock has to open its session with one.</summary>
    sealed class ClockScope(TimeProvider clock) : IServiceProvider
    {
        public object? GetService(Type serviceType) => serviceType == typeof(TimeProvider) ? clock : null;
    }

    sealed class RecordingDispatcher : IOutboxDispatcher
    {
        readonly Lock gate = new();

        /// <summary>Every message the runner handed over, including the ones the callback then rejected.</summary>
        public List<OutboxMessage> Attempted { get; } = new();

        /// <summary>Only the messages the callback accepted.</summary>
        public List<OutboxMessage> Delivered { get; } = new();

        public Func<OutboxMessage, Task>? OnDispatch { get; set; }

        public async Task Dispatch(OutboxMessage message, CancellationToken cancellationToken)
        {
            lock (this.gate)
                this.Attempted.Add(message);

            if (this.OnDispatch != null)
                await this.OnDispatch(message);

            lock (this.gate)
                this.Delivered.Add(message);
        }
    }

    DocumentStore CreateStore(Action<DocumentStoreOptions>? configure = null, bool onDisk = false)
    {
        string connection;
        if (onDisk)
        {
            var path = Path.Combine(Path.GetTempPath(), $"outbox_{Guid.NewGuid():N}.db");
            this.files.Add(path);
            connection = $"Data Source={path}";
        }
        else
        {
            connection = "Data Source=:memory:";
        }

        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connection),
            TableName = $"t{Guid.NewGuid():N}"
        };
        options.AddOutbox($"ob{Guid.NewGuid():N}");
        configure?.Invoke(options);
        return new DocumentStore(options);
    }

    static OutboxOptions Fast(Action<OutboxOptions>? configure = null)
    {
        var options = new OutboxOptions
        {
            MaxAttempts = 3,
            Backoff = attempt => TimeSpan.FromSeconds(attempt * 10)
        };
        configure?.Invoke(options);
        return options;
    }

    static Task<IReadOnlyList<OutboxMessage>> AllMessages(IDocumentStore store)
        => store.Query<OutboxMessage>().OrderBy(m => m.Id).ToList();

    // ── Enqueue + atomicity ─────────────────────────────────────────────

    [Fact]
    public async Task Enqueue_CommitsWithTheAggregate()
    {
        using var store = this.CreateStore();

        await using (var session = store.OpenSession())
        {
            session.Add(new Order { Id = "o1", Customer = "Alice", Total = 42m })
                   .Enqueue(new OrderPlaced("o1", 42m));
            await session.SaveChanges();
        }

        Assert.NotNull(await store.Get<Order>("o1"));
        var messages = await AllMessages(store);
        var message = Assert.Single(messages);
        Assert.Equal(typeof(OrderPlaced).FullName, message.MessageType);
        Assert.Equal(0, message.Attempts);
        Assert.Null(message.ProcessedAt);
        Assert.Contains("\"orderId\":\"o1\"", message.Payload);
    }

    [Fact]
    public async Task Enqueue_RolledBackWithTheAggregate()
    {
        var reject = false;
        using var store = this.CreateStore(options => options.ConfigureDocument<Order>(cfg =>
            cfg.OnBeforeWrite((_, _) => reject ? throw new InvalidOperationException("aggregate rejected") : Task.CompletedTask)));

        // A canary that commits first. Both tables are created lazily, and the rollback below would take their
        // CREATE TABLE with it — so the assertion afterwards needs them to already exist.
        await using (var session = store.OpenSession())
        {
            session.Add(new Order { Id = "canary" }).Enqueue(new OrderPlaced("canary", 1m));
            await session.SaveChanges();
        }

        reject = true;
        await using (var session = store.OpenSession())
        {
            // Enqueue FIRST, so the message insert genuinely runs and is genuinely rolled back — if it were
            // buffered after the failing write it would simply never execute and prove nothing.
            session.Enqueue(new OrderPlaced("o1", 42m))
                   .Add(new Order { Id = "o1", Customer = "Alice", Total = 42m });

            await Assert.ThrowsAsync<InvalidOperationException>(() => session.SaveChanges());
        }

        var message = Assert.Single(await AllMessages(store));
        Assert.Contains("canary", message.Payload);
        Assert.Null(await store.Get<Order>("o1"));
    }

    [Fact]
    public async Task Enqueue_LandsInTheMappedTable()
    {
        var table = $"ob{Guid.NewGuid():N}";
        var documents = $"t{Guid.NewGuid():N}";
        var path = Path.Combine(Path.GetTempPath(), $"outbox_{Guid.NewGuid():N}.db");
        this.files.Add(path);

        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={path}"),
            TableName = documents
        };
        options.AddOutbox(table);

        using (var store = new DocumentStore(options))
        {
            await using var session = store.OpenSession();
            session.Add(new Order { Id = "o1" }).Enqueue(new OrderPlaced("o1", 1m));
            await session.SaveChanges();
        }

        using var connection = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={path}");
        await connection.OpenAsync();

        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
            Assert.Equal(1L, Convert.ToInt64(await cmd.ExecuteScalarAsync()));
        }
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM {documents} WHERE TypeName = 'OutboxMessage'";
            Assert.Equal(0L, Convert.ToInt64(await cmd.ExecuteScalarAsync()));
        }
    }

    // ── PublishToOutbox ─────────────────────────────────────────────────

    [Fact]
    public async Task PublishToOutbox_PublishesOneMessagePerWrite()
    {
        using var store = this.CreateStore(options => options.ConfigureDocument<Order>(cfg =>
            cfg.PublishToOutbox(ctx => new OrderChanged((string)ctx.Id!, ctx.Operation.ToString()))));

        await store.Insert(new Order { Id = "o1", Total = 5m });
        await store.Update(new Order { Id = "o1", Total = 6m });
        await store.Remove<Order>("o1");

        var messages = await AllMessages(store);
        Assert.Equal(3, messages.Count);
        Assert.All(messages, m => Assert.Equal(typeof(OrderChanged).FullName, m.MessageType));
        Assert.Equal(
            ["Insert", "Update", "Delete"],
            messages.Select(m => JsonNode.Parse(m.Payload)!["operation"]!.GetValue<string>()).ToArray());
    }

    [Fact]
    public async Task PublishToOutbox_HonoursTheOperationFilter()
    {
        using var store = this.CreateStore(options => options.ConfigureDocument<Order>(cfg =>
            cfg.PublishToOutbox(ctx => new OrderChanged((string)ctx.Id!, ctx.Operation.ToString()), OutboxOperations.Delete)));

        await store.Insert(new Order { Id = "o1" });
        Assert.Empty(await AllMessages(store));

        await store.Remove<Order>("o1");
        Assert.Single(await AllMessages(store));
    }

    [Fact]
    public async Task PublishToOutbox_SkipsWhenTheFactoryReturnsNull()
    {
        using var store = this.CreateStore(options => options.ConfigureDocument<Order>(cfg =>
            cfg.PublishToOutbox(ctx => ((Order?)ctx.Document)?.Total > 10m ? new OrderPlaced("o", 0m) : null)));

        await store.Insert(new Order { Id = "small", Total = 1m });
        await store.Insert(new Order { Id = "big", Total = 100m });

        Assert.Single(await AllMessages(store));
    }

    [Fact]
    public async Task PublishToOutbox_IsSkippedBySuppressInterceptors()
    {
        using var store = this.CreateStore(options => options.ConfigureDocument<Order>(cfg =>
            cfg.PublishToOutbox(ctx => new OrderChanged((string)ctx.Id!, ctx.Operation.ToString()))));

        using (store.SuppressInterceptors())
            await store.Insert(new Order { Id = "o1" });

        Assert.Empty(await AllMessages(store));
    }

    // ── Delivery ────────────────────────────────────────────────────────

    [Fact]
    public async Task DrainOnce_DispatchesAndAcknowledges()
    {
        using var store = this.CreateStore();
        await Enqueue(store, 3);

        var dispatcher = new RecordingDispatcher();
        var runner = new OutboxRunner(store, dispatcher, Fast());

        Assert.Equal(3, await runner.DrainOnce());
        Assert.Equal(3, dispatcher.Delivered.Count);

        var messages = await AllMessages(store);
        Assert.All(messages, m => Assert.NotNull(m.ProcessedAt));
        Assert.All(messages, m => Assert.Equal(1, m.Attempts));
        Assert.Equal(0, await runner.PendingDepth());
    }

    [Fact]
    public async Task DrainOnce_DeliversInEnqueueOrder()
    {
        using var store = this.CreateStore();
        await Enqueue(store, 5);

        var dispatcher = new RecordingDispatcher();
        await new OutboxRunner(store, dispatcher, Fast(o => o.MaxParallelism = 1)).DrainOnce();

        var payloads = dispatcher.Delivered.Select(m => JsonNode.Parse(m.Payload)!["orderId"]!.GetValue<string>()).ToArray();
        Assert.Equal(["o0", "o1", "o2", "o3", "o4"], payloads);
    }

    [Fact]
    public async Task Retry_IncrementsAttemptsAndBacksOff()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 8, 4, 12, 0, 0, TimeSpan.Zero));
        using var store = this.CreateStore();
        await Enqueue(store, 1, clock);

        var dispatcher = new RecordingDispatcher { OnDispatch = _ => throw new InvalidOperationException("bus is down") };
        var runner = new OutboxRunner(store, dispatcher, Fast(), clock);

        await runner.DrainOnce();

        var message = Assert.Single(await AllMessages(store));
        Assert.Equal(1, message.Attempts);
        Assert.Null(message.DeadLetteredAt);
        Assert.Contains("bus is down", message.Error);
        Assert.Equal(clock.GetUtcNow().AddSeconds(10), message.AvailableAt);

        // Still backed off: nothing is eligible, so a second pass claims nothing.
        Assert.Equal(0, await runner.PendingDepth());
        Assert.Equal(0, await runner.DrainOnce());

        clock.Advance(TimeSpan.FromSeconds(11));
        Assert.Equal(1, await runner.DrainOnce());
        Assert.Equal(2, (await AllMessages(store)).Single().Attempts);
    }

    [Fact]
    public async Task DeadLetters_AfterMaxAttempts_AndIsNeverRedelivered()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 8, 4, 12, 0, 0, TimeSpan.Zero));
        using var store = this.CreateStore();
        await Enqueue(store, 1, clock);

        var dispatcher = new RecordingDispatcher { OnDispatch = _ => throw new InvalidOperationException("poison") };
        var options = Fast();
        var runner = new OutboxRunner(store, dispatcher, options, clock);

        for (var i = 0; i < options.MaxAttempts; i++)
        {
            await runner.DrainOnce();
            clock.Advance(TimeSpan.FromMinutes(10));
        }

        var message = Assert.Single(await AllMessages(store));
        Assert.Equal(options.MaxAttempts, message.Attempts);
        Assert.NotNull(message.DeadLetteredAt);
        Assert.Contains("poison", message.Error);

        var delivered = dispatcher.Delivered.Count;
        clock.Advance(TimeSpan.FromDays(1));
        Assert.Equal(0, await runner.DrainOnce());
        Assert.Equal(delivered, dispatcher.Delivered.Count);
    }

    [Fact]
    public async Task ClaimRace_DeliversEachMessageExactlyOnce()
    {
        using var store = this.CreateStore(onDisk: true);
        await Enqueue(store, 100);

        var dispatcher = new RecordingDispatcher();
        var options = Fast(o =>
        {
            o.BatchSize = 100;
            o.MaxParallelism = 8;
        });

        var runners = Enumerable.Range(0, 4).Select(_ => new OutboxRunner(store, dispatcher, options)).ToList();
        await Task.WhenAll(runners.Select(r => r.DrainOnce()));

        Assert.Equal(100, dispatcher.Delivered.Select(m => m.Id).Distinct().Count());
        Assert.Equal(100, dispatcher.Delivered.Count);
        Assert.All(await AllMessages(store), m => Assert.Equal(1, m.Attempts));
    }

    // ── Ordered partitions ──────────────────────────────────────────────

    [Fact]
    public async Task OrderedPartitions_DeliversEachKeyInOrder()
    {
        using var store = this.CreateStore();
        await using (var session = store.OpenSession())
        {
            for (var i = 0; i < 5; i++)
            {
                session.Enqueue(new OrderPlaced($"a{i}", i), partitionKey: "a");
                session.Enqueue(new OrderPlaced($"b{i}", i), partitionKey: "b");
            }
            await session.SaveChanges();
        }

        var dispatcher = new RecordingDispatcher
        {
            // Yield so the partitions genuinely interleave; ordering must survive it.
            OnDispatch = _ => Task.Delay(1)
        };
        await new OutboxRunner(store, dispatcher, Fast(o =>
        {
            o.OrderedPartitions = true;
            o.MaxParallelism = 4;
        })).DrainOnce();

        foreach (var key in new[] { "a", "b" })
        {
            var ids = dispatcher.Delivered
                .Where(m => m.PartitionKey == key)
                .Select(m => JsonNode.Parse(m.Payload)!["orderId"]!.GetValue<string>())
                .ToArray();
            Assert.Equal([$"{key}0", $"{key}1", $"{key}2", $"{key}3", $"{key}4"], ids);
        }
    }

    [Fact]
    public async Task OrderedPartitions_AStuckPartitionDoesNotBlockOthers()
    {
        using var store = this.CreateStore();
        await using (var session = store.OpenSession())
        {
            for (var i = 0; i < 3; i++)
            {
                session.Enqueue(new OrderPlaced($"bad{i}", i), partitionKey: "bad");
                session.Enqueue(new OrderPlaced($"good{i}", i), partitionKey: "good");
            }
            await session.SaveChanges();
        }

        var dispatcher = new RecordingDispatcher
        {
            OnDispatch = m => m.PartitionKey == "bad" ? throw new InvalidOperationException("poison") : Task.CompletedTask
        };
        await new OutboxRunner(store, dispatcher, Fast(o => o.OrderedPartitions = true)).DrainOnce();

        Assert.Equal(3, dispatcher.Delivered.Count(m => m.PartitionKey == "good"));
        // The poison message stops its own partition after the first failure, and only that one.
        Assert.Equal(1, dispatcher.Attempted.Count(m => m.PartitionKey == "bad"));
        Assert.Empty(dispatcher.Delivered.Where(m => m.PartitionKey == "bad"));

        var stuck = (await AllMessages(store)).Where(m => m.PartitionKey == "bad").ToList();
        Assert.Equal(1, stuck.Count(m => m.Attempts == 1));
        Assert.Equal(2, stuck.Count(m => m.Attempts == 0));
    }

    // ── Retention ───────────────────────────────────────────────────────

    [Fact]
    public async Task PurgeExpired_RemovesOnlyOldAcknowledgedMessages()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 8, 4, 12, 0, 0, TimeSpan.Zero));
        using var store = this.CreateStore();
        await Enqueue(store, 3, clock);

        var dispatcher = new RecordingDispatcher();
        var options = Fast(o => o.Retention = TimeSpan.FromHours(1));
        var runner = new OutboxRunner(store, dispatcher, options, clock);

        // One delivered, one dead-lettered, one left pending.
        var all = await AllMessages(store);
        dispatcher.OnDispatch = m => m.Id == all[1].Id ? throw new InvalidOperationException("poison") : Task.CompletedTask;
        for (var i = 0; i < options.MaxAttempts; i++)
        {
            await runner.DrainOnce();
            clock.Advance(TimeSpan.FromMinutes(5));
        }
        await store.Remove<OutboxMessage>(all[2].Id);
        await store.Insert(new OutboxMessage { Id = all[2].Id, MessageType = "Fresh", Payload = "{}", CreatedAt = clock.GetUtcNow(), AvailableAt = clock.GetUtcNow().AddDays(1) });

        clock.Advance(TimeSpan.FromDays(1));
        var purged = await runner.PurgeExpired();

        Assert.Equal(1, purged);
        var survivors = await AllMessages(store);
        Assert.Equal(2, survivors.Count);
        Assert.Contains(survivors, m => m.DeadLetteredAt != null);
        Assert.Contains(survivors, m => m.MessageType == "Fresh");
    }

    [Fact]
    public async Task PurgeExpired_IsANoOpWithoutRetention()
    {
        using var store = this.CreateStore();
        await Enqueue(store, 1);
        await new OutboxRunner(store, new RecordingDispatcher(), Fast()).DrainOnce();

        var runner = new OutboxRunner(store, new RecordingDispatcher(), Fast(o => o.Retention = null));
        Assert.Equal(0, await runner.PurgeExpired());
        Assert.Single(await AllMessages(store));
    }

    // ── IOutboxAdmin ────────────────────────────────────────────────────

    [Fact]
    public async Task Requeue_ClearsTheDeadLetterStateAndRedelivers()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 8, 4, 12, 0, 0, TimeSpan.Zero));
        using var store = this.CreateStore();
        await Enqueue(store, 1, clock);

        var dispatcher = new RecordingDispatcher { OnDispatch = _ => throw new InvalidOperationException("poison") };
        var options = Fast();
        var runner = new OutboxRunner(store, dispatcher, options, clock);
        for (var i = 0; i < options.MaxAttempts; i++)
        {
            await runner.DrainOnce();
            clock.Advance(TimeSpan.FromMinutes(10));
        }

        var admin = new OutboxAdmin(store, options, clock);
        Assert.Equal(1, await admin.DeadLetterCount());

        var deadLetters = await admin.DeadLetters();
        Assert.Equal(1, await admin.Requeue(deadLetters.Select(m => m.Id)));

        var requeued = Assert.Single(await AllMessages(store));
        Assert.Null(requeued.DeadLetteredAt);
        Assert.Null(requeued.Error);
        Assert.Equal(0, requeued.Attempts);
        Assert.Equal(clock.GetUtcNow(), requeued.AvailableAt);

        dispatcher.OnDispatch = null;
        Assert.Equal(1, await runner.DrainOnce());
        Assert.NotNull((await AllMessages(store)).Single().ProcessedAt);
    }

    [Fact]
    public async Task Requeue_IgnoresScheduledAndProcessedMessages()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 8, 4, 12, 0, 0, TimeSpan.Zero));
        using var store = this.CreateStore();
        await Enqueue(store, 2, clock);

        var all = await AllMessages(store);
        var dispatcher = new RecordingDispatcher
        {
            OnDispatch = m => m.Id == all[1].Id ? throw new InvalidOperationException("later") : Task.CompletedTask
        };
        var options = Fast();
        await new OutboxRunner(store, dispatcher, options, clock).DrainOnce();

        // all[0] is now processed, all[1] is scheduled (one failed attempt, backing off).
        var admin = new OutboxAdmin(store, options, clock);
        Assert.Equal(0, await admin.Requeue([all[0].Id, all[1].Id]));

        var after = await AllMessages(store);
        Assert.NotNull(after.Single(m => m.Id == all[0].Id).ProcessedAt);
        Assert.Equal(1, after.Single(m => m.Id == all[1].Id).Attempts);
    }

    [Fact]
    public async Task Admin_CountsAndOldestPending()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 8, 4, 12, 0, 0, TimeSpan.Zero));
        using var store = this.CreateStore();

        await using (var session = store.OpenSession(new ClockScope(clock)))
        {
            session.EnqueueRaw("A", "{}");
            await session.SaveChanges();
        }
        var first = (await AllMessages(store)).Single();

        clock.Advance(TimeSpan.FromMinutes(5));
        await using (var session = store.OpenSession(new ClockScope(clock)))
        {
            session.EnqueueRaw("B", "{}");
            await session.SaveChanges();
        }

        var admin = new OutboxAdmin(store, Fast(), clock);
        Assert.Equal(2, await admin.PendingCount());
        Assert.Equal(0, await admin.ScheduledCount());
        Assert.Equal(0, await admin.DeadLetterCount());

        var oldest = await admin.OldestPendingAt();
        Assert.NotNull(oldest);
        Assert.Equal(first.CreatedAt, oldest.Value, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task PurgeProcessed_LeavesPendingAndDeadLetteredAlone()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 8, 4, 12, 0, 0, TimeSpan.Zero));
        using var store = this.CreateStore();
        await Enqueue(store, 2, clock);

        var all = await AllMessages(store);
        var options = Fast();
        var dispatcher = new RecordingDispatcher
        {
            OnDispatch = m => m.Id == all[1].Id ? throw new InvalidOperationException("poison") : Task.CompletedTask
        };
        var runner = new OutboxRunner(store, dispatcher, options, clock);
        for (var i = 0; i < options.MaxAttempts; i++)
        {
            await runner.DrainOnce();
            clock.Advance(TimeSpan.FromMinutes(10));
        }

        var admin = new OutboxAdmin(store, options, clock);
        Assert.Equal(0, await admin.PurgeProcessed(new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero)));
        Assert.Equal(2, (await AllMessages(store)).Count);

        Assert.Equal(1, await admin.PurgeProcessed(clock.GetUtcNow()));
        var survivor = Assert.Single(await AllMessages(store));
        Assert.NotNull(survivor.DeadLetteredAt);
    }

    // ── WatchOutbox ─────────────────────────────────────────────────────

    [Fact]
    public async Task WatchOutbox_YieldsTheExistingBacklog()
    {
        using var store = this.CreateStore();
        await Enqueue(store, 3);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var seen = new List<OutboxMessage>();
        await foreach (var message in store.WatchOutbox(o => o.PollInterval = TimeSpan.FromMilliseconds(20), cts.Token))
        {
            seen.Add(message);
            if (seen.Count == 3)
                await cts.CancelAsync();
        }

        Assert.Equal(3, seen.Count);
    }

    [Fact]
    public async Task WatchOutbox_SkipsTheBacklogWhenIncludeExistingIsOff()
    {
        using var store = this.CreateStore();
        await Enqueue(store, 2);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var seen = new List<OutboxMessage>();

        var watching = Task.Run(async () =>
        {
            await foreach (var message in store.WatchOutbox(o =>
            {
                o.IncludeExisting = false;
                o.PollInterval = TimeSpan.FromMilliseconds(20);
            }, cts.Token))
            {
                seen.Add(message);
                await cts.CancelAsync();
            }
        });

        await Task.Delay(200);
        await using (var session = store.OpenSession())
        {
            session.EnqueueRaw("Later", "{}");
            await session.SaveChanges();
        }
        await watching;

        var message = Assert.Single(seen);
        Assert.Equal("Later", message.MessageType);
    }

    [Fact]
    public async Task WatchOutbox_YieldsEachRevisionOnce()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 8, 4, 12, 0, 0, TimeSpan.Zero));
        using var store = this.CreateStore();
        await Enqueue(store, 1, clock);

        var options = Fast(o => o.MaxAttempts = 2);
        var dispatcher = new RecordingDispatcher { OnDispatch = _ => throw new InvalidOperationException("poison") };
        var runner = new OutboxRunner(store, dispatcher, options, clock);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var revisions = new List<(int Attempts, bool Dead)>();

        var watching = Task.Run(async () =>
        {
            await foreach (var message in store.WatchOutbox(o =>
            {
                o.States = OutboxStates.All;
                o.PollInterval = TimeSpan.FromMilliseconds(20);
                o.TimeProvider = clock;
            }, cts.Token))
            {
                revisions.Add((message.Attempts, message.DeadLetteredAt != null));
                if (revisions.Count == 3)
                    await cts.CancelAsync();
            }
        });

        // Attempt 1 (scheduled), attempt 2 (dead-lettered) — three revisions in all, counting the enqueue.
        await Task.Delay(150);
        await runner.DrainOnce();
        clock.Advance(TimeSpan.FromMinutes(5));
        await Task.Delay(150);
        await runner.DrainOnce();

        await watching;

        Assert.Equal([(0, false), (1, false), (2, true)], revisions);
    }

    [Fact]
    public async Task WatchOutbox_YieldsAClockOnlyTransitionBackToPending()
    {
        var clock = new TestClock(new DateTimeOffset(2026, 8, 4, 12, 0, 0, TimeSpan.Zero));
        using var store = this.CreateStore();

        // Scheduled: nothing will write to this row again, so only the clock can make it pending. A stream
        // driven purely by change notification would never yield it.
        await store.Insert(new OutboxMessage
        {
            Id = Guid.CreateVersion7().ToString("N"),
            MessageType = "Backed off",
            Payload = "{}",
            CreatedAt = clock.GetUtcNow(),
            AvailableAt = clock.GetUtcNow().AddMinutes(5),
            Attempts = 1
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var seen = new List<OutboxMessage>();

        var watching = Task.Run(async () =>
        {
            await foreach (var message in store.WatchOutbox(o =>
            {
                o.States = OutboxStates.Pending;
                o.PollInterval = TimeSpan.FromMilliseconds(20);
                o.TimeProvider = clock;
            }, cts.Token))
            {
                seen.Add(message);
                await cts.CancelAsync();
            }
        });

        await Task.Delay(200);
        Assert.Empty(seen);

        clock.Advance(TimeSpan.FromMinutes(10));
        await watching;

        Assert.Single(seen);
    }

    [Fact]
    public async Task WatchOutbox_DoesNotClaim()
    {
        using var store = this.CreateStore(onDisk: true);
        await Enqueue(store, 10);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var watching = Task.Run(async () =>
        {
            await foreach (var _ in store.WatchOutbox(o =>
            {
                o.States = OutboxStates.All;
                o.PollInterval = TimeSpan.FromMilliseconds(5);
            }, cts.Token))
            {
                // Observation only.
            }
        });

        var dispatcher = new RecordingDispatcher();
        await new OutboxRunner(store, dispatcher, Fast()).DrainOnce();
        await cts.CancelAsync();
        await watching;

        Assert.Equal(10, dispatcher.Delivered.Count);
        Assert.All(await AllMessages(store), m =>
        {
            Assert.Equal(1, m.Attempts);
            Assert.NotNull(m.ProcessedAt);
        });
    }

    /// <summary>
    /// The nudge is in-process only, so it never fires for another writer — which is the normal case for an
    /// outbox. The poll has to carry it on its own, which is why polling is the correctness floor rather than
    /// an optimization on top of change notification.
    /// </summary>
    [Fact]
    public async Task WatchOutbox_SeesWritesFromAnotherStoreInstance()
    {
        var path = Path.Combine(Path.GetTempPath(), $"outbox_{Guid.NewGuid():N}.db");
        this.files.Add(path);
        var table = $"ob{Guid.NewGuid():N}";

        DocumentStore Open()
        {
            var options = new DocumentStoreOptions
            {
                DatabaseProvider = new SqliteDatabaseProvider($"Data Source={path}"),
                TableName = $"t{Guid.NewGuid():N}"
            };
            options.AddOutbox(table);
            return new DocumentStore(options);
        }

        using var watcher = Open();
        using var writer = Open();

        // Materialize the table before the watcher's first poll reads it.
        await using (var session = writer.OpenSession())
        {
            session.EnqueueRaw("Seed", "{}");
            await session.SaveChanges();
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var seen = new List<string>();
        var watching = Task.Run(async () =>
        {
            await foreach (var message in watcher.WatchOutbox(o => o.PollInterval = TimeSpan.FromMilliseconds(20), cts.Token))
            {
                seen.Add(message.MessageType);
                if (seen.Count == 2)
                    await cts.CancelAsync();
            }
        });

        await Task.Delay(200);
        await using (var session = writer.OpenSession())
        {
            session.EnqueueRaw("FromTheOtherInstance", "{}");
            await session.SaveChanges();
        }
        await watching;

        Assert.Equal(["Seed", "FromTheOtherInstance"], seen);
    }

    [Fact]
    public async Task WatchOutbox_Typed_DecodesAndFiltersByMessageType()
    {
        using var store = this.CreateStore();
        await using (var session = store.OpenSession())
        {
            session.Enqueue(new OrderPlaced("o1", 42m));
            session.Enqueue(new OrderChanged("o1", "Update"));
            await session.SaveChanges();
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var seen = new List<OutboxEnvelope<OrderPlaced>>();
        await foreach (var envelope in store.WatchOutbox<OrderPlaced>(o => o.PollInterval = TimeSpan.FromMilliseconds(20), cts.Token))
        {
            seen.Add(envelope);
            await cts.CancelAsync();
        }

        var only = Assert.Single(seen);
        Assert.Equal("o1", only.Event.OrderId);
        Assert.Equal(42m, only.Event.Total);
    }

    [Fact]
    public async Task WatchOutbox_StopsPromptlyOnCancellation()
    {
        using var store = this.CreateStore();
        using var cts = new CancellationTokenSource();

        var watching = Task.Run(async () =>
        {
            await foreach (var _ in store.WatchOutbox(o => o.PollInterval = TimeSpan.FromMinutes(5), cts.Token))
            {
                // Never reached — nothing is enqueued.
            }
        });

        await Task.Delay(100);
        await cts.CancelAsync();

        // Far shorter than the 5-minute poll interval: cancelling must break the wait, not wait it out.
        await watching.WaitAsync(TimeSpan.FromSeconds(5));
    }

    // ── Telemetry ───────────────────────────────────────────────────────

    [Fact]
    public async Task Dispatch_LinksItsSpanToTheEnqueueingActivity()
    {
        using var source = new ActivitySource("OutboxTests.Enqueue");
        var captured = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name is "OutboxTests.Enqueue" or "Shiny.DocumentDb",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = captured.Add
        };
        ActivitySource.AddActivityListener(listener);

        using var store = this.CreateStore();

        string traceId;
        using (var enqueueing = source.StartActivity("place-order")!)
        {
            traceId = enqueueing.TraceId.ToString();
            await using var session = store.OpenSession();
            session.Enqueue(new OrderPlaced("o1", 1m));
            await session.SaveChanges();
        }

        await new OutboxRunner(store, new RecordingDispatcher(), Fast()).DrainOnce();

        var dispatch = Assert.Single(captured, a => a.OperationName == "outbox.dispatch");
        Assert.Equal(traceId, dispatch.TraceId.ToString());
    }

    // ── Registration + gate ─────────────────────────────────────────────

    [Fact]
    public async Task AddDocumentOutbox_ThrowsAtStartupOnANonTransactionalStore()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IDocumentStore>(new NonTransactionalStore());
        services.AddDocumentOutbox((_, _) => Task.CompletedTask);

        using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().OfType<OutboxProcessor>().Single();

        var error = await Assert.ThrowsAsync<NotSupportedException>(() => hosted.StartAsync(CancellationToken.None));
        Assert.Contains(nameof(NonTransactionalStore), error.Message);
    }

    [Fact]
    public async Task AddDocumentOutbox_StartsOnATransactionalStore()
    {
        using var store = this.CreateStore();
        var services = new ServiceCollection();
        services.AddSingleton<IDocumentStore>(store);
        services.AddDocumentOutbox((_, _) => Task.CompletedTask, o => o.PollInterval = TimeSpan.FromMinutes(5));

        using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().OfType<OutboxProcessor>().Single();

        await hosted.StartAsync(CancellationToken.None);
        await hosted.StopAsync(CancellationToken.None);

        Assert.NotNull(provider.GetRequiredService<IOutboxAdmin>());
    }

    [Fact]
    public void AddOutbox_MapsTheVersionPropertyForClaiming()
    {
        var options = new DocumentStoreOptions { DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:") };
        options.AddOutbox();

        Assert.NotNull(options.Mappings.ResolveVersionMapping(typeof(OutboxMessage)));
    }

    // ── Wire-name guard ─────────────────────────────────────────────────

    /// <summary>
    /// The admin tool addresses outbox rows by their serialized names, so a dropped
    /// <c>[JsonPropertyName]</c> — or an application that sets its own naming policy — would turn every admin
    /// filter into a silent zero-row result. That is the worst failure available here, because an empty outbox
    /// screen reads as "nothing is stuck".
    /// </summary>
    [Fact]
    public void OutboxMessage_KeepsItsPinnedWireNamesUnderAHostileNamingPolicy()
    {
        var message = new OutboxMessage
        {
            Id = "abc",
            MessageType = "T",
            Payload = "{}",
            PartitionKey = "p",
            Headers = new Dictionary<string, string> { ["traceparent"] = "x" },
            CreatedAt = DateTimeOffset.UnixEpoch,
            AvailableAt = DateTimeOffset.UnixEpoch,
            Attempts = 1,
            ProcessedAt = DateTimeOffset.UnixEpoch,
            DeadLetteredAt = DateTimeOffset.UnixEpoch,
            Error = "boom",
            Version = 2
        };

        var camel = Keys(JsonSerializer.Serialize(message, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }));
        var hostile = Keys(JsonSerializer.Serialize(message, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower }));
        var generated = Keys(JsonSerializer.Serialize(message, OutboxJsonContext.Default.OutboxMessage));

        Assert.Equal(OutboxFields.All.Order(), camel.Order());
        Assert.Equal(OutboxFields.All.Order(), hostile.Order());
        Assert.Equal(OutboxFields.All.Order(), generated.Order());

        static IEnumerable<string> Keys(string json) => JsonNode.Parse(json)!.AsObject().Select(p => p.Key);
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    static async Task Enqueue(IDocumentStore store, int count, TimeProvider? clock = null)
    {
        await using var session = clock == null ? store.OpenSession() : store.OpenSession(new ClockScope(clock));
        for (var i = 0; i < count; i++)
            session.Enqueue(new OrderPlaced($"o{i}", i));
        await session.SaveChanges();
    }

    /// <summary>A store whose unit of work is not a transaction — the shape the outbox gate must refuse.</summary>
    sealed class NonTransactionalStore : IDocumentStore
    {
        static NotSupportedException Unused() => new("Test double: the outbox gate rejects this store before anything else runs.");

        public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class => throw Unused();
        public Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class => throw Unused();
        public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class => throw Unused();
    }
}
