using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The outbox screen's service layer, over a real SQLite database seeded through a real store.
/// </summary>
/// <remarks>
/// Worth exercising end to end because the interesting parts are exactly the ones a unit test cannot reach:
/// that the derived state filters compile to SQL the engine accepts, that the <c>:date</c> hint makes the
/// pending/scheduled split actually work, and — the pair that justifies building the engine and the admin
/// together — that a message the engine dead-lettered can be requeued <i>through this service</i> and then
/// delivered by the engine's own processor. Nothing else proves the two halves agree on the row shape.
/// </remarks>
public sealed class OutboxIntegrationTests : IAsyncLifetime
{
    const string OutboxTable = "outbox";
    const string DocumentsTable = "documents";

    string directory = "";
    string databasePath = "";
    string profileId = "";
    DocumentAdminService admin = null!;
    ConnectionManager connections = null!;
    ProvidedConnections provided = null!;

    public ValueTask InitializeAsync()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdboutbox_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);
        this.databasePath = Path.Combine(this.directory, "store.db");
        this.Build(readOnly: false);
        return ValueTask.CompletedTask;
    }

    void Build(bool readOnly)
    {
        this.connections?.Dispose();

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "admin"),
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                ["ShinyDocDbMyAdmin:ReadOnly"] = readOnly ? "true" : "false",
                ["ConnectionStrings:store"] = $"Data Source={this.databasePath}",
                ["Shiny:DocumentDb:store:Provider"] = "Sqlite"
            })
            .Build();

        var paths = new AppPaths(configuration);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        this.provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        var profiles = new ProfileStore(paths, protector, this.provided,
            new DemoMode(configuration, NullLogger<DemoMode>.Instance));

        this.profileId = this.provided.Profiles.Single().Id;
        this.connections = new ConnectionManager(profiles, NullLogger<ConnectionManager>.Instance);
        this.admin = new DocumentAdminService(this.connections, NullLogger<DocumentAdminService>.Instance);
    }

    public ValueTask DisposeAsync()
    {
        this.connections?.Dispose();
        if (Directory.Exists(this.directory))
        {
            try { Directory.Delete(this.directory, recursive: true); }
            catch (IOException) { /* a driver still holding the file is not a test failure */ }
        }
        return ValueTask.CompletedTask;
    }

    // ── Seeding ─────────────────────────────────────────────────────────

    public record Placed(string OrderId);

    DocumentStore OpenStore(string? outboxTable = OutboxTable, TypeNameResolution resolution = TypeNameResolution.ShortName)
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.databasePath}"),
            TableName = DocumentsTable,
            TypeNameResolution = resolution
        };
        options.AddOutbox(outboxTable);
        return new DocumentStore(options);
    }

    sealed class Rejecting(Func<OutboxMessage, bool> deliverable) : IOutboxDispatcher
    {
        public List<string> Delivered { get; } = new();

        public Task Dispatch(OutboxMessage message, CancellationToken cancellationToken)
        {
            if (!deliverable(message))
                throw new InvalidOperationException($"consumer refused {message.Id}");
            this.Delivered.Add(message.Id);
            return Task.CompletedTask;
        }
    }

    static OutboxOptions Options() => new() { MaxAttempts = 1, Backoff = _ => TimeSpan.Zero };

    /// <summary>Seeds one message in each of the four states, plus the message type variety the grid filters on.</summary>
    async Task<IReadOnlyList<OutboxMessage>> SeedAllStates()
    {
        using var store = this.OpenStore();

        await using (var session = store.OpenSession())
        {
            session.Enqueue(new Placed("processed"));
            session.Enqueue(new Placed("dead"));
            await session.SaveChanges();
        }

        // The first delivers; the second dead-letters on its single allowed attempt.
        var all = await store.Query<OutboxMessage>().OrderBy(m => m.Id).ToList();
        await new OutboxRunner(store, new Rejecting(m => m.Id == all[0].Id), Options()).DrainOnce();

        // A third, enqueued after the drain, so it is still pending.
        await using (var session = store.OpenSession())
        {
            session.Enqueue(new Placed("pending"), partitionKey: "north");
            await session.SaveChanges();
        }

        // A fourth that is scheduled — undelivered, but backed off into the future.
        await store.Insert(new OutboxMessage
        {
            Id = Guid.CreateVersion7().ToString("N"),
            MessageType = "Scheduled.Event",
            Payload = """{"orderId":"scheduled"}""",
            CreatedAt = DateTimeOffset.UtcNow,
            AvailableAt = DateTimeOffset.UtcNow.AddHours(1),
            Attempts = 1
        });

        return await store.Query<OutboxMessage>().OrderBy(m => m.Id).ToList();
    }

    async Task<OutboxLocation> Located()
    {
        var found = await this.admin.FindOutboxes(this.profileId);
        return Assert.Single(found);
    }

    // ── Discovery ───────────────────────────────────────────────────────

    [Fact]
    public async Task FindOutboxes_FindsTheMappedTable()
    {
        await this.SeedAllStates();

        var outbox = await this.Located();
        Assert.Equal(OutboxTable, outbox.Table);
        Assert.Equal(nameof(OutboxMessage), outbox.TypeName);
        Assert.Equal(4, outbox.Count);
        Assert.True(outbox.Addressable);
    }

    [Fact]
    public async Task FindOutboxes_FindsAnOutboxSharingTheDocumentsTable()
    {
        using (var store = this.OpenStore(outboxTable: null))
        {
            await using var session = store.OpenSession();
            session.Enqueue(new Placed("o1"));
            await session.SaveChanges();
        }

        var outbox = await this.Located();
        Assert.Equal(DocumentsTable, outbox.Table);
    }

    [Fact]
    public async Task FindOutboxes_FindsADottedTypeName_ButMarksItUnaddressable()
    {
        using (var store = this.OpenStore(resolution: TypeNameResolution.FullName))
        {
            await using var session = store.OpenSession();
            session.Enqueue(new Placed("o1"));
            await session.SaveChanges();
        }

        var outbox = await this.Located();
        Assert.Equal(typeof(OutboxMessage).FullName, outbox.TypeName);
        Assert.False(outbox.Addressable);
    }

    [Fact]
    public async Task FindOutboxes_ReturnsEveryHitWhenTwoTablesHoldOutboxes()
    {
        using (var shared = this.OpenStore(outboxTable: null))
        {
            await using var session = shared.OpenSession();
            session.Enqueue(new Placed("shared"));
            await session.SaveChanges();
        }
        using (var dedicated = this.OpenStore())
        {
            await using var session = dedicated.OpenSession();
            session.Enqueue(new Placed("dedicated"));
            await session.SaveChanges();
        }

        var found = await this.admin.FindOutboxes(this.profileId);
        Assert.Equal(2, found.Count);
        Assert.Contains(found, o => o.Table == DocumentsTable);
        Assert.Contains(found, o => o.Table == OutboxTable);
    }

    [Fact]
    public async Task FindOutboxes_IsEmptyForADatabaseWithNone()
    {
        using (var store = this.OpenStore())
            await store.Collection("Order").Insert(new System.Text.Json.Nodes.JsonObject { ["id"] = "o1" });

        Assert.Empty(await this.admin.FindOutboxes(this.profileId));
    }

    // ── Health ──────────────────────────────────────────────────────────

    [Fact]
    public async Task GetOutboxHealth_SortsEveryMessageIntoTheRightBucket()
    {
        var seeded = await this.SeedAllStates();
        var outbox = await this.Located();

        var health = await this.admin.GetOutboxHealth(this.profileId, outbox);

        Assert.Equal(1, health.Pending);
        // The future AvailableAt counts as Scheduled, not Pending — the whole point of the :date hint.
        Assert.Equal(1, health.Scheduled);
        Assert.Equal(1, health.DeadLettered);
        Assert.Equal(1, health.Processed);
        Assert.Equal(4, health.Total);

        // OldestPendingAt ignores the scheduled, dead and processed rows.
        var pending = seeded.Single(m => m.ProcessedAt == null && m.DeadLetteredAt == null && m.AvailableAt <= DateTimeOffset.UtcNow);
        Assert.NotNull(health.OldestPendingAt);
        Assert.Equal(pending.CreatedAt, health.OldestPendingAt.Value, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task GetOutboxHealth_ReportsNoOldestPendingWhenNothingIsEligible()
    {
        using (var store = this.OpenStore())
        {
            await using var session = store.OpenSession();
            session.Enqueue(new Placed("o1"));
            await session.SaveChanges();
            await new OutboxRunner(store, new Rejecting(_ => true), Options()).DrainOnce();
        }

        var health = await this.admin.GetOutboxHealth(this.profileId, await this.Located());
        Assert.Equal(0, health.Pending);
        Assert.Equal(1, health.Processed);
        Assert.Null(health.OldestPendingAt);
    }

    // ── Grid ────────────────────────────────────────────────────────────

    [Fact]
    public async Task ListOutbox_FlattensTheRowAndDerivesItsState()
    {
        await this.SeedAllStates();
        var outbox = await this.Located();

        var rows = await this.admin.ListOutbox(this.profileId, outbox, new OutboxFilter());

        Assert.Equal(4, rows.Count);
        Assert.Equal(
            [OutboxState.Pending, OutboxState.Scheduled, OutboxState.DeadLettered, OutboxState.Processed],
            rows.Select(r => r.State).Order().Distinct().ToArray());

        var dead = rows.Single(r => r.State == OutboxState.DeadLettered);
        Assert.Contains("consumer refused", dead.Error);
        Assert.Equal(1, dead.Attempts);
        Assert.NotNull(dead.Payload);
        Assert.Equal("dead", dead.Payload!["orderId"]!.GetValue<string>());
    }

    [Fact]
    public async Task ListOutbox_FiltersByStateTypeAndPartition()
    {
        await this.SeedAllStates();
        var outbox = await this.Located();

        var pending = await this.admin.ListOutbox(this.profileId, outbox, new OutboxFilter(State: OutboxState.Pending));
        Assert.Single(pending);

        var scheduled = await this.admin.ListOutbox(this.profileId, outbox, new OutboxFilter(MessageType: "Scheduled.Event"));
        Assert.Single(scheduled);
        Assert.Equal(OutboxState.Scheduled, scheduled[0].State);

        var partitioned = await this.admin.ListOutbox(this.profileId, outbox, new OutboxFilter(PartitionKey: "north"));
        Assert.Single(partitioned);
        Assert.Equal("north", partitioned[0].PartitionKey);

        Assert.Equal(1, await this.admin.CountOutbox(this.profileId, outbox, new OutboxFilter(State: OutboxState.DeadLettered)));
    }

    // ── Failure grouping ────────────────────────────────────────────────

    [Fact]
    public async Task GroupOutboxFailures_CollapsesTheSameFailure()
    {
        using (var store = this.OpenStore())
        {
            await using (var session = store.OpenSession())
            {
                for (var i = 0; i < 5; i++)
                    session.Enqueue(new Placed($"o{i}"));
                await session.SaveChanges();
            }
            await new OutboxRunner(store, new Rejecting(_ => false), Options()).DrainOnce();
        }

        var report = await this.admin.GroupOutboxFailures(this.profileId, await this.Located());

        // Every failure names a different message id, which the summary normalizes away — otherwise this
        // would be five groups of one and the view would be useless.
        var group = Assert.Single(report.Groups);
        Assert.Equal(5, group.Count);
        Assert.Equal(typeof(Placed).FullName, group.MessageType);
        Assert.False(report.Capped);
        Assert.NotEmpty(group.SampleId);
    }

    // ── Writes ──────────────────────────────────────────────────────────

    [Fact]
    public async Task RequeueOutbox_ClearsTheDeadLetterState()
    {
        await this.SeedAllStates();
        var outbox = await this.Located();

        var dead = (await this.admin.ListOutbox(this.profileId, outbox, new OutboxFilter(State: OutboxState.DeadLettered))).Single();
        Assert.Equal(1, await this.admin.RequeueOutbox(this.profileId, outbox, [dead.Id]));

        var health = await this.admin.GetOutboxHealth(this.profileId, outbox);
        Assert.Equal(0, health.DeadLettered);
        Assert.Equal(2, health.Pending);

        var requeued = (await this.admin.ListOutbox(this.profileId, outbox, new OutboxFilter())).Single(r => r.Id == dead.Id);
        Assert.Equal(0, requeued.Attempts);
        Assert.Null(requeued.Error);
    }

    [Fact]
    public async Task RequeueOutbox_IsANoOpOnProcessedAndScheduledMessages()
    {
        await this.SeedAllStates();
        var outbox = await this.Located();

        var rows = await this.admin.ListOutbox(this.profileId, outbox, new OutboxFilter());
        var untouchable = rows
            .Where(r => r.State is OutboxState.Processed or OutboxState.Scheduled or OutboxState.Pending)
            .Select(r => r.Id)
            .ToList();

        Assert.Equal(0, await this.admin.RequeueOutbox(this.profileId, outbox, untouchable));

        var health = await this.admin.GetOutboxHealth(this.profileId, outbox);
        Assert.Equal(1, health.Processed);
        Assert.Equal(1, health.Scheduled);
        Assert.Equal(1, health.DeadLettered);
    }

    [Fact]
    public async Task RequeueAllDeadLettered_CanBeNarrowedToOneMessageType()
    {
        using (var store = this.OpenStore())
        {
            await using (var session = store.OpenSession())
            {
                session.Enqueue(new Placed("a"));
                session.EnqueueRaw("Other.Event", """{"x":1}""");
                await session.SaveChanges();
            }
            await new OutboxRunner(store, new Rejecting(_ => false), Options()).DrainOnce();
        }

        var outbox = await this.Located();
        Assert.Equal(2, (await this.admin.GetOutboxHealth(this.profileId, outbox)).DeadLettered);

        Assert.Equal(1, await this.admin.RequeueAllDeadLettered(this.profileId, outbox, "Other.Event"));
        Assert.Equal(1, (await this.admin.GetOutboxHealth(this.profileId, outbox)).DeadLettered);

        Assert.Equal(1, await this.admin.RequeueAllDeadLettered(this.profileId, outbox));
        Assert.Equal(0, (await this.admin.GetOutboxHealth(this.profileId, outbox)).DeadLettered);
    }

    [Fact]
    public async Task PurgeProcessedOutbox_TouchesNothingElse()
    {
        await this.SeedAllStates();
        var outbox = await this.Located();

        Assert.Equal(0, await this.admin.PurgeProcessedOutbox(this.profileId, outbox, new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero)));
        Assert.Equal(4, (await this.admin.GetOutboxHealth(this.profileId, outbox)).Total);

        Assert.Equal(1, await this.admin.PurgeProcessedOutbox(this.profileId, outbox, DateTimeOffset.UtcNow.AddMinutes(1)));

        var health = await this.admin.GetOutboxHealth(this.profileId, outbox);
        Assert.Equal(0, health.Processed);
        Assert.Equal(1, health.Pending);
        Assert.Equal(1, health.Scheduled);
        Assert.Equal(1, health.DeadLettered);
    }

    [Fact]
    public async Task ReadOnlyProfile_RefusesEveryWrite()
    {
        await this.SeedAllStates();
        var outbox = await this.Located();
        var dead = (await this.admin.ListOutbox(this.profileId, outbox, new OutboxFilter(State: OutboxState.DeadLettered))).Single();

        this.Build(readOnly: true);

        await Assert.ThrowsAsync<InvalidOperationException>(() => this.admin.RequeueOutbox(this.profileId, outbox, [dead.Id]));
        await Assert.ThrowsAsync<InvalidOperationException>(() => this.admin.RequeueAllDeadLettered(this.profileId, outbox));
        await Assert.ThrowsAsync<InvalidOperationException>(() => this.admin.PurgeProcessedOutbox(this.profileId, outbox, DateTimeOffset.UtcNow));

        // Reads still work — a read-only profile gets a read-only screen, not a broken one.
        Assert.Equal(4, (await this.admin.GetOutboxHealth(this.profileId, outbox)).Total);
    }

    // ── The pair that justifies building both halves together ───────────

    /// <summary>
    /// Run the engine until a message dead-letters, requeue it <i>through the admin service</i>, and assert the
    /// engine's own processor then delivers it. This is the only test that proves the two halves agree on the
    /// stored row shape — a drifted wire name would leave the requeue matching nothing and the message dead.
    /// </summary>
    [Fact]
    public async Task DeadLetter_RequeuedByTheAdmin_IsDeliveredByTheEngine()
    {
        using var store = this.OpenStore();

        await using (var session = store.OpenSession())
        {
            session.Enqueue(new Placed("o1"));
            await session.SaveChanges();
        }

        var failing = new Rejecting(_ => false);
        await new OutboxRunner(store, failing, Options()).DrainOnce();

        var outbox = await this.Located();
        var dead = (await this.admin.ListOutbox(this.profileId, outbox, new OutboxFilter(State: OutboxState.DeadLettered))).Single();
        Assert.Equal(1, await this.admin.RequeueOutbox(this.profileId, outbox, [dead.Id]));

        var succeeding = new Rejecting(_ => true);
        Assert.Equal(1, await new OutboxRunner(store, succeeding, Options()).DrainOnce());

        Assert.Equal(dead.Id, Assert.Single(succeeding.Delivered));
        Assert.Equal(1, (await this.admin.GetOutboxHealth(this.profileId, outbox)).Processed);
    }
}
