using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The outbox end to end on every provider that passes the transactional gate. Nothing here is
/// provider-specific — outbox messages are ordinary documents — which is precisely what this suite exists to
/// prove: enqueue, claim (optimistic-concurrency CAS), retry, dead-letter and requeue all lower through the
/// shared write pipeline, so a backend that handles versioned updates differently is caught here.
/// </summary>
public abstract class OutboxConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    public record Placed(string OrderId);

    sealed class Counting : IOutboxDispatcher
    {
        readonly Lock gate = new();
        public List<string> Delivered { get; } = new();
        public bool Fail { get; set; }

        public Task Dispatch(OutboxMessage message, CancellationToken cancellationToken)
        {
            if (this.Fail)
                throw new InvalidOperationException("the bus said no");
            lock (this.gate)
                this.Delivered.Add(message.Id);
            return Task.CompletedTask;
        }
    }

    static OutboxOptions Options() => new()
    {
        MaxAttempts = 2,
        // Zero backoff keeps the retry test from needing a controllable clock on backends this suite reaches
        // through a container.
        Backoff = _ => TimeSpan.Zero
    };

    IDocumentStore CreateStore() => this.Fixture.CreateStore($"t{Guid.NewGuid():N}", o => o.AddOutbox($"ob{Guid.NewGuid():N}"));

    [Fact]
    public void SupportsTransactions_IsTrue()
    {
        using var owner = (IDisposable)this.CreateStore();
        Assert.True(((IDocumentStore)owner).SupportsTransactions);
    }

    [Fact]
    public async Task Enqueue_Dispatch_Acknowledge()
    {
        using var owner = (IDisposable)this.CreateStore();
        var store = (IDocumentStore)owner;

        await using (var session = store.OpenSession())
        {
            session.Enqueue(new Placed("o1"));
            session.Enqueue(new Placed("o2"));
            await session.SaveChanges();
        }

        var dispatcher = new Counting();
        Assert.Equal(2, await new OutboxRunner(store, dispatcher, Options()).DrainOnce());

        Assert.Equal(2, dispatcher.Delivered.Count);
        var messages = await store.Query<OutboxMessage>().ToList();
        Assert.Equal(2, messages.Count);
        Assert.All(messages, m => Assert.NotNull(m.ProcessedAt));
    }

    [Fact]
    public async Task Retry_Then_DeadLetter_Then_Requeue()
    {
        using var owner = (IDisposable)this.CreateStore();
        var store = (IDocumentStore)owner;

        await using (var session = store.OpenSession())
        {
            session.Enqueue(new Placed("o1"));
            await session.SaveChanges();
        }

        var options = Options();
        var dispatcher = new Counting { Fail = true };
        var runner = new OutboxRunner(store, dispatcher, options);

        await runner.DrainOnce();
        var afterFirst = Assert.Single(await store.Query<OutboxMessage>().ToList());
        Assert.Equal(1, afterFirst.Attempts);
        Assert.Null(afterFirst.DeadLetteredAt);

        await runner.DrainOnce();
        var dead = Assert.Single(await store.Query<OutboxMessage>().ToList());
        Assert.Equal(options.MaxAttempts, dead.Attempts);
        Assert.NotNull(dead.DeadLetteredAt);
        Assert.Contains("the bus said no", dead.Error);

        var admin = new OutboxAdmin(store, options, TimeProvider.System);
        Assert.Equal(1, await admin.DeadLetterCount());
        Assert.Equal(1, await admin.Requeue([dead.Id]));

        dispatcher.Fail = false;
        Assert.Equal(1, await runner.DrainOnce());
        Assert.NotNull((await store.Query<OutboxMessage>().ToList()).Single().ProcessedAt);
        Assert.Single(dispatcher.Delivered);
    }

    [Fact]
    public async Task PurgeProcessed_LeavesEverythingElse()
    {
        using var owner = (IDisposable)this.CreateStore();
        var store = (IDocumentStore)owner;

        await using (var session = store.OpenSession())
        {
            session.Enqueue(new Placed("done"));
            session.Enqueue(new Placed("waiting"));
            await session.SaveChanges();
        }

        var options = Options();
        var all = await store.Query<OutboxMessage>().OrderBy(m => m.Id).ToList();
        var runner = new OutboxRunner(store, new PickyDispatcher(all[0].Id), options);
        await runner.DrainOnce();

        var admin = new OutboxAdmin(store, options, TimeProvider.System);
        Assert.Equal(1, await admin.PurgeProcessed(DateTimeOffset.UtcNow.AddMinutes(1)));

        var survivor = Assert.Single(await store.Query<OutboxMessage>().ToList());
        Assert.Equal(all[1].Id, survivor.Id);
    }

    sealed class PickyDispatcher(string deliverableId) : IOutboxDispatcher
    {
        public Task Dispatch(OutboxMessage message, CancellationToken cancellationToken)
            => message.Id == deliverableId ? Task.CompletedTask : throw new InvalidOperationException("not this one");
    }
}

/// <summary>
/// The other half of the gate: a store whose unit of work is compensating rather than transactional must
/// refuse the outbox at startup, by name, instead of shipping a dual write dressed as an outbox.
/// </summary>
public abstract class OutboxUnsupportedTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    [Fact]
    public void SupportsTransactions_IsFalse()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        Assert.False(((IDocumentStore)owner).SupportsTransactions);
    }

    [Fact]
    public async Task AddDocumentOutbox_ThrowsAtStartup()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var store = (IDocumentStore)owner;

        var services = new ServiceCollection();
        services.AddSingleton(store);
        services.AddDocumentOutbox((_, _) => Task.CompletedTask);

        using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().Single(s => s.GetType().Name == "OutboxProcessor");

        var error = await Assert.ThrowsAsync<NotSupportedException>(() => hosted.StartAsync(CancellationToken.None));
        Assert.Contains(store.GetType().Name, error.Message);
    }
}
