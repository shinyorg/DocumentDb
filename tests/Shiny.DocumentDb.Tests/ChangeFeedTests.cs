using System.Collections.Concurrent;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Integration tests for native, external-writer-aware change feeds. These require a live server
/// (Testcontainers/Docker) and are wired for providers whose <see cref="IDatabaseProvider"/>
/// supports change feeds (PostgreSQL, SQL Server).
/// </summary>
public abstract class ChangeFeedTestsBase : IDisposable
{
    readonly IDocumentStoreFixture fixture;
    readonly string table;
    protected readonly IDocumentStore store;

    protected ChangeFeedTestsBase(IDocumentStoreFixture fixture)
    {
        this.fixture = fixture;
        this.table = $"cf{Guid.NewGuid():N}";
        this.store = fixture.CreateStore(this.table);
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    static async Task WaitForAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;
            await Task.Delay(100);
        }
    }

    [Fact]
    public async Task ObservesInsertUpdateRemove()
    {
        var changes = new ConcurrentQueue<DocumentChange<User>>();
        await using var sub = await this.store.SubscribeChanges<User>((c, _) =>
        {
            changes.Enqueue(c);
            return Task.CompletedTask;
        });

        // Allow the background listener to attach before writing.
        await Task.Delay(1000);

        await this.store.Insert(new User { Id = "u1", Name = "Allan", Age = 30 });
        await this.store.Update(new User { Id = "u1", Name = "Allan2", Age = 31 });
        await this.store.Remove<User>("u1");

        await WaitForAsync(() => changes.Count >= 3, TimeSpan.FromSeconds(15));

        var observed = changes.ToArray();
        Assert.Contains(observed, c => c.ChangeType == DocumentChangeType.Inserted && c.Id == "u1");
        Assert.Contains(observed, c => c.ChangeType == DocumentChangeType.Updated && c.Id == "u1");
        Assert.Contains(observed, c => c.ChangeType == DocumentChangeType.Removed && c.Id == "u1");
    }

    [Fact]
    public async Task ObservesExternalWriter()
    {
        var changes = new ConcurrentQueue<DocumentChange<User>>();
        await using var sub = await this.store.SubscribeChanges<User>((c, _) =>
        {
            changes.Enqueue(c);
            return Task.CompletedTask;
        });

        await Task.Delay(1000);

        // A separate store instance (separate connection) writing the same table.
        var external = this.fixture.CreateStore(this.table);
        try
        {
            await external.Insert(new User { Id = "ext1", Name = "External", Age = 1 });
        }
        finally
        {
            (external as IDisposable)?.Dispose();
        }

        await WaitForAsync(() => changes.Any(c => c.Id == "ext1"), TimeSpan.FromSeconds(15));

        Assert.Contains(changes.ToArray(), c => c.ChangeType == DocumentChangeType.Inserted && c.Id == "ext1");
    }

    [Fact]
    public async Task Disposing_StopsDelivery()
    {
        var changes = new ConcurrentQueue<DocumentChange<User>>();
        var sub = await this.store.SubscribeChanges<User>((c, _) =>
        {
            changes.Enqueue(c);
            return Task.CompletedTask;
        });
        await Task.Delay(1000);

        await this.store.Insert(new User { Id = "u1", Name = "A" });
        await WaitForAsync(() => changes.Any(), TimeSpan.FromSeconds(15));
        await sub.DisposeAsync();

        var countAfterDispose = changes.Count;
        await this.store.Insert(new User { Id = "u2", Name = "B" });
        await Task.Delay(2000);

        Assert.Equal(countAfterDispose, changes.Count);
    }
}
