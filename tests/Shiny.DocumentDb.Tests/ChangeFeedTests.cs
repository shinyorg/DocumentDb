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

        // Use distinct ids per change so providers with net-change-per-row semantics (SqlServer
        // Change Tracking) report each event. PostgreSQL LISTEN/NOTIFY would see all three on the
        // same id, but Change Tracking collapses rapid I+U+D on the same row into one D between
        // polls. Distinct ids exercise the same insert/update/delete code paths without that
        // collapse.
        await this.store.Insert(new User { Id = "ins", Name = "Inserted", Age = 30 });
        await this.store.Insert(new User { Id = "upd", Name = "ToUpdate", Age = 20 });
        await this.store.Insert(new User { Id = "rem", Name = "ToRemove", Age = 40 });

        // SqlServer Change Tracking reports the net change since the baseline version.
        // If Insert and Update of the same row happen before the next poll, the event collapses to
        // "Inserted". Wait long enough to ensure at least one poll cycle (default 2s) advances the
        // baseline so the subsequent Update is observed as "Updated".
        await Task.Delay(3000);

        await this.store.Update(new User { Id = "upd", Name = "WasUpdated", Age = 21 });
        await this.store.Remove<User>("rem");

        await WaitForAsync(() =>
            changes.Any(c => c.ChangeType == DocumentChangeType.Inserted && c.Id == "ins") &&
            changes.Any(c => c.ChangeType == DocumentChangeType.Updated && c.Id == "upd") &&
            changes.Any(c => c.ChangeType == DocumentChangeType.Removed && c.Id == "rem"),
            TimeSpan.FromSeconds(15));

        var observed = changes.ToArray();
        Assert.Contains(observed, c => c.ChangeType == DocumentChangeType.Inserted && c.Id == "ins");
        Assert.Contains(observed, c => c.ChangeType == DocumentChangeType.Updated && c.Id == "upd");
        Assert.Contains(observed, c => c.ChangeType == DocumentChangeType.Removed && c.Id == "rem");
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
