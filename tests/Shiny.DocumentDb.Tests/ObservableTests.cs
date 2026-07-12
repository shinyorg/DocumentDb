using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class ObservableTestsBase : IDisposable
{
    protected readonly IDocumentStore store;

    protected ObservableTestsBase(IDocumentStoreFixture fixture)
        => this.store = fixture.CreateStore($"t{Guid.NewGuid():N}");

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public void Store_ImplementsObservable()
        => Assert.IsAssignableFrom<IObservableDocumentStore>(this.store);

    [Fact]
    public async Task SubscribeChanges_NotSupported_Throws()
    {
        // Only stores whose provider lacks a native change feed (SQLite, LiteDB, MySQL, DuckDB)
        // should reject SubscribeChanges. PostgreSQL / SQL Server / Cosmos implement it natively
        // and the call succeeds — covered by ChangeFeedTestsBase.
        if (this.store is DocumentStore ds && ds.DatabaseProvider.SupportsChangeFeed)
            return;
        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await this.store.SubscribeChanges<User>((_, _) => Task.CompletedTask));
    }

    [Fact]
    public async Task Insert_RaisesInserted()
    {
        using var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        await this.store.Insert(new User { Id = "u1", Name = "Allan" });
        await c.WaitForCountAsync(1);

        var change = Assert.Single(c.Snapshot);
        Assert.Equal(DocumentChangeType.Inserted, change.ChangeType);
        Assert.Equal("u1", change.Id);
        Assert.NotNull(change.Document);
        Assert.Equal("Allan", change.Document!.Name);
    }

    [Fact]
    public async Task Insert_WithGeneratedId_ReportsGeneratedId()
    {
        using var c = new ChangeCollector<GuidIdModel>(this.store.NotifyOnChange<GuidIdModel>());
        await c.SettleAsync();

        var model = new GuidIdModel { Name = "x" };
        await this.store.Insert(model);
        await c.WaitForCountAsync(1);

        var change = Assert.Single(c.Snapshot);
        Assert.Equal(DocumentChangeType.Inserted, change.ChangeType);
        Assert.Equal(model.Id.ToString("N"), change.Id);
    }

    [Fact]
    public async Task Update_RaisesUpdated()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Allan" });

        using var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        await this.store.Update(new User { Id = "u1", Name = "Updated" });
        await c.WaitForCountAsync(1);

        var change = Assert.Single(c.Snapshot);
        Assert.Equal(DocumentChangeType.Updated, change.ChangeType);
        Assert.Equal("u1", change.Id);
        Assert.Equal("Updated", change.Document!.Name);
    }

    [Fact]
    public async Task Remove_RaisesRemoved()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Allan" });

        using var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        await this.store.Remove<User>("u1");
        await c.WaitForCountAsync(1);

        var change = Assert.Single(c.Snapshot);
        Assert.Equal(DocumentChangeType.Removed, change.ChangeType);
        Assert.Equal("u1", change.Id);
        Assert.Null(change.Document);
    }

    [Fact]
    public async Task Remove_Missing_RaisesNothing()
    {
        using var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        await this.store.Remove<User>("nope");
        await c.SettleAsync();

        Assert.Empty(c.Snapshot);
    }

    [Fact]
    public async Task SetProperty_RaisesUpdated()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Allan", Age = 1 });

        using var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        await this.store.SetProperty<User>("u1", u => u.Age, 42);
        await c.WaitForCountAsync(1);

        var change = Assert.Single(c.Snapshot);
        Assert.Equal(DocumentChangeType.Updated, change.ChangeType);
        Assert.Equal("u1", change.Id);
        Assert.Null(change.Document);
    }

    [Fact]
    public async Task Clear_RaisesCleared()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Allan" });
        await this.store.Insert(new User { Id = "u2", Name = "Bob" });

        using var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        await this.store.Clear<User>();
        await c.WaitForCountAsync(1);

        var change = Assert.Single(c.Snapshot);
        Assert.Equal(DocumentChangeType.Cleared, change.ChangeType);
    }

    [Fact]
    public async Task BatchInsert_RaisesPerDocument()
    {
        using var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        await this.store.BatchInsert(new[]
        {
            new User { Id = "u1", Name = "A" },
            new User { Id = "u2", Name = "B" },
            new User { Id = "u3", Name = "C" }
        });
        await c.WaitForCountAsync(3);

        Assert.Equal(3, c.Snapshot.Count);
        Assert.All(c.Snapshot, x => Assert.Equal(DocumentChangeType.Inserted, x.ChangeType));
        Assert.Equal(new[] { "u1", "u2", "u3" }, c.Snapshot.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task Subscriber_OnlyReceivesChangesAfterSubscribing()
    {
        await this.store.Insert(new User { Id = "before", Name = "Early" });

        using var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        await this.store.Insert(new User { Id = "after", Name = "Late" });
        await c.WaitForCountAsync(1);

        var change = Assert.Single(c.Snapshot);
        Assert.Equal("after", change.Id);
    }

    [Fact]
    public async Task Unsubscribe_StopsDelivery()
    {
        var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        await this.store.Insert(new User { Id = "u1", Name = "A" });
        await c.WaitForCountAsync(1);

        c.Dispose(); // cancel the async iteration

        await this.store.Insert(new User { Id = "u2", Name = "B" });
        await Task.Delay(100);

        var change = Assert.Single(c.Snapshot);
        Assert.Equal("u1", change.Id);
    }

    [Fact]
    public async Task WhenDocumentChanged_FiltersById()
    {
        var observable = (IObservableDocumentStore)this.store;
        using var c = new ChangeCollector<User>(observable.WhenDocumentChanged<User>("u1"));
        await c.SettleAsync();

        await this.store.Insert(new User { Id = "u1", Name = "A" });
        await this.store.Insert(new User { Id = "u2", Name = "B" });
        await this.store.Update(new User { Id = "u1", Name = "A2" });

        await c.WaitForCountAsync(2);
        await c.SettleAsync();

        Assert.Equal(2, c.Snapshot.Count);
        Assert.All(c.Snapshot, x => Assert.Equal("u1", x.Id));
    }

    [Fact]
    public async Task ChangesOfOtherTypes_AreNotDelivered()
    {
        using var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        await this.store.Insert(new Product { Id = "p1", Title = "Widget" });
        await c.SettleAsync();

        Assert.Empty(c.Snapshot);
    }

    [Fact]
    public async Task UnitOfWork_EmitsOnlyAfterCommit()
    {
        using var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        var uow = this.store.OpenSession()
            .Add(new User { Id = "u1", Name = "A" })
            .Add(new User { Id = "u2", Name = "B" });

        // Nothing should have been delivered yet — the unit hasn't committed.
        Assert.Empty(c.Snapshot);

        await uow.SaveChanges();

        await c.WaitForCountAsync(2);
        Assert.Equal(2, c.Snapshot.Count);
        Assert.Equal(new[] { "u1", "u2" }, c.Snapshot.Select(x => x.Id).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task UnitOfWork_Rollback_EmitsNothing()
    {
        // A row that collides with the unit's insert so SaveChanges fails and rolls back.
        await this.store.Insert(new User { Id = "u1", Name = "Existing" });

        using var c = new ChangeCollector<User>(this.store.NotifyOnChange<User>());
        await c.SettleAsync();

        var uow = this.store.OpenSession()
            .Add(new User { Id = "u2", Name = "A" })
            .Add(new User { Id = "u1", Name = "Conflict" });

        await Assert.ThrowsAsync<InvalidOperationException>(() => uow.SaveChanges());

        await c.SettleAsync();
        Assert.Empty(c.Snapshot);
    }

    // ── Per-query change monitoring ──────────────────────────────────────

    [Fact]
    public async Task Query_NotifyOnChange_FiltersByPredicate()
    {
        var query = this.store.Query<User>().Where(u => u.Age >= 30);
        using var c = new ChangeCollector<User>(query.NotifyOnChange());
        await c.SettleAsync();

        await this.store.Insert(new User { Id = "u1", Name = "Young",  Age = 20 }); // skipped
        await this.store.Insert(new User { Id = "u2", Name = "Older",  Age = 40 }); // matches
        await this.store.Insert(new User { Id = "u3", Name = "Older2", Age = 31 }); // matches

        await c.WaitForCountAsync(2);
        await c.SettleAsync();

        Assert.Equal(2, c.Snapshot.Count);
        Assert.Equal(new[] { "u2", "u3" }, c.Snapshot.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task Query_NotifyOnChange_PassesThroughRemovals()
    {
        // Removed/Cleared/SetProperty paths have no document body to test against the predicate,
        // so we pass them through so the consumer can re-query if it needs to test membership.
        await this.store.Insert(new User { Id = "u1", Name = "x", Age = 20 });

        var query = this.store.Query<User>().Where(u => u.Age >= 30);
        using var c = new ChangeCollector<User>(query.NotifyOnChange());
        await c.SettleAsync();

        await this.store.Remove<User>("u1");
        await c.WaitForCountAsync(1);

        var change = Assert.Single(c.Snapshot);
        Assert.Equal(DocumentChangeType.Removed, change.ChangeType);
        Assert.Equal("u1", change.Id);
    }

    [Fact]
    public async Task Query_NotifyOnChange_NoPredicate_DeliversAll()
    {
        var query = this.store.Query<User>();
        using var c = new ChangeCollector<User>(query.NotifyOnChange());
        await c.SettleAsync();

        await this.store.Insert(new User { Id = "u1", Name = "A", Age = 1 });
        await this.store.Insert(new User { Id = "u2", Name = "B", Age = 99 });

        await c.WaitForCountAsync(2);
        Assert.Equal(2, c.Snapshot.Count);
    }

    [Fact]
    public async Task Query_NotifyOnChange_CombinesMultipleWheres()
    {
        var query = this.store.Query<User>()
            .Where(u => u.Age >= 30)
            .Where(u => u.Name.StartsWith("A"));
        using var c = new ChangeCollector<User>(query.NotifyOnChange());
        await c.SettleAsync();

        await this.store.Insert(new User { Id = "u1", Name = "Alice",  Age = 40 }); // matches both
        await this.store.Insert(new User { Id = "u2", Name = "Bob",    Age = 40 }); // wrong name
        await this.store.Insert(new User { Id = "u3", Name = "Andrew", Age = 20 }); // wrong age

        await c.WaitForCountAsync(1);
        await c.SettleAsync();

        var change = Assert.Single(c.Snapshot);
        Assert.Equal("u1", change.Id);
    }

    [Fact]
    public void Query_NotifyOnChange_AfterSelect_Throws()
    {
        var projected = this.store.Query<User>().Select(u => new ProjectedName { Name = u.Name ?? "" });
        Assert.Throws<InvalidOperationException>(() => projected.NotifyOnChange());
    }
}

// Small DTO for the projected-query throw test.
public class ProjectedName
{
    public string Name { get; set; } = "";
}

/// <summary>
/// Collects items from an <see cref="IAsyncEnumerable{T}"/> on a background task so test bodies
/// can use a synchronous-style "do mutation, assert snapshot" pattern.
/// </summary>
sealed class ChangeCollector<T> : IDisposable where T : class
{
    readonly CancellationTokenSource cts = new();
    readonly List<DocumentChange<T>> items = new();
    readonly object gate = new();
    readonly Task readTask;
    bool disposed;

    public ChangeCollector(IAsyncEnumerable<DocumentChange<T>> source)
    {
        this.readTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var change in source.WithCancellation(this.cts.Token).ConfigureAwait(false))
                {
                    lock (this.gate)
                        this.items.Add(change);
                }
            }
            catch (OperationCanceledException) { /* expected on disposal */ }
        });
    }

    public IReadOnlyList<DocumentChange<T>> Snapshot
    {
        get { lock (this.gate) return this.items.ToArray(); }
    }

    public int Count
    {
        get { lock (this.gate) return this.items.Count; }
    }

    /// <summary>Waits for at least <paramref name="min"/> items, then returns. Throws on timeout.</summary>
    public async Task WaitForCountAsync(int min, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(2));
        while (this.Count < min && DateTime.UtcNow < deadline)
            await Task.Delay(10).ConfigureAwait(false);
        if (this.Count < min)
            throw new TimeoutException($"Expected at least {min} change(s); got {this.Count} within timeout.");
    }

    /// <summary>Waits a short interval to let the background reader register or quiesce.</summary>
    public Task SettleAsync(int milliseconds = 100) => Task.Delay(milliseconds);

    public void Dispose()
    {
        if (this.disposed) return;
        this.disposed = true;
        this.cts.Cancel();
        try { this.readTask.GetAwaiter().GetResult(); }
        catch (OperationCanceledException) { }
        catch (AggregateException ae) when (ae.InnerExceptions.All(e => e is OperationCanceledException)) { }
        this.cts.Dispose();
    }
}
