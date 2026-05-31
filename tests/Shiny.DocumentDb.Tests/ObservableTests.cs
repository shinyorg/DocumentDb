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
    public async Task Insert_RaisesInserted()
    {
        var changes = new List<DocumentChange<User>>();
        using var _ = this.store.WhenChanged<User>().Subscribe(changes.Add);

        await this.store.Insert(new User { Id = "u1", Name = "Allan" });

        var change = Assert.Single(changes);
        Assert.Equal(DocumentChangeType.Inserted, change.ChangeType);
        Assert.Equal("u1", change.Id);
        Assert.NotNull(change.Document);
        Assert.Equal("Allan", change.Document!.Name);
    }

    [Fact]
    public async Task Insert_WithGeneratedId_ReportsGeneratedId()
    {
        var changes = new List<DocumentChange<GuidIdModel>>();
        using var _ = this.store.WhenChanged<GuidIdModel>().Subscribe(changes.Add);

        var model = new GuidIdModel { Name = "x" };
        await this.store.Insert(model);

        var change = Assert.Single(changes);
        Assert.Equal(DocumentChangeType.Inserted, change.ChangeType);
        Assert.Equal(model.Id.ToString("N"), change.Id);
    }

    [Fact]
    public async Task Update_RaisesUpdated()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Allan" });

        var changes = new List<DocumentChange<User>>();
        using var _ = this.store.WhenChanged<User>().Subscribe(changes.Add);

        await this.store.Update(new User { Id = "u1", Name = "Updated" });

        var change = Assert.Single(changes);
        Assert.Equal(DocumentChangeType.Updated, change.ChangeType);
        Assert.Equal("u1", change.Id);
        Assert.Equal("Updated", change.Document!.Name);
    }

    [Fact]
    public async Task Remove_RaisesRemoved()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Allan" });

        var changes = new List<DocumentChange<User>>();
        using var _ = this.store.WhenChanged<User>().Subscribe(changes.Add);

        await this.store.Remove<User>("u1");

        var change = Assert.Single(changes);
        Assert.Equal(DocumentChangeType.Removed, change.ChangeType);
        Assert.Equal("u1", change.Id);
        Assert.Null(change.Document);
    }

    [Fact]
    public async Task Remove_Missing_RaisesNothing()
    {
        var changes = new List<DocumentChange<User>>();
        using var _ = this.store.WhenChanged<User>().Subscribe(changes.Add);

        await this.store.Remove<User>("nope");

        Assert.Empty(changes);
    }

    [Fact]
    public async Task SetProperty_RaisesUpdated()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Allan", Age = 1 });

        var changes = new List<DocumentChange<User>>();
        using var _ = this.store.WhenChanged<User>().Subscribe(changes.Add);

        await this.store.SetProperty<User>("u1", u => u.Age, 42);

        var change = Assert.Single(changes);
        Assert.Equal(DocumentChangeType.Updated, change.ChangeType);
        Assert.Equal("u1", change.Id);
        Assert.Null(change.Document);
    }

    [Fact]
    public async Task Clear_RaisesCleared()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Allan" });
        await this.store.Insert(new User { Id = "u2", Name = "Bob" });

        var changes = new List<DocumentChange<User>>();
        using var _ = this.store.WhenChanged<User>().Subscribe(changes.Add);

        await this.store.Clear<User>();

        var change = Assert.Single(changes);
        Assert.Equal(DocumentChangeType.Cleared, change.ChangeType);
    }

    [Fact]
    public async Task BatchInsert_RaisesPerDocument()
    {
        var changes = new List<DocumentChange<User>>();
        using var _ = this.store.WhenChanged<User>().Subscribe(changes.Add);

        await this.store.BatchInsert(new[]
        {
            new User { Id = "u1", Name = "A" },
            new User { Id = "u2", Name = "B" },
            new User { Id = "u3", Name = "C" }
        });

        Assert.Equal(3, changes.Count);
        Assert.All(changes, c => Assert.Equal(DocumentChangeType.Inserted, c.ChangeType));
        Assert.Equal(new[] { "u1", "u2", "u3" }, changes.Select(c => c.Id).ToArray());
    }

    [Fact]
    public async Task Subscriber_OnlyReceivesChangesAfterSubscribing()
    {
        await this.store.Insert(new User { Id = "before", Name = "Early" });

        var changes = new List<DocumentChange<User>>();
        using var _ = this.store.WhenChanged<User>().Subscribe(changes.Add);

        await this.store.Insert(new User { Id = "after", Name = "Late" });

        var change = Assert.Single(changes);
        Assert.Equal("after", change.Id);
    }

    [Fact]
    public async Task Unsubscribe_StopsDelivery()
    {
        var changes = new List<DocumentChange<User>>();
        var subscription = this.store.WhenChanged<User>().Subscribe(changes.Add);

        await this.store.Insert(new User { Id = "u1", Name = "A" });
        subscription.Dispose();
        await this.store.Insert(new User { Id = "u2", Name = "B" });

        var change = Assert.Single(changes);
        Assert.Equal("u1", change.Id);
    }

    [Fact]
    public async Task WhenDocumentChanged_FiltersById()
    {
        var observable = (IObservableDocumentStore)this.store;
        var changes = new List<DocumentChange<User>>();
        using var _ = observable.WhenDocumentChanged<User>("u1").Subscribe(changes.Add);

        await this.store.Insert(new User { Id = "u1", Name = "A" });
        await this.store.Insert(new User { Id = "u2", Name = "B" });
        await this.store.Update(new User { Id = "u1", Name = "A2" });

        Assert.Equal(2, changes.Count);
        Assert.All(changes, c => Assert.Equal("u1", c.Id));
    }

    [Fact]
    public async Task ChangesOfOtherTypes_AreNotDelivered()
    {
        var userChanges = new List<DocumentChange<User>>();
        using var _ = this.store.WhenChanged<User>().Subscribe(userChanges.Add);

        await this.store.Insert(new Product { Id = "p1", Title = "Widget" });

        Assert.Empty(userChanges);
    }

    [Fact]
    public async Task Transaction_EmitsOnlyAfterCommit()
    {
        var changes = new List<DocumentChange<User>>();
        using var _ = this.store.WhenChanged<User>().Subscribe(changes.Add);

        await this.store.RunInTransaction(async tx =>
        {
            await tx.Insert(new User { Id = "u1", Name = "A" });
            await tx.Insert(new User { Id = "u2", Name = "B" });
            // Nothing should have been delivered yet — still inside the transaction.
            Assert.Empty(changes);
        });

        Assert.Equal(2, changes.Count);
        Assert.Equal(new[] { "u1", "u2" }, changes.Select(c => c.Id).ToArray());
    }

    [Fact]
    public async Task Transaction_Rollback_EmitsNothing()
    {
        var changes = new List<DocumentChange<User>>();
        using var _ = this.store.WhenChanged<User>().Subscribe(changes.Add);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await this.store.RunInTransaction(async tx =>
            {
                await tx.Insert(new User { Id = "u1", Name = "A" });
                throw new InvalidOperationException("boom");
            });
        });

        Assert.Empty(changes);
    }
}
