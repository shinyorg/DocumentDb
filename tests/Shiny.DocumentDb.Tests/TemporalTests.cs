using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Exercises system-time temporal history (<c>MapTemporal</c>) across every relational provider:
/// each mutation records a version in the <c>{table}_history</c> sidecar, and the state can be read
/// back via History / AsOf / Restore / GetDiffBetween, with retention pruning and actor capture.
/// </summary>
public abstract class TemporalTestsBase(ITemporalDocumentStoreFixture fixture)
{
    protected readonly ITemporalDocumentStoreFixture Fixture = fixture;

    ITemporalDocumentStore CreateStore(Action<TemporalOptions>? configure = null, Func<string>? actor = null)
        => this.Fixture.CreateTemporalStore($"t{Guid.NewGuid():N}", configure, actor);

    [Fact]
    public async Task Insert_Then_Update_RecordsTwoVersions()
    {
        using var store = CreateStore();
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        await store.Update(new VersionedUser { Id = "u1", Name = "Alice", Age = 31 });

        var history = await store.History<VersionedUser>("u1");

        Assert.Equal(2, history.Count);
        Assert.Equal(1, history[0].Version);
        Assert.Equal(TemporalOperation.Inserted, history[0].Operation);
        Assert.Equal(30, history[0].Document!.Age);
        Assert.NotNull(history[0].ValidTo); // closed by the update

        Assert.Equal(2, history[1].Version);
        Assert.Equal(TemporalOperation.Updated, history[1].Operation);
        Assert.Equal(31, history[1].Document!.Age);
        Assert.Null(history[1].ValidTo); // current version is open
    }

    [Fact]
    public async Task AsOf_ReturnsStateAtPointInTime()
    {
        using var store = CreateStore();
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });

        await Task.Delay(20);
        var mid = DateTimeOffset.UtcNow;
        await Task.Delay(20);

        await store.Update(new VersionedUser { Id = "u1", Name = "Alice", Age = 31 });

        var atMid = await store.AsOf<VersionedUser>("u1", mid);
        Assert.NotNull(atMid);
        Assert.Equal(30, atMid!.Age);

        var now = await store.AsOf<VersionedUser>("u1", DateTimeOffset.UtcNow);
        Assert.Equal(31, now!.Age);

        var before = await store.AsOf<VersionedUser>("u1", DateTimeOffset.UtcNow.AddYears(-1));
        Assert.Null(before);
    }

    [Fact]
    public async Task Remove_WritesTombstone_AsOfAfterReturnsNull()
    {
        using var store = CreateStore();
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        await Task.Delay(10);
        await store.Remove<VersionedUser>("u1");

        var history = await store.History<VersionedUser>("u1");
        Assert.Equal(2, history.Count);
        Assert.Equal(TemporalOperation.Removed, history[1].Operation);
        Assert.Null(history[1].Document);

        var now = await store.AsOf<VersionedUser>("u1", DateTimeOffset.UtcNow);
        Assert.Null(now);
    }

    [Fact]
    public async Task SetProperty_RecordsVersionWithPostImage()
    {
        using var store = CreateStore();
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        await store.SetProperty<VersionedUser>("u1", u => u.Age, 42);

        var history = await store.History<VersionedUser>("u1");
        Assert.Equal(2, history.Count);
        Assert.Equal(TemporalOperation.Updated, history[1].Operation);
        Assert.Equal(42, history[1].Document!.Age);
        Assert.Equal("Alice", history[1].Document!.Name); // full post-image, not just the changed field
    }

    [Fact]
    public async Task Upsert_RecordsPostMergeImage()
    {
        using var store = CreateStore();
        await store.Insert(new MergeDoc { Id = "u1", Name = "Alice", Address = new MergeNested { City = "NYC" } });
        await store.Upsert(new MergeDoc { Id = "u1", Name = "Bob" }); // merge: Address preserved

        var history = await store.History<MergeDoc>("u1");
        Assert.Equal(2, history.Count);
        // History stores the post-merge image, not the partial patch.
        Assert.Equal("Bob", history[1].Document!.Name);
        Assert.Equal("NYC", history[1].Document!.Address!.City);
    }

    [Fact]
    public async Task Restore_ReinstatesPriorVersion()
    {
        using var store = CreateStore();
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        await store.Update(new VersionedUser { Id = "u1", Name = "Alice", Age = 99 });

        var restored = await store.Restore<VersionedUser>("u1", 1);
        Assert.NotNull(restored);
        Assert.Equal(30, restored!.Age);

        var current = await store.Get<VersionedUser>("u1");
        Assert.Equal(30, current!.Age);

        // Restore writes a new version on top rather than rewriting history.
        var history = await store.History<VersionedUser>("u1");
        Assert.Equal(3, history.Count);
        Assert.Equal(30, history[2].Document!.Age);
    }

    [Fact]
    public async Task Restore_AfterRemove_ReinsertsDocument()
    {
        using var store = CreateStore();
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        await store.Remove<VersionedUser>("u1");

        var restored = await store.Restore<VersionedUser>("u1", 1);
        Assert.NotNull(restored);

        var current = await store.Get<VersionedUser>("u1");
        Assert.NotNull(current);
        Assert.Equal(30, current!.Age);
    }

    [Fact]
    public async Task GetDiffBetween_ReturnsPatch()
    {
        using var store = CreateStore();
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        await store.Update(new VersionedUser { Id = "u1", Name = "Alicia", Age = 31 });

        var patch = await store.GetDiffBetween<VersionedUser>("u1", 1, 2);
        Assert.NotNull(patch);

        // Applying the patch to the v1 doc should yield the v2 doc.
        var rebuilt = patch!.ApplyTo(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        Assert.Equal("Alicia", rebuilt.Name);
        Assert.Equal(31, rebuilt.Age);
    }

    [Fact]
    public async Task GetDiffBetween_ReturnsNull_WhenVersionMissing()
    {
        using var store = CreateStore();
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });

        var patch = await store.GetDiffBetween<VersionedUser>("u1", 1, 99);
        Assert.Null(patch);
    }

    [Fact]
    public async Task MaxVersions_PrunesOldest()
    {
        using var store = CreateStore(o => o.MaxVersions = 3);
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 1 });
        for (var age = 2; age <= 6; age++)
            await store.Update(new VersionedUser { Id = "u1", Name = "Alice", Age = age });

        var history = await store.History<VersionedUser>("u1");
        Assert.Equal(3, history.Count);
        // Newest three versions retained (ages 4, 5, 6).
        Assert.Equal(4, history[0].Document!.Age);
        Assert.Equal(6, history[2].Document!.Age);
        Assert.Null(history[2].ValidTo);
    }

    [Fact]
    public async Task CaptureActor_IsRecorded()
    {
        using var store = CreateStore(actor: () => "tester@example.com");
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });

        var history = await store.History<VersionedUser>("u1");
        Assert.Equal("tester@example.com", history[0].Actor);
    }

    [Fact]
    public async Task History_ForNonMappedType_Throws()
    {
        using var store = CreateStore();
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await store.History<MergeNested>("x"));
    }

    [Fact]
    public async Task BatchInsert_RecordsHistoryPerDocument()
    {
        using var store = CreateStore();
        await store.BatchInsert(new[]
        {
            new VersionedUser { Id = "u1", Name = "Alice", Age = 30 },
            new VersionedUser { Id = "u2", Name = "Bob", Age = 40 }
        });

        var h1 = await store.History<VersionedUser>("u1");
        var h2 = await store.History<VersionedUser>("u2");
        Assert.Single(h1);
        Assert.Single(h2);
        Assert.Equal(TemporalOperation.Inserted, h1[0].Operation);
        Assert.Equal(30, h1[0].Document!.Age);
        Assert.Equal(40, h2[0].Document!.Age);
    }

    [Fact]
    public async Task AsOfAll_ReturnsFleetSnapshot()
    {
        using var store = CreateStore();
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        await store.Insert(new VersionedUser { Id = "u2", Name = "Bob", Age = 40 });

        await Task.Delay(20);
        var mid = DateTimeOffset.UtcNow;
        await Task.Delay(20);

        await store.Update(new VersionedUser { Id = "u1", Name = "Alice", Age = 31 });
        await store.Insert(new VersionedUser { Id = "u3", Name = "Carol", Age = 50 });
        await store.Remove<VersionedUser>("u2");

        // As of `mid`: u1 age 30, u2 present, u3 not yet created.
        var snapshot = (await store.AsOfAll<VersionedUser>(mid)).OrderBy(u => u.Id).ToList();
        Assert.Equal(2, snapshot.Count);
        Assert.Equal("u1", snapshot[0].Id);
        Assert.Equal(30, snapshot[0].Age);
        Assert.Equal("u2", snapshot[1].Id);

        // As of now: u1 age 31, u2 removed, u3 present.
        var now = (await store.AsOfAll<VersionedUser>(DateTimeOffset.UtcNow)).OrderBy(u => u.Id).ToList();
        Assert.Equal(2, now.Count);
        Assert.Equal("u1", now[0].Id);
        Assert.Equal(31, now[0].Age);
        Assert.Equal("u3", now[1].Id);
    }

    [Fact]
    public async Task ChangesByActor_ReturnsOnlyThatActorsVersions()
    {
        var current = "alice";
        using var store = CreateStore(actor: () => current);
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });
        current = "bob";
        await store.Update(new VersionedUser { Id = "u1", Name = "Alice", Age = 31 });
        await store.Insert(new VersionedUser { Id = "u2", Name = "Bob", Age = 40 });

        var byAlice = await store.ChangesByActor<VersionedUser>("alice");
        Assert.Single(byAlice);
        Assert.Equal("u1", byAlice[0].Id);
        Assert.Equal(30, byAlice[0].Document!.Age);

        var byBob = await store.ChangesByActor<VersionedUser>("bob");
        Assert.Equal(2, byBob.Count);
        Assert.All(byBob, v => Assert.Equal("bob", v.Actor));
    }

    [Fact]
    public async Task ChangesBetween_ReturnsVersionsInWindow()
    {
        using var store = CreateStore();
        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });

        await Task.Delay(20);
        var from = DateTimeOffset.UtcNow;
        await store.Update(new VersionedUser { Id = "u1", Name = "Alice", Age = 31 });
        await store.Insert(new VersionedUser { Id = "u2", Name = "Bob", Age = 40 });
        var to = DateTimeOffset.UtcNow.AddMilliseconds(1);
        await Task.Delay(20);
        await store.Update(new VersionedUser { Id = "u2", Name = "Bob", Age = 41 });

        var window = await store.ChangesBetween<VersionedUser>(from, to);
        // The two writes inside [from, to): u1's update and u2's insert. The initial insert (before
        // `from`) and u2's later update (after `to`) are excluded.
        Assert.Equal(2, window.Count);
        Assert.All(window, v => Assert.InRange(v.ValidFrom, from, to));
    }

    [Fact]
    public async Task UnitOfWork_RecordsHistory()
    {
        using var store = CreateStore();
        await store.CreateUnitOfWork()
            .Add(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 })
            .Update(new VersionedUser { Id = "u1", Name = "Alice", Age = 31 })
            .SaveChanges();

        var history = await store.History<VersionedUser>("u1");
        Assert.Equal(2, history.Count);
        Assert.Equal(31, history[1].Document!.Age);
    }
}
