using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Exercises optimistic concurrency via <c>MapVersionProperty</c>: Insert seeds version 1,
/// Update bumps it and validates the expected version on write, and a stale Update throws
/// <see cref="ConcurrencyException"/>.
/// </summary>
public abstract class VersionMappingTestsBase
{
    protected readonly IDocumentStoreFixture Fixture;

    protected VersionMappingTestsBase(IDocumentStoreFixture fixture)
        => this.Fixture = fixture;

    [Fact]
    public async Task Insert_SeedsVersionToOne()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithVersion<VersionedUser>(
            $"t{Guid.NewGuid():N}", u => u.Version);
        var s = (IDocumentStore)store;

        var doc = new VersionedUser { Id = "u1", Name = "Alice", Age = 30 };
        await s.Insert(doc);

        Assert.Equal(1, doc.Version);

        var got = await s.Get<VersionedUser>("u1");
        Assert.NotNull(got);
        Assert.Equal(1, got!.Version);
    }

    [Fact]
    public async Task Update_BumpsVersion_WhenMatching()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithVersion<VersionedUser>(
            $"t{Guid.NewGuid():N}", u => u.Version);
        var s = (IDocumentStore)store;

        var doc = new VersionedUser { Id = "u1", Name = "Alice", Age = 30 };
        await s.Insert(doc);

        doc.Age = 31;
        await s.Update(doc);

        Assert.Equal(2, doc.Version);

        var got = await s.Get<VersionedUser>("u1");
        Assert.Equal(2, got!.Version);
        Assert.Equal(31, got.Age);
    }

    [Fact]
    public async Task Update_StaleVersion_ThrowsConcurrencyException()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithVersion<VersionedUser>(
            $"t{Guid.NewGuid():N}", u => u.Version);
        var s = (IDocumentStore)store;

        await s.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });

        // Two callers load the same row, both think they're updating version 1.
        var first = await s.Get<VersionedUser>("u1");
        var second = await s.Get<VersionedUser>("u1");

        first!.Age = 31;
        await s.Update(first);

        second!.Age = 99;
        await Assert.ThrowsAsync<ConcurrencyException>(async () => await s.Update(second));

        var got = await s.Get<VersionedUser>("u1");
        Assert.Equal(31, got!.Age); // first writer's update persisted, second was rejected
        Assert.Equal(2, got.Version);
    }

    [Fact]
    public async Task Upsert_OnFreshDoc_SeedsVersion()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithVersion<VersionedUser>(
            $"t{Guid.NewGuid():N}", u => u.Version);
        var s = (IDocumentStore)store;

        var doc = new VersionedUser { Id = "u1", Name = "Alice", Age = 30 };
        await s.Upsert(doc);

        var got = await s.Get<VersionedUser>("u1");
        Assert.NotNull(got);
        Assert.Equal(1, got!.Version);
    }
}
