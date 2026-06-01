using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class QueryFilterTestsBase
{
    protected readonly IDocumentStoreFixture Fixture;

    protected QueryFilterTestsBase(IDocumentStoreFixture fixture)
        => this.Fixture = fixture;

    // ── Query path ──────────────────────────────────────────────────────

    [Fact]
    public async Task QueryPath_AppliesFilter()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithFilter<User>(
            $"t{Guid.NewGuid():N}", u => u.Age >= 18);

        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "kid", Name = "K", Age = 10 });
        await s.Insert(new User { Id = "teen", Name = "T", Age = 18 });
        await s.Insert(new User { Id = "adult", Name = "A", Age = 30 });

        var results = await s.Query<User>().ToList();
        Assert.Equal(2, results.Count);
        Assert.All(results, u => Assert.True(u.Age >= 18));
    }

    [Fact]
    public async Task QueryPath_AppliesFilter_WithUserWhere()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithFilter<User>(
            $"t{Guid.NewGuid():N}", u => u.Age >= 18);

        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "kid",   Name = "Andy",  Age = 10 });
        await s.Insert(new User { Id = "teen",  Name = "Alex",  Age = 18 });
        await s.Insert(new User { Id = "adult", Name = "Adam",  Age = 30 });
        await s.Insert(new User { Id = "bob",   Name = "Bob",   Age = 40 });

        var results = await s.Query<User>().Where(u => u.Name.StartsWith("A")).ToList();
        Assert.Equal(2, results.Count); // Alex and Adam — Andy excluded by filter, Bob by user predicate
    }

    [Fact]
    public async Task IgnoreQueryFilters_DisablesAll()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithFilter<User>(
            $"t{Guid.NewGuid():N}", u => u.Age >= 18);

        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "kid",   Name = "K", Age = 10 });
        await s.Insert(new User { Id = "adult", Name = "A", Age = 30 });

        var results = await s.Query<User>().IgnoreQueryFilters().ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task NamedFilter_IgnoreQueryFiltersByName()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithNamedFilter<User>(
            $"t{Guid.NewGuid():N}", "adultsOnly", u => u.Age >= 18);

        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "kid",   Name = "K", Age = 10 });
        await s.Insert(new User { Id = "adult", Name = "A", Age = 30 });

        var withFilter = await s.Query<User>().ToList();
        Assert.Single(withFilter);

        var withoutFilter = await s.Query<User>().IgnoreQueryFilters("adultsOnly").ToList();
        Assert.Equal(2, withoutFilter.Count);

        // Wrong name leaves the filter in place
        var stillFiltered = await s.Query<User>().IgnoreQueryFilters("unknown").ToList();
        Assert.Single(stillFiltered);
    }

    // ── Single-document paths ───────────────────────────────────────────

    [Fact]
    public async Task Get_RespectsFilter()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithFilter<User>(
            $"t{Guid.NewGuid():N}", u => u.Age >= 18);

        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "kid",   Name = "K", Age = 10 });
        await s.Insert(new User { Id = "adult", Name = "A", Age = 30 });

        Assert.Null(await s.Get<User>("kid"));
        Assert.NotNull(await s.Get<User>("adult"));
    }

    [Fact]
    public async Task Remove_RespectsFilter()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithFilter<User>(
            $"t{Guid.NewGuid():N}", u => u.Age >= 18);

        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "kid",   Name = "K", Age = 10 });
        await s.Insert(new User { Id = "adult", Name = "A", Age = 30 });

        Assert.False(await s.Remove<User>("kid"));   // filter rejects → treat as not-found
        Assert.True(await s.Remove<User>("adult"));
        var stillThere = await s.Query<User>().IgnoreQueryFilters().ToList();
        Assert.Single(stillThere); // kid still present
        Assert.Equal("kid", stillThere[0].Id);
    }

    [Fact]
    public async Task Update_RespectsFilter()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithFilter<User>(
            $"t{Guid.NewGuid():N}", u => u.Age >= 18);

        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "kid", Name = "K", Age = 10 });

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => s.Update(new User { Id = "kid", Name = "Kiddo", Age = 10 }));
    }

    // ── Insert is NOT filtered (matches EF Core) ────────────────────────

    [Fact]
    public async Task Insert_IsNotFiltered()
    {
        using var store = (IDisposable)this.Fixture.CreateStoreWithFilter<User>(
            $"t{Guid.NewGuid():N}", u => u.Age >= 18);

        var s = (IDocumentStore)store;
        // Insert succeeds even though the doc fails the filter — matches EF Core.
        await s.Insert(new User { Id = "kid", Name = "K", Age = 10 });

        // But Get / Query don't see it
        Assert.Null(await s.Get<User>("kid"));

        // IgnoreQueryFilters shows it
        var all = await s.Query<User>().IgnoreQueryFilters().ToList();
        Assert.Single(all);
    }

    // ── Captured variable is re-read per query ──────────────────────────

    [Fact]
    public async Task FilterWithCapturedVariable_ReReadsPerQuery()
    {
        var minAgeBox = new MinAgeHolder { Value = 18 };
        var tableName = $"t{Guid.NewGuid():N}";

        // Note: the captured field is read each time the predicate is translated.
        using var store = (IDisposable)this.Fixture.CreateStoreWithFilter<User>(
            tableName, u => u.Age >= minAgeBox.Value);

        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "young",  Name = "Y", Age = 10 });
        await s.Insert(new User { Id = "adult",  Name = "A", Age = 25 });

        var withAdult = await s.Query<User>().ToList();
        Assert.Single(withAdult);

        // Lower the bar and re-query — both should now be visible.
        minAgeBox.Value = 5;
        var withBoth = await s.Query<User>().ToList();
        Assert.Equal(2, withBoth.Count);
    }

    sealed class MinAgeHolder { public int Value; }
}
