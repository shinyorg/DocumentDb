using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The <see cref="IDocumentQuery{T}"/> contract, asserted identically on every provider. This is the net under
/// the shared-query-surface refactor: whatever a provider pushes down and whatever it evaluates client-side, the
/// observable behavior here must not move.
/// </summary>
public abstract class DocumentQueryConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    async Task<IDocumentStore> Seeded()
    {
        var store = this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30, Email = "a@x.com" });
        await store.Insert(new User { Id = "u2", Name = "Bob", Age = 40, Email = "b@x.com" });
        await store.Insert(new User { Id = "u3", Name = "Carol", Age = 50, Email = null });
        await store.Insert(new User { Id = "u4", Name = "Dave", Age = 20, Email = "d@x.com" });
        return store;
    }

    // ── Builder semantics ───────────────────────────────────────────────

    [Fact]
    public async Task Builder_IsImmutable_SourceQueryUnaffectedByDerivedCalls()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var all = store.Query<User>();
        var filtered = all.Where(x => x.Age >= 40);
        var paged = all.Paginate(0, 1);
        var ordered = all.OrderByDescending(x => x.Age);

        // Every builder call returns a new query; the source must still see everything, unordered and unpaged.
        Assert.Equal(4, await all.Count());
        Assert.Equal(2, await filtered.Count());
        Assert.Single(await paged.ToList());
        Assert.Equal(4, (await ordered.ToList()).Count);
    }

    [Fact]
    public async Task Where_Composes_WithAnd()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var results = await store.Query<User>().Where(x => x.Age >= 30).Where(x => x.Age < 50).ToList();

        Assert.Equal(2, results.Count);
        Assert.All(results, u => Assert.InRange(u.Age, 30, 49));
    }

    [Fact]
    public async Task OrderBy_Then_Paginate_AppliesInThatOrder()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var page = await store.Query<User>().OrderBy(x => x.Age).Paginate(1, 2).ToList();

        // Ages ascending are 20, 30, 40, 50 — skip 1, take 2.
        Assert.Equal(new[] { 30, 40 }, page.Select(u => u.Age).ToArray());
    }

    [Fact]
    public async Task OrderByDescending_Sorts()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var all = await store.Query<User>().OrderByDescending(x => x.Age).ToList();

        Assert.Equal(new[] { 50, 40, 30, 20 }, all.Select(u => u.Age).ToArray());
    }

    [Fact]
    public async Task Where_And_OrderBy_And_Paginate_Compose()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var results = await store.Query<User>()
            .Where(x => x.Age >= 30)
            .OrderByDescending(x => x.Age)
            .Paginate(0, 2)
            .ToList();

        Assert.Equal(new[] { 50, 40 }, results.Select(u => u.Age).ToArray());
    }

    // ── Terminals ───────────────────────────────────────────────────────

    [Fact]
    public async Task Count_And_Any_RespectPredicates()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        Assert.Equal(4, await store.Query<User>().Count());
        Assert.Equal(2, await store.Query<User>().Where(x => x.Age >= 40).Count());
        Assert.True(await store.Query<User>().Where(x => x.Age >= 40).Any());
        Assert.False(await store.Query<User>().Where(x => x.Age > 500).Any());
    }

    [Fact]
    public async Task ToAsyncEnumerable_StreamsTheSameRows()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var streamed = new List<User>();
        await foreach (var u in store.Query<User>().Where(x => x.Age >= 30).ToAsyncEnumerable())
            streamed.Add(u);

        Assert.Equal(3, streamed.Count);
    }

    [Fact]
    public async Task Aggregates_MatchTheFilteredSet()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var q = store.Query<User>().Where(x => x.Age >= 30);

        Assert.Equal(50, await q.Max(x => x.Age));
        Assert.Equal(30, await q.Min(x => x.Age));
        Assert.Equal(120, await q.Sum(x => x.Age));
        Assert.Equal(40d, await q.Average(x => x.Age), 3);
    }

    [Fact]
    public async Task Select_Projects()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        // The relational providers translate the projection into SQL and so need the result JsonTypeInfo to
        // materialize the rows; the client-side providers just run the compiled selector and ignore it.
        var names = await store.Query<User>()
            .Where(x => x.Age >= 40)
            .OrderBy(x => x.Age)
            .Select(x => new UserSummary { Name = x.Name, Email = x.Email }, TestJsonContext.Default.UserSummary)
            .ToList();

        Assert.Equal(new[] { "Bob", "Carol" }, names.Select(n => n.Name).ToArray());
    }

    [Fact]
    public async Task Select_RejectsFurtherComposition()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var projected = store.Query<User>().Select(x => new UserSummary { Name = x.Name }, TestJsonContext.Default.UserSummary);

        Assert.Throws<NotSupportedException>(() => projected.Where(x => x.Name == "Alice"));
        Assert.Throws<NotSupportedException>(() => projected.OrderBy(x => x.Name));
        Assert.Throws<NotSupportedException>(() => projected.Paginate(0, 1));
        Assert.Throws<NotSupportedException>(() => projected.IgnoreQueryFilters());
    }

    // ── Set-based writes ────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteUpdate_AffectsOnlyTheMatchedSet()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var affected = await store.Query<User>().Where(x => x.Age >= 40).ExecuteUpdate(x => x.Name, "Renamed");

        Assert.Equal(2, affected);
        var renamed = await store.Query<User>().Where(x => x.Name == "Renamed").Count();
        Assert.Equal(2, renamed);
    }

    [Fact]
    public async Task ExecuteDelete_DeletesOnlyTheMatchedSet()
    {
        var store = await this.Seeded();
        using var _ = (IDisposable)store;

        var affected = await store.Query<User>().Where(x => x.Age >= 40).ExecuteDelete();

        Assert.Equal(2, affected);
        Assert.Equal(2, await store.Query<User>().Count());
    }

    // ── Query filters interact correctly with everything above ──────────

    [Fact]
    public async Task Filters_ApplyToTerminalsAndSetBasedWrites()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore(
            $"t{Guid.NewGuid():N}", o => o.AddQueryFilter<User>("adults", u => u.Age >= 18));
        var store = (IDocumentStore)owner;

        await store.Insert(new User { Id = "kid", Name = "K", Age = 10 });
        await store.Insert(new User { Id = "a1", Name = "A", Age = 30 });
        await store.Insert(new User { Id = "a2", Name = "B", Age = 40 });

        Assert.Equal(2, await store.Query<User>().Count());
        Assert.Equal(3, await store.Query<User>().IgnoreQueryFilters().Count());
        Assert.Equal(3, await store.Query<User>().IgnoreQueryFilters("adults").Count());
        Assert.Equal(30, await store.Query<User>().Min(x => x.Age));

        // A filtered set-based delete must not reach the filtered-out document.
        Assert.Equal(2, await store.Query<User>().ExecuteDelete());
        Assert.Equal(1, await store.Query<User>().IgnoreQueryFilters().Count());
    }
}
