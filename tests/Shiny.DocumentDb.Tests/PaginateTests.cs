using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class PaginateTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected PaginateTestsBase(IDatabaseFixture fixture)
    {
        this.Fixture = fixture;
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    async Task SeedUsersAsync()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25, Email = "alice@test.com" }, ctx.User);
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 35 }, ctx.User);
        await this.store.Insert(new User { Id = "u3", Name = "Charlie", Age = 30 }, ctx.User);
        await this.store.Insert(new User { Id = "u4", Name = "Diana", Age = 28 }, ctx.User);
        await this.store.Insert(new User { Id = "u5", Name = "Eve", Age = 22 }, ctx.User);
    }

    // ── ToList ──────────────────────────────────────────────────

    [Fact]
    public async Task Paginate_FirstPage()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .Paginate(0, 2)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
    }

    [Fact]
    public async Task Paginate_SecondPage()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .Paginate(2, 2)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Charlie", results[0].Name);
        Assert.Equal("Diana", results[1].Name);
    }

    [Fact]
    public async Task Paginate_LastPage_PartialResults()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .Paginate(4, 2)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Eve", results[0].Name);
    }

    [Fact]
    public async Task Paginate_BeyondEnd_ReturnsEmpty()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .Paginate(10, 2)
            .ToList();

        Assert.Empty(results);
    }

    [Fact]
    public async Task Paginate_TakeAll()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .Paginate(0, 100)
            .ToList();

        Assert.Equal(5, results.Count);
    }

    // ── With Where ──────────────────────────────────────────────

    [Fact]
    public async Task Where_Paginate()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .Where(u => u.Age >= 25)
            .OrderBy(u => u.Age)
            .Paginate(0, 2)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Diana", results[1].Name);
    }

    [Fact]
    public async Task Where_Paginate_SecondPage()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .Where(u => u.Age >= 25)
            .OrderBy(u => u.Age)
            .Paginate(2, 2)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Charlie", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
    }

    // ── ToAsyncEnumerable ───────────────────────────────────────

    [Fact]
    public async Task Stream_Paginate()
    {
        await this.SeedUsersAsync();
        var results = new List<User>();
        await foreach (var user in this.store.Query(ctx.User).OrderBy(u => u.Name).Paginate(0, 3).ToAsyncEnumerable())
            results.Add(user);

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
        Assert.Equal("Charlie", results[2].Name);
    }

    [Fact]
    public async Task Stream_Paginate_WithOffset()
    {
        await this.SeedUsersAsync();
        var results = new List<User>();
        await foreach (var user in this.store.Query(ctx.User).OrderBy(u => u.Name).Paginate(2, 2).ToAsyncEnumerable())
            results.Add(user);

        Assert.Equal(2, results.Count);
        Assert.Equal("Charlie", results[0].Name);
        Assert.Equal("Diana", results[1].Name);
    }

    // ── Projection + Paginate ───────────────────────────────────

    [Fact]
    public async Task Projection_Paginate()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .Paginate(1, 2)
            .Select(
                u => new UserSummary { Name = u.Name, Email = u.Email },
                ctx.UserSummary)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Bob", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
    }

    [Fact]
    public async Task Where_Projection_Paginate()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .Where(u => u.Age >= 25)
            .OrderByDescending(u => u.Name)
            .Paginate(0, 2)
            .Select(
                u => new UserSummary { Name = u.Name, Email = u.Email },
                ctx.UserSummary)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Diana", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
    }

    // ── Without OrderBy ─────────────────────────────────────────

    [Fact]
    public async Task Paginate_WithoutOrderBy_StillLimitsResults()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .Paginate(0, 2)
            .ToList();

        Assert.Equal(2, results.Count);
    }

    // ── PageResult ──────────────────────────────────────────────

    [Fact]
    public async Task PageResult_OneBased_FirstPage()
    {
        await this.SeedUsersAsync();
        var result = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .PageResult(page: 1, pageSize: 2);

        Assert.Equal(5, result.TotalCount);
        Assert.Equal(1, result.Page);
        Assert.Equal(2, result.PageSize);

        var records = result.Records.ToList();
        Assert.Equal(2, records.Count);
        Assert.Equal("Alice", records[0].Name);
        Assert.Equal("Bob", records[1].Name);
    }

    [Fact]
    public async Task PageResult_OneBased_SecondPage()
    {
        await this.SeedUsersAsync();
        var result = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .PageResult(page: 2, pageSize: 2);

        Assert.Equal(5, result.TotalCount);
        Assert.Equal(2, result.Page);

        var records = result.Records.ToList();
        Assert.Equal(2, records.Count);
        Assert.Equal("Charlie", records[0].Name);
        Assert.Equal("Diana", records[1].Name);
    }

    [Fact]
    public async Task PageResult_LastPage_PartialRecords()
    {
        await this.SeedUsersAsync();
        var result = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .PageResult(page: 3, pageSize: 2);

        Assert.Equal(5, result.TotalCount);
        var records = result.Records.ToList();
        Assert.Single(records);
        Assert.Equal("Eve", records[0].Name);
    }

    [Fact]
    public async Task PageResult_BeyondEnd_EmptyButTotalReported()
    {
        await this.SeedUsersAsync();
        var result = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .PageResult(page: 10, pageSize: 2);

        Assert.Equal(5, result.TotalCount);
        Assert.Equal(10, result.Page);
        Assert.Empty(result.Records);
    }

    [Fact]
    public async Task PageResult_ZeroBased_FirstPage()
    {
        await this.SeedUsersAsync();
        var result = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .PageResult(page: 0, pageSize: 2, zeroBased: true);

        Assert.Equal(5, result.TotalCount);
        Assert.Equal(0, result.Page);

        var records = result.Records.ToList();
        Assert.Equal(2, records.Count);
        Assert.Equal("Alice", records[0].Name);
        Assert.Equal("Bob", records[1].Name);
    }

    [Fact]
    public async Task PageResult_ZeroBasedMatchesOneBased_SameOffset()
    {
        await this.SeedUsersAsync();
        var oneBased = await this.store.Query(ctx.User).OrderBy(u => u.Name).PageResult(page: 2, pageSize: 2);
        var zeroBased = await this.store.Query(ctx.User).OrderBy(u => u.Name).PageResult(page: 1, pageSize: 2, zeroBased: true);

        Assert.Equal(
            oneBased.Records.Select(u => u.Id).ToArray(),
            zeroBased.Records.Select(u => u.Id).ToArray());
    }

    [Fact]
    public async Task PageResult_TotalCount_RespectsWhereFilter()
    {
        await this.SeedUsersAsync();
        var result = await this.store.Query(ctx.User)
            .Where(u => u.Age >= 25)
            .OrderBy(u => u.Age)
            .PageResult(page: 1, pageSize: 2);

        Assert.Equal(4, result.TotalCount); // 25,28,30,35
        var records = result.Records.ToList();
        Assert.Equal(2, records.Count);
        Assert.Equal("Alice", records[0].Name);
        Assert.Equal("Diana", records[1].Name);
    }

    [Fact]
    public async Task PageResult_OverridesPriorPaginate()
    {
        await this.SeedUsersAsync();
        var result = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .Paginate(100, 1) // should be overridden
            .PageResult(page: 1, pageSize: 3);

        var records = result.Records.ToList();
        Assert.Equal(3, records.Count);
        Assert.Equal("Alice", records[0].Name);
        Assert.Equal("Bob", records[1].Name);
        Assert.Equal("Charlie", records[2].Name);
    }

    [Fact]
    public async Task PageResult_InvalidPageSize_Throws()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await this.store.Query(ctx.User).PageResult(page: 1, pageSize: 0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await this.store.Query(ctx.User).PageResult(page: 1, pageSize: -5));
    }

    [Fact]
    public async Task PageResult_InvalidPage_Throws()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await this.store.Query(ctx.User).PageResult(page: 0, pageSize: 10));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await this.store.Query(ctx.User).PageResult(page: -1, pageSize: 10, zeroBased: true));
    }
}
