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
}
