using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class OrderByTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly DocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected OrderByTestsBase(IDatabaseFixture fixture)
    {
        this.Fixture = fixture;
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose() => this.store.Dispose();

    async Task SeedUsersAsync()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25, Email = "alice@test.com" }, ctx.User);
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 35 }, ctx.User);
        await this.store.Insert(new User { Id = "u3", Name = "Charlie", Age = 30 }, ctx.User);
    }

    // ── ToList ──────────────────────────────────────────────────

    [Fact]
    public async Task OrderByAscending()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).OrderBy(u => u.Age).ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Bob", results[2].Name);
    }

    [Fact]
    public async Task OrderByDescending()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).OrderByDescending(u => u.Age).ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal("Bob", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Alice", results[2].Name);
    }

    [Fact]
    public async Task OrderByString()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).OrderBy(u => u.Name).ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
        Assert.Equal("Charlie", results[2].Name);
    }

    [Fact]
    public async Task NoOrderBy_ReturnsAllDocuments()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).ToList();
        Assert.Equal(3, results.Count);
    }

    // ── Query with Where + OrderBy ─────────────────────────────────

    [Fact]
    public async Task Where_OrderByAscending()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .Where(u => u.Age >= 25)
            .OrderBy(u => u.Age)
            .ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Bob", results[2].Name);
    }

    [Fact]
    public async Task Where_OrderByDescending()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .Where(u => u.Age > 25)
            .OrderByDescending(u => u.Age)
            .ToList();
        Assert.Equal(2, results.Count);
        Assert.Equal("Bob", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
    }

    // ── ToAsyncEnumerable ────────────────────────────────────────────

    [Fact]
    public async Task Stream_OrderByAscending()
    {
        await this.SeedUsersAsync();
        var results = new List<User>();
        await foreach (var user in this.store.Query(ctx.User).OrderBy(u => u.Age).ToAsyncEnumerable())
            results.Add(user);

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Bob", results[2].Name);
    }

    [Fact]
    public async Task Stream_OrderByDescending()
    {
        await this.SeedUsersAsync();
        var results = new List<User>();
        await foreach (var user in this.store.Query(ctx.User).OrderByDescending(u => u.Age).ToAsyncEnumerable())
            results.Add(user);

        Assert.Equal(3, results.Count);
        Assert.Equal("Bob", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Alice", results[2].Name);
    }

    // ── Stream with Where + OrderBy ───────────────────────────────

    [Fact]
    public async Task Stream_Where_OrderByAscending()
    {
        await this.SeedUsersAsync();
        var results = new List<User>();
        await foreach (var user in this.store.Query(ctx.User).Where(u => u.Age > 25).OrderBy(u => u.Age).ToAsyncEnumerable())
            results.Add(user);

        Assert.Equal(2, results.Count);
        Assert.Equal("Charlie", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
    }

    // ── Projection + OrderBy ────────────────────────────────────────

    [Fact]
    public async Task Projection_OrderBy()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy(u => u.Name)
            .Select(
                u => new UserSummary { Name = u.Name, Email = u.Email },
                ctx.UserSummary)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
        Assert.Equal("Charlie", results[2].Name);
    }

    [Fact]
    public async Task Where_Projection_OrderBy()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .Where(u => u.Age >= 25)
            .OrderByDescending(u => u.Name)
            .Select(
                u => new UserSummary { Name = u.Name, Email = u.Email },
                ctx.UserSummary)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Charlie", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
        Assert.Equal("Alice", results[2].Name);
    }
}
