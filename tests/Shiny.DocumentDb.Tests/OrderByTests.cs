using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class OrderByTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly IDocumentStore store;
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

    public void Dispose() => (this.store as IDisposable)?.Dispose();

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

    // ── String-based OrderBy (AOT-safe via JsonTypeInfo) ────────────

    [Fact]
    public async Task OrderByString_ClrName_Ascending()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy("Age", ctx.User)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Bob", results[2].Name);
    }

    [Fact]
    public async Task OrderByString_JsonName_Ascending()
    {
        await this.SeedUsersAsync();
        // JSON naming policy is camelCase, so "age" is the JSON property name
        var results = await this.store.Query(ctx.User)
            .OrderBy("age", ctx.User)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Bob", results[2].Name);
    }

    [Fact]
    public async Task OrderByString_CaseInsensitive()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy("AGE", ctx.User)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Bob", results[2].Name);
    }

    [Fact]
    public async Task OrderByStringDescending()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderByDescending("Age", ctx.User)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Bob", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Alice", results[2].Name);
    }

    [Fact]
    public async Task OrderByString_WithWhere()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .Where(u => u.Age >= 25)
            .OrderBy("Name", ctx.User)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Bob", results[1].Name);
        Assert.Equal("Charlie", results[2].Name);
    }

    [Fact]
    public async Task OrderByString_MatchesExpressionOverload()
    {
        await this.SeedUsersAsync();
        var byExpression = await this.store.Query(ctx.User).OrderBy(u => u.Age).ToList();
        var byString = await this.store.Query(ctx.User).OrderBy("Age", ctx.User).ToList();

        Assert.Equal(
            byExpression.Select(u => u.Id).ToArray(),
            byString.Select(u => u.Id).ToArray());
    }

    [Fact]
    public async Task OrderByString_NestedPath_ClrName()
    {
        await this.store.Insert(new Order { Id = "o1", CustomerName = "Acme", Status = "open", ShippingAddress = new Address { City = "Seattle" } }, ctx.Order);
        await this.store.Insert(new Order { Id = "o2", CustomerName = "Beta", Status = "open", ShippingAddress = new Address { City = "Atlanta" } }, ctx.Order);
        await this.store.Insert(new Order { Id = "o3", CustomerName = "Gamma", Status = "open", ShippingAddress = new Address { City = "Boston" } }, ctx.Order);

        var results = await this.store.Query(ctx.Order)
            .OrderBy("ShippingAddress.City", ctx.Order)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("o2", results[0].Id); // Atlanta
        Assert.Equal("o3", results[1].Id); // Boston
        Assert.Equal("o1", results[2].Id); // Seattle
    }

    [Fact]
    public async Task OrderByString_NestedPath_JsonName()
    {
        await this.store.Insert(new Order { Id = "o1", CustomerName = "Acme", Status = "open", ShippingAddress = new Address { City = "Seattle" } }, ctx.Order);
        await this.store.Insert(new Order { Id = "o2", CustomerName = "Beta", Status = "open", ShippingAddress = new Address { City = "Atlanta" } }, ctx.Order);

        var results = await this.store.Query(ctx.Order)
            .OrderBy("shippingAddress.city", ctx.Order)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("o2", results[0].Id);
        Assert.Equal("o1", results[1].Id);
    }

    [Fact]
    public void OrderByString_UnknownProperty_Throws()
    {
        Assert.Throws<ArgumentException>(
            () => this.store.Query(ctx.User).OrderBy("DoesNotExist", ctx.User));
    }

    [Fact]
    public void OrderByString_UnknownNestedSegment_Throws()
    {
        Assert.Throws<ArgumentException>(
            () => this.store.Query(ctx.Order).OrderBy("ShippingAddress.Nope", ctx.Order));
    }

    [Fact]
    public void OrderByString_NullPath_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => this.store.Query(ctx.User).OrderBy(null!, ctx.User));
    }

    [Fact]
    public void OrderByString_EmptyOrWhitespacePath_Throws()
    {
        Assert.Throws<ArgumentException>(
            () => this.store.Query(ctx.User).OrderBy("", ctx.User));
        Assert.Throws<ArgumentException>(
            () => this.store.Query(ctx.User).OrderBy("   ", ctx.User));
    }

    [Fact]
    public async Task OrderByString_OmittedTypeInfo_ResolvesFromQuery()
    {
        await this.SeedUsersAsync();
        // The query was created with ctx.User, so the JsonTypeInfo can be omitted here.
        var results = await this.store.Query(ctx.User).OrderBy("Age").ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Bob", results[2].Name);
    }

    [Fact]
    public async Task OrderByStringDescending_OmittedTypeInfo_ResolvesFromQuery()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).OrderByDescending("Age").ToList();
        Assert.Equal("Bob", results[0].Name);
    }

    [Fact]
    public async Task OrderByString_Direction_OmittedTypeInfo_ResolvesFromQuery()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).OrderBy("Age", "desc").ToList();
        Assert.Equal("Bob", results[0].Name);
    }

    // ── String-based OrderBy with direction argument ────────────────

    [Theory]
    [InlineData("asc")]
    [InlineData("ascending")]
    [InlineData("ASC")]
    [InlineData("Ascending")]
    [InlineData("  asc  ")]
    public async Task OrderByString_Direction_Ascending(string direction)
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy("Age", direction, ctx.User)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Bob", results[2].Name);
    }

    [Theory]
    [InlineData("desc")]
    [InlineData("descending")]
    [InlineData("DESC")]
    [InlineData("Descending")]
    [InlineData("  desc  ")]
    public async Task OrderByString_Direction_Descending(string direction)
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy("Age", direction, ctx.User)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Bob", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Alice", results[2].Name);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task OrderByString_Direction_EmptyDefaultsToAscending(string? direction)
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .OrderBy("Age", direction, ctx.User)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("Charlie", results[1].Name);
        Assert.Equal("Bob", results[2].Name);
    }

    [Fact]
    public void OrderByString_Direction_Invalid_Throws()
    {
        Assert.Throws<ArgumentException>(
            () => this.store.Query(ctx.User).OrderBy("Age", "sideways", ctx.User));
    }

    [Fact]
    public void OrderByString_Direction_NullPath_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => this.store.Query(ctx.User).OrderBy(null!, "asc", ctx.User));
    }
}
