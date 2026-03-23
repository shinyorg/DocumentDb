using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class ProjectionQueryTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly DocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected ProjectionQueryTestsBase(IDatabaseFixture fixture)
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
        await this.store.Insert(new User { Id = "u3", Name = "Charlie", Age = 25 }, ctx.User);
    }

    async Task SeedOrdersAsync()
    {
        await this.store.Insert(new Order
        {
            Id = "o1",
            CustomerName = "Alice",
            Status = "Shipped",
            ShippingAddress = new Address { Street = "123 Main St", City = "Portland", State = "OR", Zip = "97201" },
            Lines =
            [
                new OrderLine { ProductName = "Widget", Quantity = 2, UnitPrice = 9.99m },
                new OrderLine { ProductName = "Gadget", Quantity = 1, UnitPrice = 24.99m }
            ],
            Tags = ["priority", "wholesale"]
        }, ctx.Order);

        await this.store.Insert(new Order
        {
            Id = "o2",
            CustomerName = "Bob",
            Status = "Pending",
            ShippingAddress = new Address { Street = "456 Oak Ave", City = "Seattle", State = "WA", Zip = "98101" },
            Lines =
            [
                new OrderLine { ProductName = "Widget", Quantity = 5, UnitPrice = 9.99m }
            ],
            Tags = ["retail"]
        }, ctx.Order);

        await this.store.Insert(new Order
        {
            Id = "o3",
            CustomerName = "Charlie",
            Status = "Shipped",
            ShippingAddress = new Address { Street = "789 Elm Blvd", City = "Portland", State = "OR", Zip = "97205" },
            Lines =
            [
                new OrderLine { ProductName = "Doohickey", Quantity = 3, UnitPrice = 14.99m },
                new OrderLine { ProductName = "Gadget", Quantity = 2, UnitPrice = 24.99m }
            ],
            Tags = ["priority", "retail"]
        }, ctx.Order);
    }

    // ── Flat projection ─────────────────────────────────────────────

    [Fact]
    public async Task Query_FlatProjection()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User)
            .Where(u => u.Age == 25)
            .Select(
                u => new UserSummary { Name = u.Name, Email = u.Email },
                ctx.UserSummary)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Name == "Alice" && r.Email == "alice@test.com");
        Assert.Contains(results, r => r.Name == "Charlie" && r.Email == null);
    }

    // ── Nested source property ──────────────────────────────────────

    [Fact]
    public async Task Query_NestedSourceProjection()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Where(o => o.Status == "Shipped")
            .Select(
                o => new OrderSummary { Customer = o.CustomerName, City = o.ShippingAddress.City },
                ctx.OrderSummary)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Customer == "Alice" && r.City == "Portland");
        Assert.Contains(results, r => r.Customer == "Charlie" && r.City == "Portland");
    }

    // ── Select without Where ──────────────────────────────────────

    [Fact]
    public async Task Select_WithoutWhere()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User)
            .Select(
                u => new UserSummary { Name = u.Name, Email = u.Email },
                ctx.UserSummary)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.Name == "Alice");
        Assert.Contains(results, r => r.Name == "Bob");
        Assert.Contains(results, r => r.Name == "Charlie");
    }

    // ── Projection with predicate filtering ─────────────────────────

    [Fact]
    public async Task Query_ProjectionWithPredicate()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User)
            .Where(u => u.Name == "Alice")
            .Select(
                u => new UserSummary { Name = u.Name, Email = u.Email },
                ctx.UserSummary)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal("alice@test.com", results[0].Email);
    }

    // ── Empty result set ────────────────────────────────────────────

    [Fact]
    public async Task Query_Projection_EmptyResult()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User)
            .Where(u => u.Name == "Nobody")
            .Select(
                u => new UserSummary { Name = u.Name, Email = u.Email },
                ctx.UserSummary)
            .ToList();

        Assert.Empty(results);
    }

    // ── Null property in projection ─────────────────────────────────

    [Fact]
    public async Task Query_Projection_NullProperty()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query(ctx.User)
            .Where(u => u.Name == "Bob")
            .Select(
                u => new UserSummary { Name = u.Name, Email = u.Email },
                ctx.UserSummary)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Bob", results[0].Name);
        Assert.Null(results[0].Email);
    }

    // ── Count() no predicate ────────────────────────────────────────

    [Fact]
    public async Task Query_Projection_CountNoPredicate()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Where(o => o.CustomerName == "Alice")
            .Select(
                o => new OrderDetail { Customer = o.CustomerName, LineCount = o.Lines.Count() },
                ctx.OrderDetail)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Customer);
        Assert.Equal(2, results[0].LineCount);
    }

    // ── Count(predicate) ────────────────────────────────────────────

    [Fact]
    public async Task Query_Projection_CountWithPredicate()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Where(o => o.Status == "Shipped")
            .Select(
                o => new OrderDetail { Customer = o.CustomerName, LineCount = o.Lines.Count(l => l.ProductName == "Gadget") },
                ctx.OrderDetail)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Customer == "Alice" && r.LineCount == 1);
        Assert.Contains(results, r => r.Customer == "Charlie" && r.LineCount == 1);
    }

    // ── Any() no predicate ──────────────────────────────────────────

    [Fact]
    public async Task Query_Projection_AnyNoPredicate()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Select(
                o => new OrderDetail { Customer = o.CustomerName, HasPriority = o.Tags.Any() },
                ctx.OrderDetail)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.All(results, r => Assert.True(r.HasPriority));
    }

    // ── Any(predicate) ──────────────────────────────────────────────

    [Fact]
    public async Task Query_Projection_AnyWithPredicate()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Select(
                o => new OrderDetail { Customer = o.CustomerName, HasPriority = o.Tags.Any(t => t == "priority") },
                ctx.OrderDetail)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.Customer == "Alice" && r.HasPriority);
        Assert.Contains(results, r => r.Customer == "Bob" && !r.HasPriority);
        Assert.Contains(results, r => r.Customer == "Charlie" && r.HasPriority);
    }

    // ── Select with Count ───────────────────────────────────────────

    [Fact]
    public async Task Select_WithCount()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Select(
                o => new OrderDetail { Customer = o.CustomerName, LineCount = o.Lines.Count() },
                ctx.OrderDetail)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.Customer == "Alice" && r.LineCount == 2);
        Assert.Contains(results, r => r.Customer == "Bob" && r.LineCount == 1);
        Assert.Contains(results, r => r.Customer == "Charlie" && r.LineCount == 2);
    }
}
