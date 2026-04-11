using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class AggregateTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected AggregateTestsBase(IDatabaseFixture fixture)
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

    async Task SeedProductsAsync()
    {
        await this.store.Insert(new Product { Id = "p1", Title = "Widget", Price = 9.99m }, ctx.Product);
        await this.store.Insert(new Product { Id = "p2", Title = "Gadget", Price = 24.99m }, ctx.Product);
        await this.store.Insert(new Product { Id = "p3", Title = "Doohickey", Price = 14.99m }, ctx.Product);
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

    // ── Scalar Max ──────────────────────────────────────────────────

    [Fact]
    public async Task Max_AllDocuments()
    {
        await this.SeedUsersAsync();
        var maxAge = await this.store.Query(ctx.User).Max<int>(u => u.Age);
        Assert.Equal(35, maxAge);
    }

    [Fact]
    public async Task Max_WithPredicate()
    {
        await this.SeedUsersAsync();
        var maxAge = await this.store.Query(ctx.User).Where(u => u.Age < 35).Max<int>(u => u.Age);
        Assert.Equal(30, maxAge);
    }

    [Fact]
    public async Task Max_EmptyResultSet()
    {
        var maxAge = await this.store.Query(ctx.User).Max<int>(u => u.Age);
        Assert.Equal(0, maxAge);
    }

    // ── Scalar Min ──────────────────────────────────────────────────

    [Fact]
    public async Task Min_AllDocuments()
    {
        await this.SeedUsersAsync();
        var minAge = await this.store.Query(ctx.User).Min<int>(u => u.Age);
        Assert.Equal(25, minAge);
    }

    [Fact]
    public async Task Min_WithPredicate()
    {
        await this.SeedUsersAsync();
        var minAge = await this.store.Query(ctx.User).Where(u => u.Age > 25).Min<int>(u => u.Age);
        Assert.Equal(30, minAge);
    }

    // ── Scalar Sum ──────────────────────────────────────────────────

    [Fact]
    public async Task Sum_AllDocuments()
    {
        await this.SeedUsersAsync();
        var totalAge = await this.store.Query(ctx.User).Sum<int>(u => u.Age);
        Assert.Equal(90, totalAge); // 25 + 35 + 30
    }

    [Fact]
    public async Task Sum_WithPredicate()
    {
        await this.SeedUsersAsync();
        var totalAge = await this.store.Query(ctx.User).Where(u => u.Age > 25).Sum<int>(u => u.Age);
        Assert.Equal(65, totalAge); // 35 + 30
    }

    [Fact]
    public async Task Sum_Decimal()
    {
        await this.SeedProductsAsync();
        var totalPrice = await this.store.Query(ctx.Product).Sum<decimal>(p => p.Price);
        Assert.Equal(49.97m, totalPrice);
    }

    // ── Scalar Average ──────────────────────────────────────────────

    [Fact]
    public async Task Average_AllDocuments()
    {
        await this.SeedUsersAsync();
        var avgAge = await this.store.Query(ctx.User).Average(u => u.Age);
        Assert.Equal(30.0, avgAge);
    }

    [Fact]
    public async Task Average_WithPredicate()
    {
        await this.SeedUsersAsync();
        var avgAge = await this.store.Query(ctx.User).Where(u => u.Age > 25).Average(u => u.Age);
        Assert.Equal(32.5, avgAge); // (35 + 30) / 2
    }

    [Fact]
    public async Task Average_EmptyResultSet()
    {
        var avgAge = await this.store.Query(ctx.User).Average(u => u.Age);
        Assert.Equal(0d, avgAge);
    }

    // ── Collection aggregates in projections ────────────────────────

    [Fact]
    public async Task Projection_SumOnCollection()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Select(
                o => new OrderLineAggregates
                {
                    Customer = o.CustomerName,
                    TotalQty = o.Lines.Sum(l => l.Quantity)
                },
                ctx.OrderLineAggregates)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.Customer == "Alice" && r.TotalQty == 3);   // 2 + 1
        Assert.Contains(results, r => r.Customer == "Bob" && r.TotalQty == 5);
        Assert.Contains(results, r => r.Customer == "Charlie" && r.TotalQty == 5); // 3 + 2
    }

    [Fact]
    public async Task Projection_MaxOnCollection()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Select(
                o => new OrderLineAggregates
                {
                    Customer = o.CustomerName,
                    MaxPrice = o.Lines.Max(l => l.UnitPrice)
                },
                ctx.OrderLineAggregates)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.Customer == "Alice" && r.MaxPrice == 24.99m);
        Assert.Contains(results, r => r.Customer == "Bob" && r.MaxPrice == 9.99m);
        Assert.Contains(results, r => r.Customer == "Charlie" && r.MaxPrice == 24.99m);
    }

    [Fact]
    public async Task Projection_MinOnCollection()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Select(
                o => new OrderLineAggregates
                {
                    Customer = o.CustomerName,
                    MinPrice = o.Lines.Min(l => l.UnitPrice)
                },
                ctx.OrderLineAggregates)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.Customer == "Alice" && r.MinPrice == 9.99m);
        Assert.Contains(results, r => r.Customer == "Bob" && r.MinPrice == 9.99m);
        Assert.Contains(results, r => r.Customer == "Charlie" && r.MinPrice == 14.99m);
    }

    [Fact]
    public async Task Projection_MultipleCollectionAggregates()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Where(o => o.CustomerName == "Alice")
            .Select(
                o => new OrderLineAggregates
                {
                    Customer = o.CustomerName,
                    TotalQty = o.Lines.Sum(l => l.Quantity),
                    MaxPrice = o.Lines.Max(l => l.UnitPrice),
                    MinPrice = o.Lines.Min(l => l.UnitPrice),
                },
                ctx.OrderLineAggregates)
            .ToList();

        Assert.Single(results);
        var r = results[0];
        Assert.Equal("Alice", r.Customer);
        Assert.Equal(3, r.TotalQty);
        Assert.Equal(24.99m, r.MaxPrice);
        Assert.Equal(9.99m, r.MinPrice);
    }

    // ── Aggregate with GROUP BY ─────────────────────────────────────

    [Fact]
    public async Task Aggregate_GroupBy_WithCount()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Select(
                o => new OrderStats
                {
                    Status = o.Status,
                    OrderCount = Sql.Count(),
                },
                ctx.OrderStats)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Status == "Shipped" && r.OrderCount == 2);
        Assert.Contains(results, r => r.Status == "Pending" && r.OrderCount == 1);
    }

    [Fact]
    public async Task Aggregate_GroupBy_WithPredicate()
    {
        await this.SeedOrdersAsync();

        var results = await this.store.Query(ctx.Order)
            .Where(o => o.Status == "Shipped")
            .Select(
                o => new OrderStats
                {
                    Status = o.Status,
                    OrderCount = Sql.Count(),
                },
                ctx.OrderStats)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Shipped", results[0].Status);
        Assert.Equal(2, results[0].OrderCount);
    }

    [Fact]
    public async Task Aggregate_NoGroupBy_AllAggregates()
    {
        await this.SeedProductsAsync();

        var results = await this.store.Query(ctx.Product)
            .Select(
                p => new PriceSummary
                {
                    TotalCount = Sql.Count(),
                    MaxPrice = Sql.Max(p.Price),
                    MinPrice = Sql.Min(p.Price),
                    SumPrice = Sql.Sum(p.Price),
                    AvgPrice = Sql.Avg(p.Price),
                },
                ctx.PriceSummary)
            .ToList();

        Assert.Single(results);
        var r = results[0];
        Assert.Equal(3, r.TotalCount);
        Assert.Equal(24.99m, r.MaxPrice);
        Assert.Equal(9.99m, r.MinPrice);
        Assert.Equal(49.97m, r.SumPrice);
        Assert.True(Math.Abs(r.AvgPrice - 16.66) < 0.01);
    }

    [Fact]
    public async Task Aggregate_EmptyResultSet()
    {
        // No products seeded — aggregate over empty set
        var results = await this.store.Query(ctx.Product)
            .Select(
                p => new PriceSummary
                {
                    TotalCount = Sql.Count(),
                    MaxPrice = Sql.Max(p.Price),
                    MinPrice = Sql.Min(p.Price),
                    SumPrice = Sql.Sum(p.Price),
                    AvgPrice = Sql.Avg(p.Price),
                },
                ctx.PriceSummary)
            .ToList();

        Assert.Single(results);
        var r = results[0];
        Assert.Equal(0, r.TotalCount);
    }
}
