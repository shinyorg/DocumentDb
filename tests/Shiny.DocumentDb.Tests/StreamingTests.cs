using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class StreamingTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected StreamingTestsBase(IDatabaseFixture fixture)
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
    }

    static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source.ConfigureAwait(false))
            list.Add(item);
        return list;
    }

    [Fact]
    public async Task Stream_ReturnsAllItems()
    {
        await this.SeedUsersAsync();

        var results = await ToListAsync(this.store.Query(ctx.User).ToAsyncEnumerable());

        Assert.Equal(3, results.Count);
        Assert.Contains(results, u => u.Name == "Alice");
        Assert.Contains(results, u => u.Name == "Bob");
        Assert.Contains(results, u => u.Name == "Charlie");
    }

    [Fact]
    public async Task Stream_Projection()
    {
        await this.SeedOrdersAsync();

        var results = await ToListAsync(
            this.store.Query(ctx.Order)
                .Select(
                    o => new OrderSummary { Customer = o.CustomerName, City = o.ShippingAddress.City },
                    ctx.OrderSummary)
                .ToAsyncEnumerable());

        Assert.Equal(2, results.Count);
        Assert.Contains(results, s => s.Customer == "Alice" && s.City == "Portland");
        Assert.Contains(results, s => s.Customer == "Bob" && s.City == "Seattle");
    }

    [Fact]
    public async Task Stream_Where()
    {
        await this.SeedUsersAsync();

        var results = await ToListAsync(
            this.store.Query(ctx.User).Where(u => u.Age == 25).ToAsyncEnumerable());

        Assert.Equal(2, results.Count);
        Assert.Contains(results, u => u.Name == "Alice");
        Assert.Contains(results, u => u.Name == "Charlie");
    }

    [Fact]
    public async Task Stream_Where_Projection()
    {
        await this.SeedOrdersAsync();

        var results = await ToListAsync(
            this.store.Query(ctx.Order)
                .Where(o => o.Status == "Shipped")
                .Select(
                    o => new OrderSummary { Customer = o.CustomerName, City = o.ShippingAddress.City },
                    ctx.OrderSummary)
                .ToAsyncEnumerable());

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Customer);
        Assert.Equal("Portland", results[0].City);
    }

    [Fact]
    public async Task Stream_EmptyResult()
    {
        await this.SeedUsersAsync();

        var results = await ToListAsync(
            this.store.Query(ctx.User).Where(u => u.Name == "Nobody").ToAsyncEnumerable());

        Assert.Empty(results);
    }

    [Fact]
    public async Task Stream_NestedObjects()
    {
        await this.SeedOrdersAsync();

        var results = await ToListAsync(this.store.Query(ctx.Order).ToAsyncEnumerable());

        Assert.Equal(2, results.Count);

        var alice = Assert.Single(results, o => o.CustomerName == "Alice");
        Assert.Equal("Portland", alice.ShippingAddress.City);
        Assert.Equal(2, alice.Lines.Count);
        Assert.Contains(alice.Lines, l => l.ProductName == "Widget");
        Assert.Contains(alice.Tags, t => t == "priority");
    }
}
