using Shiny.DocumentDb.Tests.Fixtures;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class QueryTests : IDisposable
{
    readonly SqliteDocumentStore store;

    public QueryTests()
    {
        this.store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:")
        });
    }

    public void Dispose() => this.store.Dispose();

    async Task SeedUsersAsync()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25, Email = "alice@test.com" });
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 35 });
        await this.store.Insert(new User { Id = "u3", Name = "Charlie", Age = 25 });
    }

    [Fact]
    public async Task Query_ByJsonExtract_EqualityFilter()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query<User>(
            "json_extract(Data, '$.name') = @name",
            parameters: new { name = "Alice" });

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Query_ByJsonExtract_NumericComparison()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query<User>(
            "json_extract(Data, '$.age') > @minAge",
            parameters: new { minAge = 30 });

        Assert.Single(results);
        Assert.Equal("Bob", results[0].Name);
    }

    [Fact]
    public async Task Query_ByJsonExtract_MultipleResults()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query<User>(
            "json_extract(Data, '$.age') = @age",
            parameters: new { age = 25 });

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Query_NoMatches_ReturnsEmpty()
    {
        await this.SeedUsersAsync();

        var results = await this.store.Query<User>(
            "json_extract(Data, '$.name') = @name",
            parameters: new { name = "Nobody" });

        Assert.Empty(results);
    }

    [Fact]
    public async Task Count_WithWhereClause()
    {
        await this.SeedUsersAsync();

        var count = await this.store.Count<User>(
            "json_extract(Data, '$.age') = @age",
            new { age = 25 });

        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Query_WithDictionaryParameters()
    {
        await this.SeedUsersAsync();

        var parameters = new Dictionary<string, object?> { ["name"] = "Bob" };
        var results = await this.store.Query<User>(
            "json_extract(Data, '$.name') = @name",
            parameters: parameters);

        Assert.Single(results);
        Assert.Equal("Bob", results[0].Name);
    }

    // ── Complex object with children ──────────────────────────────────

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
        });

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
        });

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
        });
    }

    [Fact]
    public async Task Query_NestedObject_PropertyAccess()
    {
        await this.SeedOrdersAsync();

        // query by a nested object property: shippingAddress.city
        var results = await this.store.Query<Order>(
            "json_extract(Data, '$.shippingAddress.city') = @city",
            parameters: new { city = "Portland" });

        Assert.Equal(2, results.Count);
        Assert.All(results, o => Assert.Equal("Portland", o.ShippingAddress.City));
    }

    [Fact]
    public async Task Query_NestedObject_MultipleProperties()
    {
        await this.SeedOrdersAsync();

        // combine top-level + nested filters
        var results = await this.store.Query<Order>(
            "json_extract(Data, '$.status') = @status AND json_extract(Data, '$.shippingAddress.state') = @state",
            parameters: new { status = "Shipped", state = "OR" });

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Query_ArrayElement_ByIndex()
    {
        await this.SeedOrdersAsync();

        // access the first order line by index
        var results = await this.store.Query<Order>(
            "json_extract(Data, '$.lines[0].productName') = @product",
            parameters: new { product = "Widget" });

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o.CustomerName == "Alice");
        Assert.Contains(results, o => o.CustomerName == "Bob");
    }

    [Fact]
    public async Task Query_ArrayLength()
    {
        await this.SeedOrdersAsync();

        // orders with more than one line item
        var results = await this.store.Query<Order>(
            "json_array_length(json_extract(Data, '$.lines')) > @minLines",
            parameters: new { minLines = 1 });

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o.CustomerName == "Alice");
        Assert.Contains(results, o => o.CustomerName == "Charlie");
    }

    [Fact]
    public async Task Query_ArrayContainsValue_UsingJsonEach()
    {
        await this.SeedOrdersAsync();

        // find orders where the tags array contains 'priority' using json_each
        var results = await this.store.Query<Order>(
            "EXISTS (SELECT 1 FROM json_each(Data, '$.tags') WHERE value = @tag)",
            parameters: new { tag = "priority" });

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o.CustomerName == "Alice");
        Assert.Contains(results, o => o.CustomerName == "Charlie");
    }

    [Fact]
    public async Task Query_ChildCollection_UsingJsonEach()
    {
        await this.SeedOrdersAsync();

        // find orders that contain a line item for 'Gadget' using json_each + json_extract
        var results = await this.store.Query<Order>(
            "EXISTS (SELECT 1 FROM json_each(Data, '$.lines') WHERE json_extract(value, '$.productName') = @product)",
            parameters: new { product = "Gadget" });

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o.CustomerName == "Alice");
        Assert.Contains(results, o => o.CustomerName == "Charlie");
    }

    [Fact]
    public async Task Query_ChildCollection_NumericFilter()
    {
        await this.SeedOrdersAsync();

        // find orders that have any line item with quantity >= 3
        var results = await this.store.Query<Order>(
            "EXISTS (SELECT 1 FROM json_each(Data, '$.lines') WHERE json_extract(value, '$.quantity') >= @minQty)",
            parameters: new { minQty = 3 });

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o.CustomerName == "Bob");     // Widget qty 5
        Assert.Contains(results, o => o.CustomerName == "Charlie"); // Doohickey qty 3
    }

    [Fact]
    public async Task Count_ChildCollection_WithAggregateFilter()
    {
        await this.SeedOrdersAsync();

        // count orders where the total line count > 1
        var count = await this.store.Count<Order>(
            "json_array_length(json_extract(Data, '$.lines')) > @minLines",
            new { minLines = 1 });

        Assert.Equal(2, count);
    }
}
