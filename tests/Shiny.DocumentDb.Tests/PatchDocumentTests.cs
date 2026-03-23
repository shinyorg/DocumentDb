using Shiny.DocumentDb.Tests.Fixtures;
using SystemTextJsonPatch;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class PatchDocumentTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly DocumentStore store;

    protected PatchDocumentTestsBase(IDatabaseFixture fixture)
    {
        this.Fixture = fixture;
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose() => this.store.Dispose();

    [Fact]
    public async Task GetDiff_ReturnsNull_WhenDocumentNotFound()
    {
        var modified = new User { Id = "missing", Name = "Alice", Age = 30 };
        var patch = await this.store.GetDiff("missing", modified);
        Assert.Null(patch);
    }

    [Fact]
    public async Task GetDiff_ReturnsEmpty_WhenNoChanges()
    {
        var user = new User { Id = "user-1", Name = "Alice", Age = 30, Email = "alice@test.com" };
        await this.store.Insert(user);

        var same = new User { Id = "user-1", Name = "Alice", Age = 30, Email = "alice@test.com" };
        var patch = await this.store.GetDiff("user-1", same);

        Assert.NotNull(patch);
        Assert.Empty(patch.Operations);
    }

    [Fact]
    public async Task GetDiff_DetectsScalarChanges()
    {
        var user = new User { Id = "user-1", Name = "Alice", Age = 30, Email = "alice@test.com" };
        await this.store.Insert(user);

        var modified = new User { Id = "user-1", Name = "Alice", Age = 31, Email = "alice.new@test.com" };
        var patch = await this.store.GetDiff("user-1", modified);

        Assert.NotNull(patch);
        Assert.Equal(2, patch.Operations.Count);
        Assert.Contains(patch.Operations, op => op.Path.EndsWith("/age") || op.Path.EndsWith("/Age"));
        Assert.Contains(patch.Operations, op => op.Path.EndsWith("/email") || op.Path.EndsWith("/Email"));
    }

    [Fact]
    public async Task GetDiff_DetectsAddedProperty()
    {
        var user = new User { Id = "user-1", Name = "Alice", Age = 30, Email = null };
        await this.store.Insert(user);

        var modified = new User { Id = "user-1", Name = "Alice", Age = 30, Email = "alice@test.com" };
        var patch = await this.store.GetDiff("user-1", modified);

        Assert.NotNull(patch);
        Assert.Single(patch.Operations);
        var op = patch.Operations[0];
        Assert.True(op.Path.EndsWith("/email") || op.Path.EndsWith("/Email"));
    }

    [Fact]
    public async Task GetDiff_DetectsRemovedProperty()
    {
        var user = new User { Id = "user-1", Name = "Alice", Age = 30, Email = "alice@test.com" };
        await this.store.Insert(user);

        var modified = new User { Id = "user-1", Name = "Alice", Age = 30, Email = null };
        var patch = await this.store.GetDiff("user-1", modified);

        Assert.NotNull(patch);
        Assert.Single(patch.Operations);
        var op = patch.Operations[0];
        Assert.True(op.Path.EndsWith("/email") || op.Path.EndsWith("/Email"));
    }

    [Fact]
    public async Task GetDiff_DetectsNestedObjectChanges()
    {
        var order = new Order
        {
            Id = "ord-1",
            CustomerName = "Alice",
            Status = "Pending",
            ShippingAddress = new Address { Street = "123 Main", City = "Portland", State = "OR", Zip = "97201" }
        };
        await this.store.Insert(order);

        var modified = new Order
        {
            Id = "ord-1",
            CustomerName = "Alice",
            Status = "Pending",
            ShippingAddress = new Address { Street = "123 Main", City = "Seattle", State = "WA", Zip = "97201" }
        };
        var patch = await this.store.GetDiff("ord-1", modified);

        Assert.NotNull(patch);
        Assert.Equal(2, patch.Operations.Count);
        Assert.All(patch.Operations, op =>
            Assert.Contains("/shippingAddress/", op.Path, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task GetDiff_DetectsArrayChanges()
    {
        var order = new Order
        {
            Id = "ord-1",
            CustomerName = "Alice",
            Status = "Pending",
            Tags = ["urgent", "premium"]
        };
        await this.store.Insert(order);

        var modified = new Order
        {
            Id = "ord-1",
            CustomerName = "Alice",
            Status = "Pending",
            Tags = ["urgent", "standard"]
        };
        var patch = await this.store.GetDiff("ord-1", modified);

        Assert.NotNull(patch);
        Assert.NotEmpty(patch.Operations);
    }

    [Fact]
    public async Task GetDiff_PatchCanBeApplied()
    {
        var original = new User { Id = "user-1", Name = "Alice", Age = 30, Email = "alice@test.com" };
        await this.store.Insert(original);

        var modified = new User { Id = "user-1", Name = "Bob", Age = 35, Email = "bob@test.com" };
        var patch = await this.store.GetDiff("user-1", modified);

        Assert.NotNull(patch);

        // Get stored doc and apply patch
        var stored = await this.store.Get<User>("user-1");
        Assert.NotNull(stored);
        patch.ApplyTo(stored);

        Assert.Equal("Bob", stored.Name);
        Assert.Equal(35, stored.Age);
        Assert.Equal("bob@test.com", stored.Email);
    }

    [Fact]
    public async Task GetDiff_WorksWithTablePerType()
    {
        using var mappedStore = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}"
        }.MapTypeToTable<Order>($"orders_{Guid.NewGuid():N}"));

        var order = new Order
        {
            Id = "ord-1",
            CustomerName = "Alice",
            Status = "Shipped",
            ShippingAddress = new Address { City = "Portland" }
        };
        await mappedStore.Insert(order);

        var modified = new Order
        {
            Id = "ord-1",
            CustomerName = "Alice",
            Status = "Delivered",
            ShippingAddress = new Address { City = "Portland" }
        };
        var patch = await mappedStore.GetDiff("ord-1", modified);

        Assert.NotNull(patch);
        Assert.Single(patch.Operations);
    }

    [Fact]
    public async Task GetDiff_WorksWithCustomId()
    {
        using var customStore = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider()
        }.MapTypeToTable<CustomIdModel>($"custom_{Guid.NewGuid():N}", x => x.UserId));

        var model = new CustomIdModel { UserId = "u-1", Name = "Alice", Age = 30 };
        await customStore.Insert(model);

        var modified = new CustomIdModel { UserId = "u-1", Name = "Bob", Age = 30 };
        var patch = await customStore.GetDiff("u-1", modified);

        Assert.NotNull(patch);
        Assert.Single(patch.Operations);
    }

    [Fact]
    public async Task GetDiff_WorksInTransaction()
    {
        var user = new User { Id = "user-1", Name = "Alice", Age = 30, Email = "alice@test.com" };
        await this.store.Insert(user);

        await this.store.RunInTransaction(async txStore =>
        {
            var modified = new User { Id = "user-1", Name = "Alice", Age = 31, Email = "alice@test.com" };
            var patch = await txStore.GetDiff("user-1", modified);

            Assert.NotNull(patch);
            Assert.Single(patch.Operations);
        });
    }
}
