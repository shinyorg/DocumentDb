using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class DocumentStoreTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly DocumentStore store;

    protected DocumentStoreTestsBase(IDatabaseFixture fixture)
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
    public async Task Insert_WithAutoId_PopulatesId()
    {
        var model = new GuidIdModel { Name = "Allan" };
        await this.store.Insert(model);
        Assert.NotEqual(Guid.Empty, model.Id);
    }

    [Fact]
    public async Task Insert_And_Get_RoundTrips()
    {
        var user = new User { Id = "user-rt", Name = "Allan", Age = 30, Email = "allan@test.com" };
        await this.store.Insert(user);

        var result = await this.store.Get<User>(user.Id);

        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Equal(30, result.Age);
        Assert.Equal("allan@test.com", result.Email);
    }

    [Fact]
    public async Task Insert_WithExplicitId_And_Get()
    {
        await this.store.Insert(new User { Id = "user-1", Name = "Allan" });

        var result = await this.store.Get<User>("user-1");

        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
    }

    [Fact]
    public async Task Insert_ThrowsOnDuplicate()
    {
        await this.store.Insert(new User { Id = "user-1", Name = "Allan", Age = 30 });

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => this.store.Insert(new User { Id = "user-1", Name = "Updated", Age = 31 }));
    }

    [Fact]
    public async Task Update_ReplacesExistingDocument()
    {
        await this.store.Insert(new User { Id = "user-1", Name = "Allan", Age = 30 });
        await this.store.Update(new User { Id = "user-1", Name = "Updated", Age = 31 });

        var result = await this.store.Get<User>("user-1");

        Assert.NotNull(result);
        Assert.Equal("Updated", result.Name);
        Assert.Equal(31, result.Age);
    }

    [Fact]
    public async Task Update_ThrowsWhenNotFound()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => this.store.Update(new User { Id = "does-not-exist", Name = "Allan" }));
    }

    [Fact]
    public async Task Update_ThrowsWithDefaultId()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => this.store.Update(new User { Name = "Allan" }));
    }

    [Fact]
    public async Task Get_NonExistent_ReturnsNull()
    {
        var result = await this.store.Get<User>("does-not-exist");
        Assert.Null(result);
    }

    [Fact]
    public async Task Query_ReturnsAllDocumentsOfType()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice" });
        await this.store.Insert(new User { Id = "u2", Name = "Bob" });

        var results = await this.store.Query<User>().ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Remove_DeletesDocument()
    {
        await this.store.Insert(new User { Id = "user-1", Name = "Allan" });

        var deleted = await this.store.Remove<User>("user-1");
        Assert.True(deleted);

        var result = await this.store.Get<User>("user-1");
        Assert.Null(result);
    }

    [Fact]
    public async Task Remove_NonExistent_ReturnsFalse()
    {
        var deleted = await this.store.Remove<User>("nope");
        Assert.False(deleted);
    }

    [Fact]
    public async Task Clear_RemovesAllOfType()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice" });
        await this.store.Insert(new User { Id = "u2", Name = "Bob" });
        await this.store.Insert(new Product { Id = "p1", Title = "Widget", Price = 9.99m });

        var cleared = await this.store.Clear<User>();
        Assert.Equal(2, cleared);

        var users = await this.store.Query<User>().ToList();
        Assert.Empty(users);

        // Product should still exist
        var products = await this.store.Query<Product>().ToList();
        Assert.Single(products);
    }

    [Fact]
    public async Task Count_ReturnsCorrectCount()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice" });
        await this.store.Insert(new User { Id = "u2", Name = "Bob" });
        await this.store.Insert(new Product { Id = "p1", Title = "Widget" });

        Assert.Equal(2, await this.store.Count<User>());
        Assert.Equal(1, await this.store.Count<Product>());
    }

    [Fact]
    public async Task TypeIsolation_SameIdDifferentTypes()
    {
        await this.store.Insert(new User { Id = "id-1", Name = "Allan" });
        await this.store.Insert(new Product { Id = "id-1", Title = "Widget" });

        var user = await this.store.Get<User>("id-1");
        var product = await this.store.Get<Product>("id-1");

        Assert.NotNull(user);
        Assert.Equal("Allan", user.Name);
        Assert.NotNull(product);
        Assert.Equal("Widget", product.Title);
    }

    [Fact]
    public async Task Upsert_InsertsNewDocument_WhenIdDoesNotExist()
    {
        await this.store.Upsert(new User { Id = "user-1", Name = "Allan", Age = 30 });

        var result = await this.store.Get<User>("user-1");

        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Equal(30, result.Age);
    }

    [Fact]
    public async Task Upsert_MergesPatch_IntoExistingDocument()
    {
        await this.store.Insert(new User { Id = "user-1", Name = "Allan", Age = 30, Email = "allan@test.com" });

        // Patch name and age; Email is null so it is excluded from the patch and preserved
        await this.store.Upsert(new User { Id = "user-1", Name = "Allan", Age = 31 });

        var result = await this.store.Get<User>("user-1");

        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Equal(31, result.Age);
        Assert.Equal("allan@test.com", result.Email);
    }

    [Fact]
    public async Task Upsert_AotOverload_MergesPatch()
    {
        var typeInfo = Fixtures.TestJsonContext.Default.User;
        await this.store.Insert(new User { Id = "user-1", Name = "Allan", Age = 30, Email = "allan@test.com" }, typeInfo);

        // Patch name and age; Email is null so it is excluded from the patch and preserved
        await this.store.Upsert(new User { Id = "user-1", Name = "Allan", Age = 31 }, typeInfo);

        var result = await this.store.Get("user-1", typeInfo);

        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Equal(31, result.Age);
        Assert.Equal("allan@test.com", result.Email);
    }

    [Fact]
    public async Task RunInTransaction_CommitsOnSuccess()
    {
        await this.store.RunInTransaction(async tx =>
        {
            await tx.Insert(new User { Id = "u1", Name = "Alice" });
            await tx.Insert(new User { Id = "u2", Name = "Bob" });
        });

        var count = await this.store.Count<User>();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task RunInTransaction_RollsBackOnFailure()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await this.store.RunInTransaction(async tx =>
            {
                await tx.Insert(new User { Id = "u1", Name = "Alice" });
                throw new InvalidOperationException("Simulated failure");
            });
        });

        var count = await this.store.Count<User>();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task SetProperty_UpdatesScalarField()
    {
        await this.store.Insert(new User { Id = "user-1", Name = "Allan", Age = 30, Email = "allan@test.com" });

        var updated = await this.store.SetProperty<User>("user-1", u => u.Age, 31);
        Assert.True(updated);

        var result = await this.store.Get<User>("user-1");
        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Equal(31, result.Age);
        Assert.Equal("allan@test.com", result.Email);
    }

    [Fact]
    public async Task SetProperty_ReturnsFalse_WhenDocumentNotFound()
    {
        var updated = await this.store.SetProperty<User>("does-not-exist", u => u.Age, 31);
        Assert.False(updated);
    }

    [Fact]
    public async Task SetProperty_SetsNullValue()
    {
        await this.store.Insert(new User { Id = "user-1", Name = "Allan", Age = 30, Email = "allan@test.com" });

        var updated = await this.store.SetProperty<User>("user-1", u => u.Email, null);
        Assert.True(updated);

        var result = await this.store.Get<User>("user-1");
        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Null(result.Email);
    }

    [Fact]
    public async Task SetProperty_AotOverload()
    {
        var typeInfo = Fixtures.TestJsonContext.Default.User;
        await this.store.Insert(new User { Id = "user-1", Name = "Allan", Age = 30, Email = "allan@test.com" }, typeInfo);

        var updated = await this.store.SetProperty("user-1", (User u) => u.Age, 31, typeInfo);
        Assert.True(updated);

        var result = await this.store.Get("user-1", typeInfo);
        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Equal(31, result.Age);
        Assert.Equal("allan@test.com", result.Email);
    }

    [Fact]
    public async Task SetProperty_NestedPath()
    {
        var order = new Order
        {
            Id = "order-1",
            CustomerName = "Allan",
            Status = "Pending",
            ShippingAddress = new Address { Street = "123 Main", City = "Springfield", State = "IL", Zip = "62701" }
        };
        await this.store.Insert(order);

        var updated = await this.store.SetProperty<Order>("order-1", o => o.ShippingAddress.City, "Shelbyville");
        Assert.True(updated);

        var result = await this.store.Get<Order>("order-1");
        Assert.NotNull(result);
        Assert.Equal("Allan", result.CustomerName);
        Assert.Equal("Shelbyville", result.ShippingAddress.City);
        Assert.Equal("123 Main", result.ShippingAddress.Street);
    }

    [Fact]
    public async Task RemoveProperty_RemovesField()
    {
        await this.store.Insert(new User { Id = "user-1", Name = "Allan", Age = 30, Email = "allan@test.com" });

        var updated = await this.store.RemoveProperty<User>("user-1", u => u.Email);
        Assert.True(updated);

        var result = await this.store.Get<User>("user-1");
        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Equal(30, result.Age);
        Assert.Null(result.Email);
    }

    [Fact]
    public async Task RemoveProperty_ReturnsFalse_WhenDocumentNotFound()
    {
        var updated = await this.store.RemoveProperty<User>("does-not-exist", u => u.Email);
        Assert.False(updated);
    }

    [Fact]
    public async Task RemoveProperty_AotOverload()
    {
        var typeInfo = Fixtures.TestJsonContext.Default.User;
        await this.store.Insert(new User { Id = "user-1", Name = "Allan", Age = 30, Email = "allan@test.com" }, typeInfo);

        var updated = await this.store.RemoveProperty("user-1", (User u) => u.Email, typeInfo);
        Assert.True(updated);

        var result = await this.store.Get("user-1", typeInfo);
        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Equal(30, result.Age);
        Assert.Null(result.Email);
    }

    [Fact]
    public async Task RemoveProperty_NestedPath()
    {
        var order = new Order
        {
            Id = "order-1",
            CustomerName = "Allan",
            Status = "Pending",
            ShippingAddress = new Address { Street = "123 Main", City = "Springfield", State = "IL", Zip = "62701" }
        };
        await this.store.Insert(order);

        var updated = await this.store.RemoveProperty<Order>("order-1", o => o.ShippingAddress.City);
        Assert.True(updated);

        var result = await this.store.Get<Order>("order-1");
        Assert.NotNull(result);
        Assert.Equal("Allan", result.CustomerName);
        Assert.Equal("", result.ShippingAddress.City); // default value since field was removed
        Assert.Equal("123 Main", result.ShippingAddress.Street);
    }
}

public abstract class DocumentStoreResolverTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly DocumentStore store;

    protected DocumentStoreResolverTestsBase(IDatabaseFixture fixture)
    {
        this.Fixture = fixture;
        var ctx = new TestJsonContext(new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose() => this.store.Dispose();

    [Fact]
    public async Task Insert_WithResolver_UsesTypeInfo()
    {
        var user = new User { Id = "resolver-1", Name = "Allan", Age = 30, Email = "allan@test.com" };
        await this.store.Insert(user);

        var result = await this.store.Get<User>(user.Id);

        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Equal(30, result.Age);
        Assert.Equal("allan@test.com", result.Email);
    }

    [Fact]
    public async Task Upsert_WithResolver_UsesTypeInfo()
    {
        await this.store.Insert(new User { Id = "user-1", Name = "Allan", Age = 30, Email = "allan@test.com" });

        await this.store.Upsert(new User { Id = "user-1", Name = "Allan", Age = 31 });

        var result = await this.store.Get<User>("user-1");

        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Equal(31, result.Age);
        Assert.Equal("allan@test.com", result.Email);
    }

    [Fact]
    public async Task Query_ReturnsAllDocumentsOfType()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice" });
        await this.store.Insert(new User { Id = "u2", Name = "Bob" });

        var results = await this.store.Query<User>().ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task NoReflectionFallback_Throws_WhenTypeNotRegistered()
    {
        using var strict = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider(),
            UseReflectionFallback = false,
            TableName = $"t{Guid.NewGuid():N}"
        });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => strict.Insert(new User { Id = "test", Name = "Allan" }));

        Assert.Contains("No JsonTypeInfo registered", ex.Message);
        Assert.Contains("User", ex.Message);
    }

    [Fact]
    public async Task NoReflectionFallback_WithResolver_Works()
    {
        var ctx = new TestJsonContext(new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
        using var strict = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = Fixture.CreateProvider(),
            JsonSerializerOptions = ctx.Options,
            UseReflectionFallback = false,
            TableName = $"t{Guid.NewGuid():N}"
        });

        var user = new User { Id = "strict-1", Name = "Allan", Age = 30 };
        await strict.Insert(user);
        var result = await strict.Get<User>(user.Id);

        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
    }
}
