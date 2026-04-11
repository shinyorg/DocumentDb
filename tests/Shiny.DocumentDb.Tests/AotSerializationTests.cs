using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class AotSerializationTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly IDocumentStore store;

    protected AotSerializationTestsBase(IDatabaseFixture fixture)
    {
        this.Fixture = fixture;
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public async Task Insert_And_Get_WithJsonTypeInfo()
    {
        var user = new User { Id = "user-aot-1", Name = "Allan", Age = 30, Email = "allan@test.com" };
        await this.store.Insert(user, TestJsonContext.Default.User);

        var result = await this.store.Get(user.Id, TestJsonContext.Default.User);

        Assert.NotNull(result);
        Assert.Equal("Allan", result.Name);
        Assert.Equal(30, result.Age);
    }

    [Fact]
    public async Task Insert_WithExplicitId_And_Get_WithJsonTypeInfo()
    {
        await this.store.Insert(new User { Id = "user-aot", Name = "AOT User" }, TestJsonContext.Default.User);

        var result = await this.store.Get("user-aot", TestJsonContext.Default.User);

        Assert.NotNull(result);
        Assert.Equal("AOT User", result.Name);
    }

    [Fact]
    public async Task Query_ReturnsAll_WithJsonTypeInfo()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice" }, TestJsonContext.Default.User);
        await this.store.Insert(new User { Id = "u2", Name = "Bob" }, TestJsonContext.Default.User);

        var results = await this.store.Query(TestJsonContext.Default.User).ToList();

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Query_WithJsonTypeInfo()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25 }, TestJsonContext.Default.User);
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 35 }, TestJsonContext.Default.User);

        var provider = this.Fixture.CreateProvider();
        var results = await this.store.Query(
            $"{provider.JsonExtract("Data", "Name")} = @name",
            TestJsonContext.Default.User,
            new { name = "Alice" });

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task Product_WithJsonTypeInfo()
    {
        await this.store.Insert(new Product { Id = "p1", Title = "Widget", Price = 9.99m }, TestJsonContext.Default.Product);

        var result = await this.store.Get("p1", TestJsonContext.Default.Product);

        Assert.NotNull(result);
        Assert.Equal("Widget", result.Title);
        Assert.Equal(9.99m, result.Price);
    }
}
