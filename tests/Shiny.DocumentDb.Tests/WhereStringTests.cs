using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class WhereStringTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected WhereStringTestsBase(IDatabaseFixture fixture)
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

    static string[] Ids(IReadOnlyList<User> users) => users.Select(u => u.Id).OrderBy(x => x).ToArray();

    [Fact]
    public async Task NumericComparison()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("Age > 28", ctx.User).ToList();
        Assert.Equal(["u2", "u3"], Ids(results));
    }

    [Fact]
    public async Task EqualsString()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("Name == 'Alice'", ctx.User).ToList();
        Assert.Equal(["u1"], Ids(results));
    }

    [Fact]
    public async Task SingleEqualsSign()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("Age = 30", ctx.User).ToList();
        Assert.Equal(["u3"], Ids(results));
    }

    [Fact]
    public async Task AndCombinator()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("Age >= 25 and Age <= 30", ctx.User).ToList();
        Assert.Equal(["u1", "u3"], Ids(results));
    }

    [Fact]
    public async Task OrCombinator()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("Name == 'Alice' or Name == 'Bob'", ctx.User).ToList();
        Assert.Equal(["u1", "u2"], Ids(results));
    }

    [Fact]
    public async Task NotWithParentheses()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("not (Age < 30)", ctx.User).ToList();
        Assert.Equal(["u2", "u3"], Ids(results));
    }

    [Fact]
    public async Task IsNull()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("Email is null", ctx.User).ToList();
        Assert.Equal(["u2", "u3"], Ids(results));
    }

    [Fact]
    public async Task IsNotNull()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("Email is not null", ctx.User).ToList();
        Assert.Equal(["u1"], Ids(results));
    }

    [Fact]
    public async Task EqualsNull()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("Email == null", ctx.User).ToList();
        Assert.Equal(["u2", "u3"], Ids(results));
    }

    [Fact]
    public async Task InList()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("Name in ('Alice', 'Charlie')", ctx.User).ToList();
        Assert.Equal(["u1", "u3"], Ids(results));
    }

    [Fact]
    public async Task StartsWithFunction()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("startsWith(Name, 'A')", ctx.User).ToList();
        Assert.Equal(["u1"], Ids(results));
    }

    [Fact]
    public async Task ContainsFunction()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where("contains(Email, 'alice')", ctx.User).ToList();
        Assert.Equal(["u1"], Ids(results));
    }

    [Theory]
    [InlineData("age > 28")]   // JSON name
    [InlineData("AGE > 28")]   // case-insensitive CLR name
    public async Task FieldNameMatching(string filter)
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User).Where(filter, ctx.User).ToList();
        Assert.Equal(["u2", "u3"], Ids(results));
    }

    [Fact]
    public async Task ComposesWithExpressionWhere()
    {
        await this.SeedUsersAsync();
        var results = await this.store.Query(ctx.User)
            .Where(u => u.Age >= 25)
            .Where("Age <= 30", ctx.User)
            .ToList();
        Assert.Equal(["u1", "u3"], Ids(results));
    }

    [Fact]
    public async Task EscapedQuoteLiteral()
    {
        await this.store.Insert(new User { Id = "q1", Name = "O'Brien", Age = 40 }, ctx.User);
        var results = await this.store.Query(ctx.User).Where("Name == 'O''Brien'", ctx.User).ToList();
        Assert.Equal(["q1"], Ids(results));
    }

    [Fact]
    public async Task NestedPath()
    {
        await this.store.Insert(new Order { Id = "o1", CustomerName = "Acme", Status = "open", ShippingAddress = new Address { City = "Seattle" } }, ctx.Order);
        await this.store.Insert(new Order { Id = "o2", CustomerName = "Beta", Status = "open", ShippingAddress = new Address { City = "Atlanta" } }, ctx.Order);

        var results = await this.store.Query(ctx.Order).Where("ShippingAddress.City == 'Seattle'", ctx.Order).ToList();
        Assert.Single(results);
        Assert.Equal("o1", results[0].Id);
    }

    [Fact]
    public async Task MatchesExpressionOverload()
    {
        await this.SeedUsersAsync();
        var byExpression = await this.store.Query(ctx.User).Where(u => u.Age > 28 && u.Name != "Charlie").ToList();
        var byString = await this.store.Query(ctx.User).Where("Age > 28 and Name != 'Charlie'", ctx.User).ToList();
        Assert.Equal(Ids(byExpression), Ids(byString));
    }

    [Fact]
    public void RelationalOnStringThrows()
        => Assert.Throws<ArgumentException>(() => this.store.Query(ctx.User).Where("Name > 'A'", ctx.User));

    [Fact]
    public void UnknownFieldThrows()
        => Assert.Throws<ArgumentException>(() => this.store.Query(ctx.User).Where("Nope == 1", ctx.User));

    [Fact]
    public void SyntaxErrorThrows()
        => Assert.Throws<ArgumentException>(() => this.store.Query(ctx.User).Where("Age >", ctx.User));

    [Fact]
    public void NullFilterThrows()
        => Assert.Throws<ArgumentNullException>(() => this.store.Query(ctx.User).Where(null!, ctx.User));

    [Fact]
    public void EmptyOrWhitespaceFilterThrows()
    {
        Assert.Throws<ArgumentException>(() => this.store.Query(ctx.User).Where("", ctx.User));
        Assert.Throws<ArgumentException>(() => this.store.Query(ctx.User).Where("   ", ctx.User));
    }

    [Fact]
    public async Task OmittedTypeInfo_ResolvesFromQuery()
    {
        await this.SeedUsersAsync();
        // The query already carries ctx.User, so the JsonTypeInfo can be omitted.
        var results = await this.store.Query(ctx.User).Where("Age > 28").ToList();
        Assert.Equal(["u2", "u3"], Ids(results));
    }
}
