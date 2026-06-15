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

    [Fact]
    public async Task Interpolated_NumericValue()
    {
        await this.SeedUsersAsync();
        var min = 28;
        var results = await this.store.Query(ctx.User).Where($"Age > {min}", ctx.User).ToList();
        Assert.Equal(["u2", "u3"], Ids(results));
    }

    [Fact]
    public async Task Interpolated_StringValue_NeedsNoQuotes()
    {
        await this.SeedUsersAsync();
        var name = "Alice";
        var results = await this.store.Query(ctx.User).Where($"Name == {name}", ctx.User).ToList();
        Assert.Equal(["u1"], Ids(results));
    }

    [Fact]
    public async Task Interpolated_MultipleHoles()
    {
        await this.SeedUsersAsync();
        var lo = 25;
        var hi = 30;
        var results = await this.store.Query(ctx.User).Where($"Age >= {lo} and Age <= {hi}", ctx.User).ToList();
        Assert.Equal(["u1", "u3"], Ids(results));
    }

    [Fact]
    public async Task Interpolated_InList()
    {
        await this.SeedUsersAsync();
        var a = "Alice";
        var c = "Charlie";
        var results = await this.store.Query(ctx.User).Where($"Name in ({a}, {c})", ctx.User).ToList();
        Assert.Equal(["u1", "u3"], Ids(results));
    }

    [Fact]
    public async Task Interpolated_StringFunctionArgument()
    {
        await this.SeedUsersAsync();
        var fragment = "alice";
        var results = await this.store.Query(ctx.User).Where($"contains(Email, {fragment})", ctx.User).ToList();
        Assert.Equal(["u1"], Ids(results));
    }

    [Fact]
    public async Task Interpolated_NullValue_BecomesIsNull()
    {
        await this.SeedUsersAsync();
        string? missing = null;
        var results = await this.store.Query(ctx.User).Where($"Email == {missing}", ctx.User).ToList();
        Assert.Equal(["u2", "u3"], Ids(results));
    }

    [Fact]
    public async Task Interpolated_ValueWithQuote_IsLiteralNotEscaped()
    {
        await this.store.Insert(new User { Id = "q1", Name = "O'Brien", Age = 40 }, ctx.User);
        var name = "O'Brien";
        var results = await this.store.Query(ctx.User).Where($"Name == {name}", ctx.User).ToList();
        Assert.Equal(["q1"], Ids(results));
    }

    [Fact]
    public async Task Interpolated_ValueIsNotParsedAsFilterSyntax()
    {
        await this.SeedUsersAsync();
        // A classic injection attempt: the whole value is treated as a literal, matching no one.
        var name = "Alice' or Age > '0";
        var results = await this.store.Query(ctx.User).Where($"Name == {name}", ctx.User).ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Interpolated_MatchesRawStringOverload()
    {
        await this.SeedUsersAsync();
        var byRaw = await this.store.Query(ctx.User).Where("Age > 28 and Name != 'Charlie'", ctx.User).ToList();
        var threshold = 28;
        var excluded = "Charlie";
        var byInterpolated = await this.store.Query(ctx.User).Where($"Age > {threshold} and Name != {excluded}", ctx.User).ToList();
        Assert.Equal(Ids(byRaw), Ids(byInterpolated));
    }

    [Fact]
    public async Task Interpolated_NestedPathValue()
    {
        await this.store.Insert(new Order { Id = "o1", CustomerName = "Acme", Status = "open", ShippingAddress = new Address { City = "Seattle" } }, ctx.Order);
        await this.store.Insert(new Order { Id = "o2", CustomerName = "Beta", Status = "open", ShippingAddress = new Address { City = "Atlanta" } }, ctx.Order);

        var city = "Seattle";
        var results = await this.store.Query(ctx.Order).Where($"ShippingAddress.City == {city}", ctx.Order).ToList();
        Assert.Single(results);
        Assert.Equal("o1", results[0].Id);
    }

    [Fact]
    public async Task Interpolated_OmittedTypeInfo_ResolvesFromQuery()
    {
        await this.SeedUsersAsync();
        var min = 28;
        var results = await this.store.Query(ctx.User).Where($"Age > {min}").ToList();
        Assert.Equal(["u2", "u3"], Ids(results));
    }

    // ── Scalar functions in the string DSL ───────────────────────────────

    [Fact]
    public async Task StringFunctions()
    {
        await this.SeedUsersAsync();
        Assert.Equal(["u1"], Ids(await this.store.Query(ctx.User).Where("lower(name) = 'alice'", ctx.User).ToList()));
        Assert.Equal(["u1"], Ids(await this.store.Query(ctx.User).Where("length(name) = 5", ctx.User).ToList()));
        Assert.Equal(["u1"], Ids(await this.store.Query(ctx.User).Where("substring(name, 0, 2) = 'Al'", ctx.User).ToList()));
        Assert.Equal(["u2"], Ids(await this.store.Query(ctx.User).Where("upper(name) = 'BOB'", ctx.User).ToList()));
    }

    [Fact]
    public async Task FlagFunction()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "X", Permissions = Permissions.Read | Permissions.Write }, ctx.Account);
        await this.store.Insert(new Account { Id = "a2", Name = "Y", Permissions = Permissions.Read }, ctx.Account);

        var r = await this.store.Query(ctx.Account).Where("hasflag(permissions, 'Write')", ctx.Account).ToList();
        Assert.Equal(["a1"], r.Select(a => a.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task SoundexFunction()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "Smith" }, ctx.Account);
        await this.store.Insert(new Account { Id = "a2", Name = "Smyth" }, ctx.Account);
        await this.store.Insert(new Account { Id = "a3", Name = "Jones" }, ctx.Account);

        var r = await this.store.Query(ctx.Account).Where("soundex(name) = soundex('Smith')", ctx.Account).ToList();
        Assert.Equal(["a1", "a2"], r.Select(a => a.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task DatePartFunction()
    {
        await this.store.Insert(new Event { Id = "e1", Title = "A", StartDate = new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc) }, ctx.Event);
        await this.store.Insert(new Event { Id = "e2", Title = "B", StartDate = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc) }, ctx.Event);

        var r = await this.store.Query(ctx.Event).Where("year(startDate) = 2026", ctx.Event).ToList();
        Assert.Equal(["e1"], r.Select(e => e.Id).ToList());
    }
}
