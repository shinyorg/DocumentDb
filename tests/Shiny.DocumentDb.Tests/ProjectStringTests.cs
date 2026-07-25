using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class ProjectStringTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected ProjectStringTestsBase(IDatabaseFixture fixture)
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

    [Fact]
    public async Task StringGrammar_Pow_Round_Concat_Parity()
    {
        // Parity with LINQ Math.Pow / Math.Round(x,n) / string concat, now reachable from the string grammar.
        await this.SeedUsersAsync();

        var pow = await this.store.Query(ctx.User).Where("pow(age, 2) > 1000", ctx.User).Project("name", ctx.User).ToList();
        Assert.Equal(new[] { "Bob" }, pow.Select(r => r["name"]!.GetValue<string>()).ToArray());

        var concat = await this.store.Query(ctx.User).Where("concat(name, '!') == 'Alice!'", ctx.User).Project("name", ctx.User).ToList();
        Assert.Equal(new[] { "Alice" }, concat.Select(r => r["name"]!.GetValue<string>()).ToArray());

        var round = await this.store.Query(ctx.User).Where("round(age, 0) > 26", ctx.User).Project("name", ctx.User).ToList();
        Assert.Equal(new[] { "Bob", "Charlie" }, round.Select(r => r["name"]!.GetValue<string>()).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task SelectsNamedFields()
    {
        await this.SeedUsersAsync();
        var rows = await this.store.Query(ctx.User)
            .Where("Name == 'Alice'", ctx.User)
            .Project("Name, Email", ctx.User)
            .ToList();

        var row = Assert.Single(rows);
        Assert.Equal("Alice", row["name"]!.GetValue<string>());
        Assert.Equal("alice@test.com", row["email"]!.GetValue<string>());
        // Only the requested keys are present.
        Assert.Equal(2, row.Count);
        Assert.DoesNotContain("age", (IDictionary<string, JsonNode?>)row);
    }

    [Fact]
    public async Task SingleField()
    {
        await this.SeedUsersAsync();
        var rows = await this.store.Query(ctx.User)
            .OrderBy("Age", ctx.User)
            .Project("Age", ctx.User)
            .ToList();

        Assert.Equal([25, 30, 35], rows.Select(r => r["age"]!.GetValue<int>()).ToArray());
    }

    [Fact]
    public async Task JsonNameAndCaseInsensitive()
    {
        await this.SeedUsersAsync();
        var rows = await this.store.Query(ctx.User)
            .Where("Name == 'Bob'", ctx.User)
            .Project("name, AGE", ctx.User)
            .ToList();

        var row = Assert.Single(rows);
        Assert.Equal("Bob", row["name"]!.GetValue<string>());
        Assert.Equal(35, row["age"]!.GetValue<int>());
    }

    [Fact]
    public async Task NestedPathFlattensToLeafName()
    {
        await this.store.Insert(new Order { Id = "o1", CustomerName = "Acme", Status = "open", ShippingAddress = new Address { City = "Seattle" } }, ctx.Order);

        var rows = await this.store.Query(ctx.Order)
            .Project("CustomerName, ShippingAddress.City", ctx.Order)
            .ToList();

        var row = Assert.Single(rows);
        Assert.Equal("Acme", row["customerName"]!.GetValue<string>());
        Assert.Equal("Seattle", row["city"]!.GetValue<string>());
    }

    [Fact]
    public async Task RespectsWhereAndOrderBy()
    {
        await this.SeedUsersAsync();
        var rows = await this.store.Query(ctx.User)
            .Where("Age >= 30", ctx.User)
            .OrderByDescending("Age", ctx.User)
            .Project("Name", ctx.User)
            .ToList();

        Assert.Equal(["Bob", "Charlie"], rows.Select(r => r["name"]!.GetValue<string>()).ToArray());
    }

    [Fact]
    public async Task PageResultWorks()
    {
        await this.SeedUsersAsync();
        var page = await this.store.Query(ctx.User)
            .OrderBy("Age", ctx.User)
            .Project("Name", ctx.User)
            .PageResult(1, 2);

        Assert.Equal(3, page.TotalCount);
        Assert.Equal(2, page.Records.Count());
        Assert.Equal(["Alice", "Charlie"], page.Records.Select(r => r["name"]!.GetValue<string>()).ToArray());
    }

    [Fact]
    public async Task CountAndAny()
    {
        await this.SeedUsersAsync();
        var projected = this.store.Query(ctx.User).Where("Age > 28", ctx.User).Project("Name", ctx.User);
        Assert.Equal(2, await projected.Count());
        Assert.True(await this.store.Query(ctx.User).Project("Name", ctx.User).Any());
    }

    [Fact]
    public async Task Streaming()
    {
        await this.SeedUsersAsync();
        var names = new List<string>();
        await foreach (var row in this.store.Query(ctx.User).OrderBy("Age", ctx.User).Project("Name", ctx.User).ToAsyncEnumerable())
            names.Add(row["name"]!.GetValue<string>());

        Assert.Equal(["Alice", "Charlie", "Bob"], names);
    }

    [Fact]
    public void DuplicateLeafThrows()
        => Assert.Throws<ArgumentException>(() => this.store.Query(ctx.User).Project("Email, Email", ctx.User));

    [Fact]
    public void UnknownFieldThrows()
        => Assert.Throws<ArgumentException>(() => this.store.Query(ctx.User).Project("Nope", ctx.User));

    [Fact]
    public void EmptyThrows()
    {
        Assert.Throws<ArgumentException>(() => this.store.Query(ctx.User).Project("", ctx.User));
        Assert.Throws<ArgumentException>(() => this.store.Query(ctx.User).Project("  ", ctx.User));
    }

    [Fact]
    public async Task OmittedTypeInfo_ResolvesFromQuery()
    {
        await this.SeedUsersAsync();
        // The query already carries ctx.User, so the JsonTypeInfo can be omitted.
        var rows = await this.store.Query(ctx.User)
            .Where("Name == 'Alice'")
            .Project("Name, Email")
            .ToList();

        var row = Assert.Single(rows);
        Assert.Equal("Alice", row["name"]!.GetValue<string>());
        Assert.Equal("alice@test.com", row["email"]!.GetValue<string>());
    }

    [Fact]
    public void CannotProjectAfterSelect()
    {
        var projected = this.store.Query(ctx.User).Select(u => new UserSummary { Name = u.Name }, ctx.UserSummary);
        Assert.Throws<NotSupportedException>(() => projected.Project("name", ctx.UserSummary));
    }

    // ── Scalar functions in the projection DSL ───────────────────────────

    [Fact]
    public async Task ProjectFunctions_StringAndMixed()
    {
        await this.SeedUsersAsync();
        var rows = await this.store.Query(ctx.User)
            .Where("Name == 'Alice'", ctx.User)
            .Project("name, lower(name) as lname, length(name) as len, substring(name, 0, 2) as init", ctx.User)
            .ToList();

        var row = Assert.Single(rows);
        Assert.Equal("Alice", row["name"]!.GetValue<string>());
        Assert.Equal("alice", row["lname"]!.GetValue<string>());
        Assert.Equal(5, row["len"]!.GetValue<int>());
        Assert.Equal("Al", row["init"]!.GetValue<string>());
    }

    [Fact]
    public async Task ProjectFunction_DatePart()
    {
        await this.store.Insert(new Event { Id = "e1", Title = "A", StartDate = new DateTime(2026, 7, 15, 0, 0, 0, DateTimeKind.Utc) }, ctx.Event);
        var rows = await this.store.Query(ctx.Event)
            .Project("title, year(startDate) as yr, month(startDate) as mo", ctx.Event)
            .ToList();

        var row = Assert.Single(rows);
        Assert.Equal(2026, row["yr"]!.GetValue<int>());
        Assert.Equal(7, row["mo"]!.GetValue<int>());
    }

    [Fact]
    public void ProjectFunction_RequiresAlias()
        => Assert.Throws<ArgumentException>(() => this.store.Query(ctx.User).Project("lower(name)", ctx.User));
}
