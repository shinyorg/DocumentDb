using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.LiteDb;

// Exercises the in-memory predicate.Compile() path (LiteDB / IndexedDB) for the boxed
// Enumerable.Contains<object> lowering produced by WhereIn/WhereNotIn — a distinct code path from the
// relational JsonExpressionVisitor covered by ExpressionQueryTestsBase.
[Collection("LiteDB")]
public class WhereInInMemoryTests(LiteDbDatabaseFixture db) : IDisposable
{
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
    readonly IDocumentStore store = db.CreateStore($"t{Guid.NewGuid():N}");

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    async Task Seed()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25, Email = "alice@test.com" }, ctx.User);
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 35 }, ctx.User);
        await this.store.Insert(new User { Id = "u3", Name = "Charlie", Age = 25 }, ctx.User);
    }

    [Fact]
    public async Task WhereIn_Strings()
    {
        await this.Seed();
        var results = await this.store.Query(ctx.User).WhereIn(u => u.Name, ["Alice", "Charlie"]).ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task WhereIn_Ints_BoxedEquality()
    {
        await this.Seed();
        var results = await this.store.Query(ctx.User).WhereIn(u => u.Age, [25]).ToList();
        Assert.Equal(2, results.Count); // boxed int equality must hold across the Compile path
    }

    [Fact]
    public async Task WhereNotIn_ExcludesMatches()
    {
        await this.Seed();
        var results = await this.store.Query(ctx.User).WhereNotIn(u => u.Name, ["Bob"]).ToList();
        Assert.Equal(2, results.Count);
        Assert.DoesNotContain(results, u => u.Name == "Bob");
    }

    [Fact]
    public async Task WhereIn_NullMatch_IncludesNullFields()
    {
        await this.Seed();
        var results = await this.store.Query(ctx.User)
            .WhereIn(u => u.Email, ["alice@test.com", null], NullHandling.Match)
            .ToList();
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task WhereIn_NullIgnore_SkipsNulls()
    {
        await this.Seed();
        var results = await this.store.Query(ctx.User)
            .WhereIn(u => u.Email, ["alice@test.com", null], NullHandling.Ignore)
            .ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task WhereIn_EmptySet_MatchesNothing()
    {
        await this.Seed();
        var results = await this.store.Query(ctx.User).WhereIn(u => u.Name, Array.Empty<string>()).ToList();
        Assert.Empty(results);
    }
}
