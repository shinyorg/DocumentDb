using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Exercises the compile-free <c>ExpressionInterpreter</c> on the new query mechanics. The in-memory
/// providers (LiteDB/IndexedDB) evaluate predicates by walking the expression tree — these tests confirm
/// scalar functions, flag enums, math, dates, concat, and DocumentFunctions all evaluate correctly there.
/// </summary>
[Collection("LiteDB")]
public class InMemoryScalarTests(LiteDbDatabaseFixture db) : IDisposable
{
    readonly IDocumentStore store = db.CreateStore($"t{Guid.NewGuid():N}");

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    async Task SeedAccountsAsync()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "Alice", Permissions = Permissions.Read | Permissions.Write });
        await this.store.Insert(new Account { Id = "a2", Name = "Bob", Permissions = Permissions.Read });
        await this.store.Insert(new Account { Id = "a3", Name = "Smith", Permissions = Permissions.Admin });
    }

    async Task<List<string>> Ids(System.Linq.Expressions.Expression<Func<Account, bool>> p)
        => (await this.store.Query<Account>().Where(p).ToList()).Select(a => a.Id).OrderBy(x => x).ToList();

    [Fact]
    public async Task StringFunctions()
    {
        await this.SeedAccountsAsync();
        Assert.Equal(["a1"], await this.Ids(a => a.Name.ToLower() == "alice"));
        Assert.Equal(["a2"], await this.Ids(a => a.Name.ToUpper() == "BOB"));
        Assert.Equal(["a1", "a3"], await this.Ids(a => a.Name.Length == 5));
        Assert.Equal(["a1"], await this.Ids(a => a.Name.Substring(0, 2) == "Al"));
        Assert.Equal(["a1"], await this.Ids(a => a.Name.Replace("i", "y") == "Alyce"));
        Assert.Equal(["a1"], await this.Ids(a => a.Name + "!" == "Alice!"));
    }

    [Fact]
    public async Task IsNullOrEmpty()
    {
        // The in-memory interpreter runs real .NET methods, so it matches BCL semantics exactly.
        await this.store.Insert(new Account { Id = "full", Name = "Alice" });
        await this.store.Insert(new Account { Id = "empty", Name = "" });
        Assert.Equal(["empty"], await this.Ids(a => string.IsNullOrEmpty(a.Name)));
    }

    [Fact]
    public async Task FlagEnums()
    {
        await this.SeedAccountsAsync();
        Assert.Equal(["a1", "a3"], await this.Ids(a => a.Permissions.HasFlag(Permissions.Write)));
        Assert.Equal(["a3"], await this.Ids(a => a.Permissions.HasFlag(Permissions.Delete)));
        Assert.Equal(["a1", "a3"], await this.Ids(a => (a.Permissions & Permissions.Write) == Permissions.Write));
    }

    [Fact]
    public async Task Soundex()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "Smith" });
        await this.store.Insert(new Account { Id = "a2", Name = "Smyth" });
        await this.store.Insert(new Account { Id = "a3", Name = "Jones" });

        var r = await this.store.Query<Account>()
            .Where(a => DocumentFunctions.Soundex(a.Name) == DocumentFunctions.Soundex("Smith"))
            .ToList();
        Assert.Equal(["a1", "a2"], r.Select(a => a.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task MathAndDate()
    {
        await this.store.Insert(new Product { Id = "p1", Title = "A", Price = 9.8m });
        await this.store.Insert(new Product { Id = "p2", Title = "B", Price = -4m });

        Assert.Equal(["p1"], (await this.store.Query<Product>().Where(p => (int)Math.Ceiling(p.Price) == 10).ToList()).Select(p => p.Id).ToList());
        Assert.Equal(["p2"], (await this.store.Query<Product>().Where(p => Math.Sign(p.Price) == -1).ToList()).Select(p => p.Id).ToList());

        await this.store.Insert(new Event { Id = "e1", Title = "X", StartDate = new DateTime(2026, 7, 15, 14, 0, 0) });
        Assert.Equal(["e1"], (await this.store.Query<Event>().Where(e => e.StartDate.Year == 2026 && e.StartDate.Month == 7).ToList()).Select(e => e.Id).ToList());
    }
}
