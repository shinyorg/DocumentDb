using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// String scalar functions and flag-enum querying over the relational IR/dialect pipeline. Runs against
/// any provider whose dialect supplies (or defaults) the translations — currently SQLite and DuckDB.
/// </summary>
public abstract class ScalarFunctionTestsBase : IDisposable
{
    protected readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected ScalarFunctionTestsBase(IDatabaseFixture fixture)
        => this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        });

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    async Task SeedAsync()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "Alice", Permissions = Permissions.Read | Permissions.Write }, ctx.Account);
        await this.store.Insert(new Account { Id = "a2", Name = "Bob", Permissions = Permissions.Read }, ctx.Account);
        await this.store.Insert(new Account { Id = "a3", Name = "Smith", Permissions = Permissions.Admin }, ctx.Account);
        await this.store.Insert(new Account { Id = "a4", Name = "  Trimmed  ", Permissions = Permissions.None }, ctx.Account);
        await this.store.Insert(new Account { Id = "a5", Name = "", Permissions = Permissions.None }, ctx.Account);
    }

    async Task<List<string>> NamesWhere(System.Linq.Expressions.Expression<Func<Account, bool>> predicate)
    {
        var results = await this.store.Query(ctx.Account).Where(predicate).ToList();
        return results.Select(a => a.Id).OrderBy(x => x).ToList();
    }

    [Fact]
    public async Task ToLower()
    {
        await this.SeedAsync();
        Assert.Equal(["a1"], await this.NamesWhere(a => a.Name.ToLower() == "alice"));
    }

    [Fact]
    public async Task ToUpper()
    {
        await this.SeedAsync();
        Assert.Equal(["a2"], await this.NamesWhere(a => a.Name.ToUpper() == "BOB"));
    }

    [Fact]
    public async Task Length()
    {
        await this.SeedAsync();
        Assert.Equal(["a1", "a3"], await this.NamesWhere(a => a.Name.Length == 5));
    }

    [Fact]
    public async Task Substring()
    {
        await this.SeedAsync();
        Assert.Equal(["a1"], await this.NamesWhere(a => a.Name.Substring(0, 2) == "Al"));
    }

    [Fact]
    public async Task Replace()
    {
        await this.SeedAsync();
        Assert.Equal(["a1"], await this.NamesWhere(a => a.Name.Replace("i", "y") == "Alyce"));
    }

    [Fact]
    public async Task IndexOf()
    {
        await this.SeedAsync();
        // "Bob".IndexOf("ob") == 1
        Assert.Equal(["a2"], await this.NamesWhere(a => a.Name.IndexOf("ob") == 1));
    }

    [Fact]
    public async Task Trim()
    {
        await this.SeedAsync();
        Assert.Equal(["a4"], await this.NamesWhere(a => a.Name.Trim() == "Trimmed"));
    }

    [Fact]
    public async Task IsNullOrEmpty()
    {
        await this.SeedAsync();
        Assert.Equal(["a5"], await this.NamesWhere(a => string.IsNullOrEmpty(a.Name)));
    }

    [Fact]
    public async Task LoweredContains()
    {
        await this.SeedAsync();
        // ToLower then Contains composes the scalar fn under the LIKE.
        Assert.Equal(["a3"], await this.NamesWhere(a => a.Name.ToLower().Contains("mit")));
    }

    [Fact]
    public async Task HasFlag()
    {
        await this.SeedAsync();
        // Read|Write (a1) and Admin (a3) both contain Write.
        Assert.Equal(["a1", "a3"], await this.NamesWhere(a => a.Permissions.HasFlag(Permissions.Write)));
    }

    [Fact]
    public async Task HasFlag_SingleBit()
    {
        await this.SeedAsync();
        Assert.Equal(["a3"], await this.NamesWhere(a => a.Permissions.HasFlag(Permissions.Delete)));
    }

    [Fact]
    public async Task Bitwise_And_Equals()
    {
        await this.SeedAsync();
        // (x & Write) == Write — same result set as HasFlag(Write).
        Assert.Equal(["a1", "a3"], await this.NamesWhere(a => (a.Permissions & Permissions.Write) == Permissions.Write));
    }

    [Fact]
    public async Task Math_Round()
    {
        await this.store.Insert(new Product { Id = "p1", Title = "A", Price = 9.99m }, ctx.Product);
        await this.store.Insert(new Product { Id = "p2", Title = "B", Price = 24.20m }, ctx.Product);

        var results = await this.store.Query(ctx.Product).Where(p => (int)Math.Round(p.Price) == 10).ToList();
        Assert.Equal(["p1"], results.Select(p => p.Id));
    }

    [Fact]
    public async Task Math_Abs()
    {
        await this.store.Insert(new Product { Id = "p1", Title = "A", Price = 5m }, ctx.Product);
        await this.store.Insert(new Product { Id = "p2", Title = "B", Price = 20m }, ctx.Product);

        var results = await this.store.Query(ctx.Product).Where(p => (int)Math.Abs(p.Price) > 10).ToList();
        Assert.Equal(["p2"], results.Select(p => p.Id));
    }

    [Fact]
    public async Task DateTime_Year()
    {
        await this.store.Insert(new Event { Id = "e1", Title = "Old", StartDate = new DateTime(2024, 3, 1, 9, 0, 0) }, ctx.Event);
        await this.store.Insert(new Event { Id = "e2", Title = "New", StartDate = new DateTime(2026, 7, 15, 14, 30, 0) }, ctx.Event);

        var results = await this.store.Query(ctx.Event).Where(e => e.StartDate.Year == 2026).ToList();
        Assert.Equal(["e2"], results.Select(e => e.Id));
    }

    [Fact]
    public async Task DateTime_Month()
    {
        await this.store.Insert(new Event { Id = "e1", Title = "A", StartDate = new DateTime(2026, 3, 1, 9, 0, 0) }, ctx.Event);
        await this.store.Insert(new Event { Id = "e2", Title = "B", StartDate = new DateTime(2026, 7, 15, 14, 30, 0) }, ctx.Event);

        var results = await this.store.Query(ctx.Event).Where(e => e.StartDate.Month == 7).ToList();
        Assert.Equal(["e2"], results.Select(e => e.Id));
    }
}
