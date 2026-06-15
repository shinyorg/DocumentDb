using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Scalar string/flag function translation on CosmosDB through the shared IR + CosmosSqlEmitter, now that
/// documents are stored as nested JSON objects (so <c>c.data.*</c> paths resolve).
/// </summary>
[Collection("CosmosDB")]
public class CosmosScalarTests(CosmosDbDatabaseFixture db) : IDisposable
{
    readonly IDocumentStore store = db.CreateStore($"t{Guid.NewGuid():N}");

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    async Task SeedAsync()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "Alice", Permissions = Permissions.Read | Permissions.Write });
        await this.store.Insert(new Account { Id = "a2", Name = "Bob", Permissions = Permissions.Read });
        await this.store.Insert(new Account { Id = "a3", Name = "Smith", Permissions = Permissions.Admin });
    }

    async Task<List<string>> Ids(System.Linq.Expressions.Expression<Func<Account, bool>> p)
        => (await this.store.Query<Account>().Where(p).ToList()).Select(a => a.Id).OrderBy(x => x).ToList();

    [Fact]
    public async Task ToLower()
    {
        await this.SeedAsync();
        Assert.Equal(["a1"], await this.Ids(a => a.Name.ToLower() == "alice"));
    }

    [Fact]
    public async Task Length()
    {
        await this.SeedAsync();
        Assert.Equal(["a1", "a3"], await this.Ids(a => a.Name.Length == 5));
    }

    [Fact]
    public async Task Substring()
    {
        await this.SeedAsync();
        Assert.Equal(["a1"], await this.Ids(a => a.Name.Substring(0, 2) == "Al"));
    }

    [Fact]
    public async Task HasFlag()
    {
        await this.SeedAsync();
        Assert.Equal(["a1", "a3"], await this.Ids(a => a.Permissions.HasFlag(Permissions.Write)));
    }

    [Fact]
    public async Task Math_Abs()
    {
        await this.store.Insert(new Product { Id = "p1", Title = "A", Price = 5m });
        await this.store.Insert(new Product { Id = "p2", Title = "B", Price = 20m });
        var r = await this.store.Query<Product>().Where(p => (int)Math.Abs(p.Price) > 10).ToList();
        Assert.Equal(["p2"], r.Select(p => p.Id));
    }

    [Fact]
    public async Task DateTime_Year()
    {
        await this.store.Insert(new Event { Id = "e1", Title = "A", StartDate = new DateTime(2024, 3, 1, 0, 0, 0, DateTimeKind.Utc) });
        await this.store.Insert(new Event { Id = "e2", Title = "B", StartDate = new DateTime(2026, 7, 15, 0, 0, 0, DateTimeKind.Utc) });
        var r = await this.store.Query<Event>().Where(e => e.StartDate.Year == 2026).ToList();
        Assert.Equal(["e2"], r.Select(e => e.Id));
    }
}
