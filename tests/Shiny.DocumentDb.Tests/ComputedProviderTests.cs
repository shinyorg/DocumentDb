using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Shared computed-property behaviour run against every provider via its fixture's <c>CreateComputedStore</c>.
/// Read-back and projection are validated everywhere; server-side filter/sort by a computed property is
/// validated only where <see cref="SupportsComputedQuery"/> (the relational providers — and there the numeric
/// computed is materialized as a native generated/computed column + index).
/// </summary>
public abstract class ComputedProviderTestsBase
{
    protected abstract IDocumentStore CreateStore(string tableName);

    /// <summary>True when the backend translates <c>Where</c>/<c>OrderBy</c> over a computed property to the
    /// server (relational). MongoDB/Cosmos populate on read but don't translate computed predicates.</summary>
    protected virtual bool SupportsComputedQuery => true;

    // Ids are GUID-prefixed and unique per test: the Mongo/Cosmos bulk-backup round-trip exports the whole
    // shared database, so reused ids across collections would collide there. Queries stay scoped to the
    // store's own collection, so assertions use the (per-store) First name rather than the id.
    async Task<IDocumentStore> SeedAsync(string name)
    {
        var store = this.CreateStore(name);
        var k = Guid.NewGuid().ToString("N");
        await store.Insert(new ComputedSale { Id = $"{k}-1", First = "Jane", Last = "Doe", Quantity = 2, UnitPriceCents = 500 });  // 1000
        await store.Insert(new ComputedSale { Id = $"{k}-2", First = "John", Last = "Roe", Quantity = 1, UnitPriceCents = 300 });  // 300
        await store.Insert(new ComputedSale { Id = $"{k}-3", First = "Amy", Last = "Poe", Quantity = 5, UnitPriceCents = 250 });   // 1250
        return store;
    }

    [Fact]
    public async Task ReadBack_PopulatesComputed()
    {
        var store = await this.SeedAsync($"cc_read_{Guid.NewGuid():N}");
        var all = await store.Query<ComputedSale>().ToList();
        var one = all.Single(s => s.First == "Jane");

        Assert.Equal("Jane Doe", one.FullName);
        Assert.Equal(1000, one.LineTotalCents);
    }

    [Fact]
    public async Task Project_ByComputedName()
    {
        var store = await this.SeedAsync($"cc_proj_{Guid.NewGuid():N}");
        var rows = await store.Query<ComputedSale>().Project("fullName as fn, lineTotalCents as lt").ToList();

        Assert.Equal(3, rows.Count);
        var jane = rows.Single(r => (string)r["fn"]! == "Jane Doe");
        Assert.Equal(1000, (int)jane["lt"]!);
    }

    [Fact]
    public async Task Filter_ByComputed_Numeric()
    {
        if (!this.SupportsComputedQuery)
            Assert.Skip("Provider does not translate server-side filters over computed properties.");

        var store = await this.SeedAsync($"cc_filter_{Guid.NewGuid():N}");
        var results = await store.Query<ComputedSale>().Where(s => s.LineTotalCents > 500).ToList();

        Assert.Equal(["Amy", "Jane"], results.Select(s => s.First).OrderBy(x => x));
    }

    [Fact]
    public async Task OrderBy_ByComputed_Numeric()
    {
        if (!this.SupportsComputedQuery)
            Assert.Skip("Provider does not translate server-side ordering over computed properties.");

        var store = await this.SeedAsync($"cc_order_{Guid.NewGuid():N}");
        var results = await store.Query<ComputedSale>().OrderByDescending(s => s.LineTotalCents).ToList();

        Assert.Equal(["Amy", "Jane", "John"], results.Select(s => s.First).ToArray());
    }

    [Fact]
    public async Task Filter_ByComputed_StringConcat()
    {
        if (!this.SupportsComputedQuery)
            Assert.Skip("Provider does not translate server-side filters over computed properties.");

        var store = await this.SeedAsync($"cc_concat_{Guid.NewGuid():N}");
        var results = await store.Query<ComputedSale>().Where(s => s.FullName == "Jane Doe").ToList();

        Assert.Single(results);
        Assert.Equal("Jane", results[0].First);
    }
}

[Collection("DuckDB")]
public class DuckDbComputedTests(DuckDbDatabaseFixture fx) : ComputedProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateComputedStore(tableName);
}

[Collection("PostgreSQL")]
public class PostgreSqlComputedTests(PostgreSqlDatabaseFixture fx) : ComputedProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateComputedStore(tableName);
}

[Collection("MySQL")]
public class MySqlComputedTests(MySqlDatabaseFixture fx) : ComputedProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateComputedStore(tableName);
}

[Collection("MSSQL")]
public class SqlServerComputedTests(MsSqlDatabaseFixture fx) : ComputedProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateComputedStore(tableName);
}

[Collection("Oracle")]
public class OracleComputedTests(OracleDatabaseFixture fx) : ComputedProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateComputedStore(tableName);
}

[Collection("MongoDB")]
public class MongoDbComputedTests(MongoDbDatabaseFixture fx) : ComputedProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateComputedStore(tableName);
    protected override bool SupportsComputedQuery => false;
}

[Collection("CosmosDB")]
public class CosmosDbComputedTests(CosmosDbDatabaseFixture fx) : ComputedProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateComputedStore(tableName);
    protected override bool SupportsComputedQuery => false;
}
