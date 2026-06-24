using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Shared full-text search behaviour, run against every relational provider via its fixture's
/// <c>CreateFullTextStore</c>. The title/body corpus is identical to the SQLite and LiteDB suites so
/// ranking and field coverage are validated the same way across backends.
/// </summary>
public abstract class FullTextProviderTestsBase
{
    protected abstract IDocumentStore CreateStore(string tableName);

    /// <summary>
    /// Overridden by providers whose backing engine ships full-text as an optional, sometimes-absent
    /// feature (Oracle Text, SQL Server Full-Text Search). Calls <see cref="Assert.Skip"/> when the
    /// feature is not installed in the current environment so CI stays green on slim container images.
    /// </summary>
    protected virtual Task EnsureAvailableAsync() => Task.CompletedTask;

    async Task<IDocumentStore> SeedAsync(IDocumentStore store)
    {
        foreach (var d in FullTextTestSeed.Docs)
            await store.Insert(d);
        return store;
    }

    [Fact]
    public async Task SupportsFullText_IsTrue()
    {
        await this.EnsureAvailableAsync();
        Assert.True(this.CreateStore($"fts_supports_{Guid.NewGuid():N}").SupportsFullText);
    }

    [Fact]
    public async Task FullTextSearch_FindsMatches()
    {
        await this.EnsureAvailableAsync();
        var store = await SeedAsync(this.CreateStore($"fts_find_{Guid.NewGuid():N}"));
        var results = await store.FullTextSearch<FtArticle>("orleans");

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Contains("orleans", (r.Document.Title + " " + r.Document.Body).ToLowerInvariant()));
    }

    [Fact]
    public async Task FullTextSearch_RanksByRelevance_DescendingScore()
    {
        await this.EnsureAvailableAsync();
        var store = await SeedAsync(this.CreateStore($"fts_rank_{Guid.NewGuid():N}"));
        var results = await store.FullTextSearch<FtArticle>("orleans");

        Assert.Equal(2, results.Count);
        Assert.True(results[0].Score >= results[1].Score);
    }

    [Fact]
    public async Task FullTextSearch_SearchesBodyField()
    {
        await this.EnsureAvailableAsync();
        var store = await SeedAsync(this.CreateStore($"fts_body_{Guid.NewGuid():N}"));
        var results = await store.FullTextSearch<FtArticle>("garlic");

        Assert.Single(results);
        Assert.Equal("2", results[0].Document.Id);
    }

    [Fact]
    public async Task FullTextSearch_NoMatches_ReturnsEmpty()
    {
        await this.EnsureAvailableAsync();
        var store = await SeedAsync(this.CreateStore($"fts_none_{Guid.NewGuid():N}"));
        Assert.Empty(await store.FullTextSearch<FtArticle>("kubernetes"));
    }

    [Fact]
    public async Task FullTextSearch_WithPreFilter()
    {
        await this.EnsureAvailableAsync();
        var store = await SeedAsync(this.CreateStore($"fts_filter_{Guid.NewGuid():N}"));
        var results = await store.FullTextSearch<FtArticle>("orleans", filter: a => a.Category == "tech");

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("tech", r.Document.Category));
    }

    [Fact]
    public async Task FullTextSearch_MaxResults_LimitsCount()
    {
        await this.EnsureAvailableAsync();
        var store = await SeedAsync(this.CreateStore($"fts_max_{Guid.NewGuid():N}"));
        Assert.Single(await store.FullTextSearch<FtArticle>("orleans", maxResults: 1));
    }

    [Fact]
    public async Task FullTextSearch_ViaFluentQuery()
    {
        await this.EnsureAvailableAsync();
        var store = await SeedAsync(this.CreateStore($"fts_fluent_{Guid.NewGuid():N}"));
        var results = await store.Query<FtArticle>().Where(a => a.Category == "tech").FullTextMatch("orleans");

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("tech", r.Document.Category));
    }
}

[Collection("DuckDB")]
public class DuckDbFullTextTests(DuckDbDatabaseFixture fx) : FullTextProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateFullTextStore(tableName);
}

[Collection("PostgreSQL")]
public class PostgreSqlFullTextTests(PostgreSqlDatabaseFixture fx) : FullTextProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateFullTextStore(tableName);
}

[Collection("MySQL")]
public class MySqlFullTextTests(MySqlDatabaseFixture fx) : FullTextProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateFullTextStore(tableName);
}

[Collection("MongoDB")]
public class MongoDbFullTextTests(MongoDbDatabaseFixture fx) : FullTextProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateFullTextStore(tableName);
}

[Collection("CosmosDB")]
public class CosmosDbFullTextTests(CosmosDbDatabaseFixture fx) : FullTextProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateFullTextStore(tableName);

    // Cosmos full-text search is a newer service feature that the local emulator may not implement.
    protected override async Task EnsureAvailableAsync()
    {
        try
        {
            var store = this.CreateStore($"fts_probe_{Guid.NewGuid():N}");
            await store.Insert(new FtArticle { Id = "p", Title = "probe", Body = "probe", Category = "p" });
            await store.FullTextSearch<FtArticle>("probe");
        }
        catch (Exception ex) when (ex.ToString().Contains("full", StringComparison.OrdinalIgnoreCase)
                                   || ex.ToString().Contains("FullText", StringComparison.OrdinalIgnoreCase))
        {
            Assert.Skip("Cosmos DB full-text search is not supported by this emulator/environment.");
        }
    }
}

[Collection("Oracle")]
public class OracleFullTextTests(OracleDatabaseFixture fx) : FullTextProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateFullTextStore(tableName);

    // Oracle Text (CTXSYS.CONTEXT) is an optional component absent from slim container images.
    protected override async Task EnsureAvailableAsync()
    {
        await using var conn = fx.CreateProvider().CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM all_indextypes WHERE indextype_name = 'CONTEXT' AND owner = 'CTXSYS'";
        var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
        if (count == 0)
            Assert.Skip("Oracle Text (CTXSYS.CONTEXT) is not installed in this environment.");
    }
}

[Collection("MSSQL")]
public class SqlServerFullTextTests(MsSqlDatabaseFixture fx) : FullTextProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateFullTextStore(tableName);

    // SQL Server Full-Text Search is an optional feature absent from the default container image.
    protected override async Task EnsureAvailableAsync()
    {
        await using var conn = fx.CreateProvider().CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT CAST(ISNULL(SERVERPROPERTY('IsFullTextInstalled'), 0) AS INT)";
        var installed = Convert.ToInt32(await cmd.ExecuteScalarAsync());
        if (installed == 0)
            Assert.Skip("SQL Server Full-Text Search is not installed in this environment.");
    }
}
