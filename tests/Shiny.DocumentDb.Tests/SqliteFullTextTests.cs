using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// Full-text search coverage for the SQLite FTS5 backend (virtual table + sync triggers).
/// </summary>
public class SqliteFullTextTests : IDisposable
{
    sealed class Article
    {
        public string Id { get; set; } = "";
        public string Title { get; set; } = "";
        public string Body { get; set; } = "";
        public string Category { get; set; } = "";
    }

    readonly SqliteConnection holdOpen;
    readonly DocumentStore store;

    public SqliteFullTextTests()
    {
        var connectionString = $"Data Source=fts_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(connectionString);
        this.holdOpen.Open();

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connectionString),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.MapFullTextProperty<Article>([a => a.Title, a => a.Body]);
        this.store = new DocumentStore(opts);
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.holdOpen.Dispose();
    }

    async Task SeedAsync()
    {
        await this.store.Insert(new Article { Id = "1", Title = "Orleans persistence", Body = "Using DocumentDb as an Orleans grain storage provider.", Category = "tech" });
        await this.store.Insert(new Article { Id = "2", Title = "Cooking with garlic", Body = "A recipe that uses a lot of garlic and onion.", Category = "food" });
        await this.store.Insert(new Article { Id = "3", Title = "Distributed systems", Body = "Orleans is a virtual actor framework for distributed applications.", Category = "tech" });
    }

    [Fact]
    public async Task FullTextSearch_FindsMatches()
    {
        await this.SeedAsync();
        var results = await this.store.FullTextSearch<Article>("orleans");

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Contains("Orleans", r.Document.Title + " " + r.Document.Body, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task FullTextSearch_RanksByRelevance_DescendingScore()
    {
        await this.SeedAsync();
        var results = await this.store.FullTextSearch<Article>("orleans");

        Assert.Equal(2, results.Count);
        // Scores are normalized so higher = more relevant; results must be ordered descending.
        Assert.True(results[0].Score >= results[1].Score);
    }

    [Fact]
    public async Task FullTextSearch_SearchesBodyField()
    {
        await this.SeedAsync();
        var results = await this.store.FullTextSearch<Article>("garlic");

        Assert.Single(results);
        Assert.Equal("2", results[0].Document.Id);
    }

    [Fact]
    public async Task FullTextSearch_NoMatches_ReturnsEmpty()
    {
        await this.SeedAsync();
        var results = await this.store.FullTextSearch<Article>("kubernetes");
        Assert.Empty(results);
    }

    [Fact]
    public async Task FullTextSearch_WithPreFilter()
    {
        await this.SeedAsync();
        var results = await this.store.FullTextSearch<Article>("orleans", filter: a => a.Category == "tech");

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("tech", r.Document.Category));
    }

    [Fact]
    public async Task FullTextSearch_MaxResults_LimitsCount()
    {
        await this.SeedAsync();
        var results = await this.store.FullTextSearch<Article>("orleans", maxResults: 1);
        Assert.Single(results);
    }

    [Fact]
    public async Task Remove_AlsoRemovesFromFullTextIndex()
    {
        await this.SeedAsync();
        await this.store.Remove<Article>("1");

        var results = await this.store.FullTextSearch<Article>("persistence");
        Assert.Empty(results);
    }

    [Fact]
    public async Task Update_RewritesFullTextEntry()
    {
        await this.SeedAsync();
        await this.store.Update(new Article { Id = "2", Title = "Cooking with basil", Body = "Now using basil instead.", Category = "food" });

        Assert.Empty(await this.store.FullTextSearch<Article>("garlic"));
        Assert.Single(await this.store.FullTextSearch<Article>("basil"));
    }

    [Fact]
    public async Task Clear_AlsoClearsFullTextIndex()
    {
        await this.SeedAsync();
        await this.store.Clear<Article>();

        var results = await this.store.FullTextSearch<Article>("orleans");
        Assert.Empty(results);
    }

    [Fact]
    public async Task FullTextSearch_ViaFluentQuery()
    {
        await this.SeedAsync();
        var results = await this.store.Query<Article>().Where(a => a.Category == "tech").FullTextMatch("orleans");

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("tech", r.Document.Category));
    }
}
