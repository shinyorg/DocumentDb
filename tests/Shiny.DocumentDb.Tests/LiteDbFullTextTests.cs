using Shiny.DocumentDb.LiteDb;
using Xunit;

namespace Shiny.DocumentDb.Tests.LiteDb;

/// <summary>
/// Full-text search coverage for the in-memory TF-IDF fallback (LiteDB has no native FTS engine).
/// </summary>
public class LiteDbFullTextTests : IDisposable
{
    sealed class Article
    {
        public string Id { get; set; } = "";
        public string Title { get; set; } = "";
        public string Body { get; set; } = "";
        public string Category { get; set; } = "";
    }

    readonly string file = Path.GetTempFileName();
    readonly LiteDbDocumentStore store;

    public LiteDbFullTextTests()
    {
        var opts = new LiteDbDocumentStoreOptions
        {
            ConnectionString = $"Filename={this.file};Connection=direct",
            CollectionName = $"t{Guid.NewGuid():N}"
        };
        opts.MapFullTextProperty<Article>([a => a.Title, a => a.Body]);
        this.store = new LiteDbDocumentStore(opts);
    }

    public void Dispose()
    {
        this.store.Dispose();
        try { File.Delete(this.file); } catch { /* best effort */ }
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
        Assert.Empty(await this.store.FullTextSearch<Article>("kubernetes"));
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
    public async Task Remove_DropsFromResults()
    {
        await this.SeedAsync();
        await this.store.Remove<Article>("1");
        Assert.Empty(await this.store.FullTextSearch<Article>("persistence"));
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
