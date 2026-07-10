using Shiny.DocumentDb.LiteDb;
using Xunit;

namespace Shiny.DocumentDb.Tests.LiteDb;

/// <summary>
/// Composable Lucene queries on the in-memory provider (LiteDB): DocumentFunctions.LuceneMatch/LuceneScore
/// run through the parse-and-evaluate BCL fallback, so the full grammar — including operators no SQL engine
/// supports, like fuzzy — works here.
/// </summary>
public class LiteDbLuceneQueryTests : IDisposable
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

    public LiteDbLuceneQueryTests()
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

    async Task Seed()
    {
        await this.store.Insert(new Article { Id = "1", Title = "Orleans persistence", Body = "Using DocumentDb as an Orleans grain storage provider.", Category = "tech" });
        await this.store.Insert(new Article { Id = "2", Title = "Cooking with garlic", Body = "A recipe that uses a lot of garlic and onion.", Category = "food" });
        await this.store.Insert(new Article { Id = "3", Title = "Distributed systems", Body = "Orleans is a virtual actor framework for distributed applications.", Category = "tech" });
    }

    [Fact]
    public async Task LuceneMatch_BooleanOperators()
    {
        await this.Seed();
        var hits = await this.store.Query<Article>()
            .Where(a => DocumentFunctions.LuceneMatch(a.Body, "orleans AND grain"))
            .ToList();
        Assert.Equal(new[] { "1" }, hits.Select(h => h.Id));
    }

    [Fact]
    public async Task LuceneMatch_Fuzzy_WorksInMemory()
    {
        await this.Seed();
        // Fuzzy is unsupported on SQL engines but the in-memory evaluator honors it.
        var hits = await this.store.Query<Article>()
            .Where(a => DocumentFunctions.LuceneMatch(a.Body, "orlean~"))
            .ToList();
        Assert.Equal(new[] { "1", "3" }, hits.Select(h => h.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task LuceneScore_OrderBy()
    {
        await this.Seed();
        var ranked = await this.store.Query<Article>()
            .Where(a => DocumentFunctions.LuceneMatch(a.Body, "orleans"))
            .OrderByDescending(a => DocumentFunctions.LuceneScore(a.Body, "orleans grain"))
            .ToList();
        Assert.Equal("1", ranked[0].Id);
    }
}
