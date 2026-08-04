using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// Composable full-text queries — DocumentFunctions.LuceneMatch / LuceneScore inside a LINQ (and string)
/// Where / OrderBy on SQLite (FTS5 MATCH pushdown), composing with ordinary predicates, paging, and ranking.
/// </summary>
public class SqliteLuceneQueryTests : IDisposable
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

    public SqliteLuceneQueryTests()
    {
        var cs = $"Data Source=lucene_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(cs);
        this.holdOpen.Open();

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(cs),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.ConfigureDocument<Article>(cfg => cfg.MapFullTextProperty([a => a.Title, a => a.Body], FullTextLanguage.English));
        this.store = new DocumentStore(opts);
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.holdOpen.Dispose();
    }

    async Task Seed()
    {
        await this.store.Insert(new Article { Id = "1", Title = "Orleans persistence", Body = "Using DocumentDb as an Orleans grain storage provider.", Category = "tech" });
        await this.store.Insert(new Article { Id = "2", Title = "Cooking with garlic", Body = "A recipe that uses a lot of garlic and onion.", Category = "food" });
        await this.store.Insert(new Article { Id = "3", Title = "Distributed systems", Body = "Orleans is a virtual actor framework for distributed applications.", Category = "tech" });
    }

    [Fact]
    public async Task LuceneMatch_SimpleTerm()
    {
        await this.Seed();
        var hits = await this.store.Query<Article>()
            .Where(a => DocumentFunctions.LuceneMatch(a.Body, "orleans"))
            .ToList();
        Assert.Equal(new[] { "1", "3" }, hits.Select(h => h.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task LuceneMatch_ComposesWithOrdinaryPredicate()
    {
        await this.Seed();
        var hits = await this.store.Query<Article>()
            .Where(a => a.Category == "tech" && DocumentFunctions.LuceneMatch(a.Body, "orleans OR garlic"))
            .ToList();
        Assert.Equal(new[] { "1", "3" }, hits.Select(h => h.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task LuceneMatch_And_ExcludesNonMatches()
    {
        await this.Seed();
        var hits = await this.store.Query<Article>()
            .Where(a => DocumentFunctions.LuceneMatch(a.Body, "orleans AND grain"))
            .ToList();
        Assert.Equal(new[] { "1" }, hits.Select(h => h.Id));
    }

    [Fact]
    public async Task LuceneMatch_Not_Excludes()
    {
        await this.Seed();
        var hits = await this.store.Query<Article>()
            .Where(a => DocumentFunctions.LuceneMatch(a.Body, "orleans NOT grain"))
            .ToList();
        Assert.Equal(new[] { "3" }, hits.Select(h => h.Id));
    }

    [Fact]
    public async Task LuceneMatch_Prefix()
    {
        await this.Seed();
        var hits = await this.store.Query<Article>()
            .Where(a => DocumentFunctions.LuceneMatch(a.Body, "distrib*"))
            .ToList();
        Assert.Equal(new[] { "3" }, hits.Select(h => h.Id));
    }

    [Fact]
    public async Task LuceneScore_OrderByRelevance()
    {
        await this.Seed();
        var ranked = await this.store.Query<Article>()
            .Where(a => DocumentFunctions.LuceneMatch(a.Body, "orleans"))
            .OrderByDescending(a => DocumentFunctions.LuceneScore(a.Body, "orleans grain"))
            .ToList();
        // Article 1 mentions Orleans twice + "grain"; it must outrank article 3.
        Assert.Equal(new[] { "1", "3" }, ranked.Select(h => h.Id));
    }

    [Fact]
    public async Task LuceneMatch_NoMatch_ReturnsEmpty()
    {
        await this.Seed();
        var hits = await this.store.Query<Article>()
            .Where(a => DocumentFunctions.LuceneMatch(a.Body, "kubernetes"))
            .ToList();
        Assert.Empty(hits);
    }

    [Fact]
    public async Task LuceneMatch_Fuzzy_ThrowsOnSqlite()
    {
        await this.Seed();
        // SQLite FTS5 has no fuzzy operator — the capability gate must reject it.
        await Assert.ThrowsAsync<NotSupportedException>(() => this.store.Query<Article>()
            .Where(a => DocumentFunctions.LuceneMatch(a.Body, "orleans~"))
            .ToList());
    }

    [Fact]
    public async Task LuceneMatch_StringGrammar_MatchesLinq()
    {
        await this.Seed();
        var linq = (await this.store.Query<Article>()
            .Where(a => DocumentFunctions.LuceneMatch(a.Body, "orleans AND grain"))
            .ToList()).Select(h => h.Id).OrderBy(x => x).ToArray();

        var str = (await this.store.Query<Article>()
            .Where("lucenematch(body, 'orleans AND grain')")
            .ToList()).Select(h => h.Id).OrderBy(x => x).ToArray();

        Assert.Equal(linq, str);
        Assert.NotEmpty(str);
    }
}
