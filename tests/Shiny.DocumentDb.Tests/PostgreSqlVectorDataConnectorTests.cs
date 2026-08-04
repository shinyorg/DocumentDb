using Microsoft.Extensions.VectorData;
using Shiny.DocumentDb.Extensions.VectorData;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The relational leg of the MEVD connector: the same record model and the same calling code as the SQLite
/// suite, over pgvector — which is the whole point of a multi-backend connector. Also the leg where the filter
/// is pushed <i>into</i> the ANN search rather than applied to a candidate scan.
/// </summary>
[Collection("PostgreSQL")]
public class PostgreSqlVectorDataConnectorTests
{
    readonly PostgreSqlDatabaseFixture fx;

    public PostgreSqlVectorDataConnectorTests(PostgreSqlDatabaseFixture fx) => this.fx = fx;

    async Task<VectorStoreCollection<string, MevdArticle>> SeedAsync(string tableName)
    {
        var store = this.fx.CreateVectorStore(tableName, o => o.MapVectorRecord<MevdArticle>());
        var collection = new DocumentDbVectorStore(store).GetCollection<string, MevdArticle>(nameof(MevdArticle));
        await collection.UpsertAsync(MevdSeed.Articles());
        return collection;
    }

    [Fact]
    public async Task Upsert_Get_Delete_RoundTrips()
    {
        var collection = await this.SeedAsync("pgvec_mevd_crud");

        var fetched = await collection.GetAsync("n1", new RecordRetrievalOptions { IncludeVectors = true });
        Assert.NotNull(fetched);
        Assert.Equal("a", fetched!.Tag);
        Assert.Equal(4, fetched.Embedding.Length);

        await collection.DeleteAsync("n1");
        Assert.Null(await collection.GetAsync("n1"));
    }

    [Fact]
    public async Task Search_ReturnsNearestFirst()
    {
        var collection = await this.SeedAsync("pgvec_mevd_topk");

        var hits = new List<VectorSearchResult<MevdArticle>>();
        await foreach (var hit in collection.SearchAsync(MevdSeed.Query, top: 3))
            hits.Add(hit);

        Assert.Equal(["n1", "n2", "n3"], hits.Select(h => h.Record.Id));

        // pgvector reports cosine *distance*, so the identical vector scores ~0 and the scores climb.
        Assert.InRange(hits[0].Score!.Value, -0.001, 0.001);
        Assert.True(hits[0].Score <= hits[1].Score);
        Assert.True(hits[1].Score <= hits[2].Score);
    }

    [Fact]
    public async Task Search_PushesTheFilterIntoTheAnnQuery()
    {
        var collection = await this.SeedAsync("pgvec_mevd_filter");

        var ids = new List<string>();
        await foreach (var hit in collection.SearchAsync(MevdSeed.Query, top: 5, new VectorSearchOptions<MevdArticle> { Filter = a => a.Tag == "b" }))
            ids.Add(hit.Record.Id);

        Assert.Equal(["n3", "n4"], ids);
    }

    [Fact]
    public async Task GetByFilter_ReadsWithoutTheVectorIndex()
    {
        var collection = await this.SeedAsync("pgvec_mevd_getfilter");

        var ids = new List<string>();
        await foreach (var article in collection.GetAsync(a => a.Tag == "a", top: 10,
            new FilteredRecordRetrievalOptions<MevdArticle> { OrderBy = o => o.Ascending(a => a.Id) }))
        {
            ids.Add(article.Id);
        }

        Assert.Equal(["n1", "n2"], ids);
    }
}
