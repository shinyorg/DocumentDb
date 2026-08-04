using Microsoft.Extensions.VectorData;
using Shiny.DocumentDb.Extensions.VectorData;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The document-native leg of the MEVD connector, over MongoDB Atlas Local. Atlas reports a normalized
/// <i>similarity</i> where the relational providers report a distance — the opposite direction for the same
/// metric — so this suite is where that gets pinned down: the ordering is portable, the raw score is not.
/// </summary>
[Collection("MongoDB")]
public class MongoDbVectorDataConnectorTests
{
    readonly MongoDbDatabaseFixture fx;

    public MongoDbVectorDataConnectorTests(MongoDbDatabaseFixture fx) => this.fx = fx;

    const string IndexName = "vector_index_MevdMemo";

    async Task<VectorStoreCollection<string, MevdMemo>> SeedAsync(string collectionName)
    {
        await this.fx.EnsureAtlasLocalAsync();
        await this.fx.CreateAtlasVectorIndexAsync(collectionName, IndexName, 4);

        var store = this.fx.CreateAtlasVectorStore(collectionName, o => o.MapVectorRecord<MevdMemo>());
        var collection = new DocumentDbVectorStore(store).GetCollection<string, MevdMemo>(nameof(MevdMemo));
        await collection.UpsertAsync(MevdSeed.Memos());

        // mongot ingests asynchronously, so poll until $vectorSearch sees the writes rather than racing it.
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            var probe = await Count(collection);
            if (probe >= MevdSeed.Rows.Count)
                break;
            await Task.Delay(500);
        }
        return collection;
    }

    static async Task<int> Count(VectorStoreCollection<string, MevdMemo> collection)
    {
        var count = 0;
        await foreach (var _ in collection.SearchAsync(MevdSeed.Query, top: 10))
            count++;
        return count;
    }

    [Fact]
    public async Task Upsert_Get_Delete_RoundTrips()
    {
        var collection = await this.SeedAsync("mongo_mevd_crud");

        var fetched = await collection.GetAsync("n1", new RecordRetrievalOptions { IncludeVectors = true });
        Assert.NotNull(fetched);
        Assert.Equal("a", fetched!.Tag);
        Assert.Equal(4, fetched.Embedding.Length);

        Assert.Equal(0, (await collection.GetAsync("n1"))!.Embedding.Length);

        await collection.DeleteAsync("n1");
        Assert.Null(await collection.GetAsync("n1"));
    }

    [Fact]
    public async Task Search_ReturnsNearestFirst_WithASimilarityScore()
    {
        var collection = await this.SeedAsync("mongo_mevd_topk");

        var hits = new List<VectorSearchResult<MevdMemo>>();
        await foreach (var hit in collection.SearchAsync(MevdSeed.Query, top: 3))
            hits.Add(hit);

        Assert.Equal(["n1", "n2", "n3"], hits.Select(h => h.Record.Id));

        // The ordering matches the relational leg exactly, but the score runs the other way: Atlas returns a
        // similarity (higher = closer) where pgvector returns a distance (lower = closer).
        Assert.True(hits[0].Score >= hits[1].Score);
        Assert.True(hits[1].Score >= hits[2].Score);
    }

    [Fact]
    public async Task Search_PassesTheFilterThroughToTheAnnQuery()
    {
        var collection = await this.SeedAsync("mongo_mevd_filter");

        var ids = new List<string>();
        await foreach (var hit in collection.SearchAsync(MevdSeed.Query, top: 5, new VectorSearchOptions<MevdMemo> { Filter = m => m.Tag == "b" }))
            ids.Add(hit.Record.Id);

        Assert.Equal(["n3", "n4"], ids);
    }
}
