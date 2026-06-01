using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

[Collection("MongoDB")]
public class MongoDbVectorTests
{
    readonly MongoDbDatabaseFixture fx;

    public MongoDbVectorTests(MongoDbDatabaseFixture fx) => this.fx = fx;

    const string Collection = "mongo_vec_docs";
    const string IndexName = "vector_index_VectorDoc";

    async Task<IDocumentStore> SeedAsync(string collectionName, int dimensions = 4)
    {
        await this.fx.EnsureAtlasLocalAsync();
        await this.fx.CreateAtlasVectorIndexAsync(collectionName, IndexName, dimensions);

        var store = this.fx.CreateAtlasVectorStore(collectionName, dimensions);
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        // Atlas Search is eventually consistent — the writes above are durable in mongod, but
        // mongot (the search server) ingests them asynchronously. Poll until $vectorSearch sees
        // them, capped at 30 s, so the tests don't race against indexing.
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            var probe = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 5);
            if (probe.Count >= VectorTestSeed.Docs.Count) return store;
            await Task.Delay(500);
        }
        return store;
    }

    [Fact]
    public async Task SupportsVector_IsTrue_WhenMappingRegistered()
    {
        await this.fx.EnsureAtlasLocalAsync();
        var store = this.fx.CreateAtlasVectorStore("mongo_vec_supports", 4);
        Assert.True(store.SupportsVector);
    }

    [Fact]
    public async Task Insert_Then_NearestVectors_ReturnsOrderedTopK()
    {
        var store = await this.SeedAsync(Collection);

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 3);

        Assert.Equal(3, hits.Count);
        Assert.Equal("u1", hits[0].Document.Id);
        Assert.Equal("u2", hits[1].Document.Id);
        Assert.Equal("u3", hits[2].Document.Id);
    }

    [Fact]
    public async Task Remove_DropsDocumentFromAnnResults()
    {
        var store = await this.SeedAsync("mongo_vec_remove");
        await store.Remove<VectorDoc>("u1");

        // Atlas search is eventually consistent — poll until the removal is reflected.
        var deadline = DateTime.UtcNow.AddSeconds(30);
        IReadOnlyList<VectorResult<VectorDoc>> hits = Array.Empty<VectorResult<VectorDoc>>();
        while (DateTime.UtcNow < deadline)
        {
            hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 5);
            if (!hits.Any(h => h.Document.Id == "u1")) return;
            await Task.Delay(500);
        }
        Assert.DoesNotContain(hits, h => h.Document.Id == "u1");
    }
}
