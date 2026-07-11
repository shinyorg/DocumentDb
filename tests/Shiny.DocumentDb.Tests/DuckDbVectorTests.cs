using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

[Collection("DuckDB")]
public class DuckDbVectorTests
{
    readonly DuckDbDatabaseFixture fx;

    public DuckDbVectorTests(DuckDbDatabaseFixture fx) => this.fx = fx;

    [Fact]
    public async Task SupportsVector_IsTrue()
    {
        var store = this.fx.CreateVectorStore("duck_vec_supports");
        Assert.True(store.SupportsVector);
    }

    [Fact]
    public async Task Insert_Then_NearestVectors_ReturnsOrderedTopK()
    {
        var store = this.fx.CreateVectorStore("duck_vec_topk");
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 3);

        Assert.Equal(3, hits.Count);
        Assert.Equal("u1", hits[0].Document.Id);
        Assert.Equal("u2", hits[1].Document.Id);
        Assert.Equal("u3", hits[2].Document.Id);
        Assert.True(hits[0].Score <= hits[1].Score);
        Assert.True(hits[1].Score <= hits[2].Score);
    }

    [Fact]
    public async Task NearestVectors_WithWhereFilter_PreFilters()
    {
        var store = this.fx.CreateVectorStore("duck_vec_filter");
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        var hits = await store.Query<VectorDoc>()
            .Where(d => d.Tag == "b")
            .NearestVectors(VectorTestSeed.Query, k: 5);

        Assert.Equal(2, hits.Count);
        Assert.Equal("u3", hits[0].Document.Id);
        Assert.Equal("u4", hits[1].Document.Id);
    }

    [Fact]
    public async Task Remove_DropsDocumentFromAnnResults()
    {
        var store = this.fx.CreateVectorStore("duck_vec_remove");
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        await store.Remove<VectorDoc>("u1");
        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 5);
        Assert.DoesNotContain(hits, h => h.Document.Id == "u1");
    }

    [Fact]
    public async Task EuclideanMetric_RanksCorrectly()
    {
        var store = this.fx.CreateVectorStore("duck_vec_l2", metric: VectorDistance.Euclidean);
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 3);
        Assert.Equal("u1", hits[0].Document.Id);
        Assert.True(hits[0].Score < hits[1].Score);
    }

    [Fact]
    public async Task DotProductMetric_RanksTopFirst()
    {
        var store = this.fx.CreateVectorStore("duck_vec_dot", metric: VectorDistance.DotProduct, indexKind: VectorIndexKind.None);
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 5);
        Assert.Equal("u1", hits[0].Document.Id);

        // Regression: Score is the raw inner product (higher = closer), not the DB's negated distance. The
        // nearest hit must therefore have the highest (not lowest) score.
        for (var i = 1; i < hits.Count; i++)
            Assert.True(hits[0].Score >= hits[i].Score, $"nearest score {hits[0].Score} should be >= {hits[i].Score}");
    }
}
