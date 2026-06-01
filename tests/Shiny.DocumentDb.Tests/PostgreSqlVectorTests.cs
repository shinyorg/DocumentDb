using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

[Collection("PostgreSQL")]
public class PostgreSqlVectorTests
{
    readonly PostgreSqlDatabaseFixture fx;

    public PostgreSqlVectorTests(PostgreSqlDatabaseFixture fx) => this.fx = fx;

    [Fact]
    public async Task SupportsVector_IsTrue()
    {
        var store = this.fx.CreateVectorStore("pg_vec_supports");
        Assert.True(store.SupportsVector);
    }

    [Fact]
    public async Task Insert_Then_NearestVectors_ReturnsOrderedTopK()
    {
        var store = this.fx.CreateVectorStore("pg_vec_topk");
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 3);

        Assert.Equal(3, hits.Count);
        Assert.Equal("u1", hits[0].Document.Id);
        Assert.Equal("u2", hits[1].Document.Id);
        Assert.Equal("u3", hits[2].Document.Id);

        // Cosine distance: identical vectors → ~0
        Assert.InRange(hits[0].Score, -0.001, 0.001);
        // And monotonically increasing.
        Assert.True(hits[0].Score <= hits[1].Score);
        Assert.True(hits[1].Score <= hits[2].Score);
    }

    [Fact]
    public async Task NearestVectors_WithWhereFilter_PreFiltersInsideAnnSearch()
    {
        var store = this.fx.CreateVectorStore("pg_vec_filter");
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        // Only tag "b" docs are eligible — u3 and u4 — and u3 wins by cosine distance.
        var hits = await store.Query<VectorDoc>()
            .Where(d => d.Tag == "b")
            .NearestVectors(VectorTestSeed.Query, k: 5);

        Assert.Equal(2, hits.Count);
        Assert.Equal("u3", hits[0].Document.Id);
        Assert.Equal("u4", hits[1].Document.Id);
    }

    [Fact]
    public async Task Update_OverwritesEmbedding_AndChangesRanking()
    {
        var store = this.fx.CreateVectorStore("pg_vec_update");
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        // Move u5 right on top of the query vector.
        await store.Update(new VectorDoc { Id = "u5", Tag = "c", Embedding = VectorTestSeed.Query });

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 2);
        Assert.Contains(hits, h => h.Document.Id == "u5");
        Assert.True(hits[0].Score <= hits[1].Score);
    }

    [Fact]
    public async Task Remove_DropsDocumentFromAnnResults()
    {
        var store = this.fx.CreateVectorStore("pg_vec_remove");
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        await store.Remove<VectorDoc>("u1");

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 5);
        Assert.DoesNotContain(hits, h => h.Document.Id == "u1");
    }

    [Fact]
    public async Task EuclideanMetric_RanksCorrectly()
    {
        var store = this.fx.CreateVectorStore("pg_vec_l2", metric: VectorDistance.Euclidean);
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 3);
        Assert.Equal("u1", hits[0].Document.Id);
        Assert.True(hits[0].Score < hits[1].Score);
    }

    [Fact]
    public async Task DotProductMetric_RanksCorrectly()
    {
        var store = this.fx.CreateVectorStore("pg_vec_dot", metric: VectorDistance.DotProduct);
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 5);
        // Top is u1 (largest inner product with query (1,0,0,0)).
        Assert.Equal("u1", hits[0].Document.Id);
    }

    [Fact]
    public async Task DimensionMismatch_Throws()
    {
        var store = this.fx.CreateVectorStore("pg_vec_dim");
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.NearestVectors<VectorDoc>(new ReadOnlyMemory<float>(new float[3]), 5));
    }
}
