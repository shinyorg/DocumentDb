using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

[Collection("MSSQL")]
public class SqlServerVectorTests
{
    readonly MsSqlDatabaseFixture fx;

    public SqlServerVectorTests(MsSqlDatabaseFixture fx) => this.fx = fx;

    [Fact]
    public async Task SupportsVector_IsTrue()
    {
        var store = this.fx.CreateVectorStore("mssql_vec_supports");
        Assert.True(store.SupportsVector);
    }

    [Fact]
    public async Task Insert_Then_NearestVectors_ReturnsOrderedTopK()
    {
        var store = this.fx.CreateVectorStore("mssql_vec_topk");
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
        var store = this.fx.CreateVectorStore("mssql_vec_filter");
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
        var store = this.fx.CreateVectorStore("mssql_vec_remove");
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        await store.Remove<VectorDoc>("u1");
        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 5);
        Assert.DoesNotContain(hits, h => h.Document.Id == "u1");
    }

    [Fact]
    public async Task EuclideanMetric_RanksCorrectly()
    {
        var store = this.fx.CreateVectorStore("mssql_vec_l2", metric: VectorDistance.Euclidean);
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 3);
        Assert.Equal("u1", hits[0].Document.Id);
        Assert.True(hits[0].Score < hits[1].Score);
    }
}
