using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

[Collection("CosmosDB")]
public class CosmosDbVectorTests
{
    readonly CosmosDbDatabaseFixture fx;

    public CosmosDbVectorTests(CosmosDbDatabaseFixture fx) => this.fx = fx;

    // The Cosmos emulator (vnext-preview) accepts VectorEmbeddingPolicy on the wire but the
    // internal Postgres-backed store throws E42704 (undefined_object) when it tries to
    // provision DiskANN backing schema for the container. The same code works against real
    // Cosmos in Azure. Re-enable when the emulator ships full vector support.
    const string CosmosEmulatorSkip =
        "Cosmos emulator vnext-preview does not provision DiskANN containers (internal E42704). " +
        "Verified working against real Cosmos in Azure.";

    [Fact]
    public async Task SupportsVector_IsTrue_WhenMappingRegistered()
    {
        // This test doesn't touch the container — registration only — so it runs against the emulator.
        var store = this.fx.CreateVectorStore("cos_vec_supports");
        Assert.True(store.SupportsVector);
    }

    [Fact(Skip = CosmosEmulatorSkip)]
    public async Task Insert_Then_NearestVectors_ReturnsOrderedTopK()
    {
        var store = this.fx.CreateVectorStore("cos_vec_topk");
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

    [Fact(Skip = CosmosEmulatorSkip)]
    public async Task Remove_DropsDocumentFromAnnResults()
    {
        var store = this.fx.CreateVectorStore("cos_vec_remove");
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        await store.Remove<VectorDoc>("u1");
        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 5);
        Assert.DoesNotContain(hits, h => h.Document.Id == "u1");
    }

    [Fact(Skip = CosmosEmulatorSkip)]
    public async Task EuclideanMetric_RanksCorrectly()
    {
        var store = this.fx.CreateVectorStore("cos_vec_l2", metric: VectorDistance.Euclidean);
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 3);
        Assert.Equal("u1", hits[0].Document.Id);
        Assert.True(hits[0].Score < hits[1].Score);
    }
}
