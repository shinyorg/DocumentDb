using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

[Collection("SQLite")]
public class SqliteVectorTests : IDisposable
{
    readonly SqliteDatabaseFixture fx;
    readonly string dbPath;
    readonly bool vecAvailable;

    public SqliteVectorTests(SqliteDatabaseFixture fx)
    {
        this.fx = fx;
        this.vecAvailable = SqliteDatabaseFixture.FindVec0BinaryPath() != null;
        this.dbPath = Path.Combine(Path.GetTempPath(), $"sqlite_vec_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(this.dbPath)) File.Delete(this.dbPath);
    }

    [Fact]
    public void Vec0Binary_IsAvailable_OnDeveloperMachine()
    {
        // Documents the requirement — when this fails, the rest of the suite skips, and CI
        // needs to provision the binary or download it on demand.
        Assert.True(this.vecAvailable, "sqlite-vec native binary (vec0.dylib / vec0.so / vec0.dll) was not found next to the test assembly.");
    }

    [Fact]
    public async Task SupportsVector_IsTrue_WhenExtensionEnabled()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        Assert.True(store.SupportsVector);
    }

    [Fact]
    public async Task Insert_Then_NearestVectors_ReturnsOrderedTopK()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
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
    public async Task NearestVectors_WithWhereFilter_PostFiltersWithCandidateMultiplier()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        // sqlite-vec post-filters: ANN candidates are gathered first, then the predicate runs.
        // The default multiplier (4) keeps enough candidates that the tag="b" docs survive.
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
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        await store.Remove<VectorDoc>("u1");
        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 5);
        Assert.DoesNotContain(hits, h => h.Document.Id == "u1");
    }

    [Fact]
    public async Task EuclideanMetric_RanksCorrectly()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath, metric: VectorDistance.Euclidean);
        foreach (var d in VectorTestSeed.Docs)
            await store.Insert(d);

        var hits = await store.NearestVectors<VectorDoc>(VectorTestSeed.Query, k: 3);
        Assert.Equal("u1", hits[0].Document.Id);
        Assert.True(hits[0].Score < hits[1].Score);
    }
}
