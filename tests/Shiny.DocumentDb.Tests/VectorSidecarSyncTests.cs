using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// The vector index is a sidecar, and the mapped embedding is a ReadOnlyMemory<float> — a non-nullable struct
// that serializes as [] when unset. So "absent" has to mean the same thing in the body and in the index:
// a merge leaves both alone, a whole-document write treats an empty embedding as a genuine clear.
[Collection("SQLite")]
public class VectorSidecarSyncTests : IDisposable
{
    readonly SqliteDatabaseFixture fx;
    readonly string dbPath;
    readonly bool vecAvailable;

    public VectorSidecarSyncTests(SqliteDatabaseFixture fx)
    {
        this.fx = fx;
        this.vecAvailable = SqliteDatabaseFixture.FindVec0BinaryPath() != null;
        this.dbPath = Path.Combine(Path.GetTempPath(), $"vecsync_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(this.dbPath)) File.Delete(this.dbPath);
    }

    static readonly ReadOnlyMemory<float> Probe = VectorDoc.V(1, 0, 0, 0);

    [Fact]
    public async Task Merge_patch_keeps_both_the_stored_embedding_and_its_index_row()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        await store.Insert(new VectorDoc { Id = "v1", Tag = "a", Embedding = Probe });

        // Tag-only patch: Embedding is unset, which serializes as [] — it must not reach the body.
        await store.Upsert(new VectorDoc { Id = "v1", Tag = "b" });

        var doc = await store.Get<VectorDoc>("v1");
        Assert.Equal("b", doc!.Tag);
        Assert.Equal(4, doc.Embedding.Length);                                  // body kept it
        Assert.Single(await store.NearestVectors<VectorDoc>(Probe, k: 5));      // index kept it
    }

    [Fact]
    public async Task Merge_patch_leaves_no_empty_array_in_the_body()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        await store.Insert(new VectorDoc { Id = "v1", Tag = "a", Embedding = Probe });

        await store.Upsert(new VectorDoc { Id = "v1", Tag = "b" });

        var body = (await store.Collection(typeof(VectorDoc)).Get("v1"))!.ToJsonString();
        Assert.DoesNotContain("\"embedding\":[]", body);
    }

    [Fact]
    public async Task Update_merge_keeps_both_too()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        await store.Insert(new VectorDoc { Id = "v1", Tag = "a", Embedding = Probe });

        await store.Update(new VectorDoc { Id = "v1", Tag = "b" }, patch: true);

        Assert.Equal(4, (await store.Get<VectorDoc>("v1"))!.Embedding.Length);
        Assert.Single(await store.NearestVectors<VectorDoc>(Probe, k: 5));
    }

    [Fact]
    public async Task Merge_still_reindexes_an_embedding_the_patch_does_carry()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        await store.Insert(new VectorDoc { Id = "v1", Tag = "a", Embedding = Probe });

        await store.Upsert(new VectorDoc { Id = "v1", Embedding = VectorDoc.V(0, 1, 0, 0) });

        var hits = await store.NearestVectors<VectorDoc>(Probe, k: 5);
        Assert.Single(hits);
        Assert.Equal(4, hits[0].Document.Embedding.Length);
        // The stored vector moved, so the distance from the original probe is no longer ~0.
        Assert.True(hits[0].Score > 0.1, $"expected the re-indexed vector to be further away, got {hits[0].Score}");
    }

    [Fact]
    public async Task Update_replace_clearing_the_embedding_drops_the_index_row()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        await store.Insert(new VectorDoc { Id = "v1", Tag = "a", Embedding = Probe });
        Assert.Single(await store.NearestVectors<VectorDoc>(Probe, k: 5));

        // Replace carries the whole document, so an empty embedding genuinely clears it.
        await store.Update(new VectorDoc { Id = "v1", Tag = "b", Embedding = default });

        Assert.Empty(await store.NearestVectors<VectorDoc>(Probe, k: 5));
        Assert.Equal(0, (await store.Get<VectorDoc>("v1"))!.Embedding.Length);
    }

    [Fact]
    public async Task Upsert_replace_clearing_the_embedding_drops_the_index_row()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        await store.Insert(new VectorDoc { Id = "v1", Tag = "a", Embedding = Probe });

        await store.Upsert(new VectorDoc { Id = "v1", Tag = "b", Embedding = default }, patchIfUpdate: false);

        Assert.Empty(await store.NearestVectors<VectorDoc>(Probe, k: 5));
    }

    [Fact]
    public async Task Session_merge_keeps_the_embedding_and_replace_clears_it()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        await store.Insert(new VectorDoc { Id = "v1", Tag = "a", Embedding = Probe });

        await store.OpenSession().Upsert(new VectorDoc { Id = "v1", Tag = "b" }).SaveChanges();
        Assert.Equal(4, (await store.Get<VectorDoc>("v1"))!.Embedding.Length);
        Assert.Single(await store.NearestVectors<VectorDoc>(Probe, k: 5));

        await store.OpenSession().Update(new VectorDoc { Id = "v1", Tag = "c", Embedding = default }).SaveChanges();
        Assert.Empty(await store.NearestVectors<VectorDoc>(Probe, k: 5));
    }

    [Fact]
    public async Task Json_lane_merge_keeps_the_row_and_replace_with_null_clears_it()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        await store.Insert(new VectorDoc { Id = "v1", Tag = "a", Embedding = Probe });
        var lane = store.Collection(typeof(VectorDoc));

        // Merge patch without the embedding member — the row stays.
        await lane.Upsert(System.Text.Json.Nodes.JsonNode.Parse("""{"id":"v1","tag":"b"}""")!.AsObject());
        Assert.Single(await store.NearestVectors<VectorDoc>(Probe, k: 5));

        // Whole-document replace with an explicit null — the documented "no value" — drops it.
        await lane.Update(System.Text.Json.Nodes.JsonNode.Parse("""{"id":"v1","tag":"c","embedding":null}""")!.AsObject());
        Assert.Empty(await store.NearestVectors<VectorDoc>(Probe, k: 5));
    }

    [Fact]
    public async Task Json_lane_merge_with_an_empty_array_does_not_overwrite_the_stored_vector()
    {
        if (!this.vecAvailable) { Assert.Skip("sqlite-vec native binary not present next to test assembly."); return; }
        var store = this.fx.CreateVectorStore(this.dbPath);
        await store.Insert(new VectorDoc { Id = "v1", Tag = "a", Embedding = Probe });

        // An explicit [] on a merge reads the same as the typed lane's unset struct: "not supplied".
        await store.Collection(typeof(VectorDoc))
            .Upsert(System.Text.Json.Nodes.JsonNode.Parse("""{"id":"v1","tag":"b","embedding":[]}""")!.AsObject());

        Assert.Equal(4, (await store.Get<VectorDoc>("v1"))!.Embedding.Length);
        Assert.Single(await store.NearestVectors<VectorDoc>(Probe, k: 5));
    }
}
