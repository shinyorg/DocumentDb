using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite.VectorSupport;
using Xunit;

namespace Shiny.DocumentDb.Sqlite.VectorSupport.Tests;

// RegisterAutoExtension installs a process-global SQLite auto-extension, so these live in their
// own assembly (separate process) to avoid colliding with the LoadExtension-based tests in the
// main suite.
public class SqliteVecTests
{
    sealed class Doc
    {
        public string Id { get; set; } = "";
        public ReadOnlyMemory<float> Embedding { get; set; }
    }

    static bool VecAvailable =>
        File.Exists(Path.Combine(AppContext.BaseDirectory, "vec0.dylib")) ||
        File.Exists(Path.Combine(AppContext.BaseDirectory, "vec0.so")) ||
        File.Exists(Path.Combine(AppContext.BaseDirectory, "vec0.dll"));

    [Fact]
    public void RegisterAutoExtension_IsIdempotent()
    {
        if (!VecAvailable) { Assert.Skip("bundled sqlite-vec loadable not present next to test assembly."); return; }
        SqliteVec.RegisterAutoExtension();
        SqliteVec.RegisterAutoExtension(); // must not throw on second call
    }

    [Fact]
    public void CreateProvider_SetsPreloadedAndSupportsVector()
    {
        if (!VecAvailable) { Assert.Skip("bundled sqlite-vec loadable not present next to test assembly."); return; }
        var provider = SqliteVec.CreateProvider("Data Source=:memory:");
        Assert.True(provider.VectorExtensionPreloaded);
        Assert.True(provider.SupportsVector);
    }

    [Fact]
    public async Task NearestVectors_RoundTrips_ViaAutoExtension()
    {
        if (!VecAvailable) { Assert.Skip("bundled sqlite-vec loadable not present next to test assembly."); return; }
        var db = Path.Combine(Path.GetTempPath(), $"vecpkg_{Guid.NewGuid():N}.db");
        try
        {
            var opts = new DocumentStoreOptions { DatabaseProvider = SqliteVec.CreateProvider($"Data Source={db}") };
            opts.MapVectorProperty<Doc>(d => d.Embedding, dimensions: 4, metric: VectorDistance.Cosine);
            using var store = new DocumentStore(opts);

            await store.Insert(new Doc { Id = "a", Embedding = new float[] { 1, 0, 0, 0 } });
            await store.Insert(new Doc { Id = "b", Embedding = new float[] { 0, 1, 0, 0 } });
            await store.Insert(new Doc { Id = "c", Embedding = new float[] { 0.9f, 0.1f, 0, 0 } });

            var hits = await store.NearestVectors<Doc>(new float[] { 1, 0, 0, 0 }, k: 2);

            Assert.Equal(2, hits.Count);
            Assert.Equal("a", hits[0].Document.Id);
            Assert.Equal("c", hits[1].Document.Id);
        }
        finally
        {
            if (File.Exists(db)) File.Delete(db);
        }
    }

    [Fact]
    public async Task NearestVectors_ViaSession_RoundTrips()
    {
        if (!VecAvailable) { Assert.Skip("bundled sqlite-vec loadable not present next to test assembly."); return; }
        var db = Path.Combine(Path.GetTempPath(), $"vecpkg_{Guid.NewGuid():N}.db");
        try
        {
            var opts = new DocumentStoreOptions { DatabaseProvider = SqliteVec.CreateProvider($"Data Source={db}") };
            opts.MapVectorProperty<Doc>(d => d.Embedding, dimensions: 4, metric: VectorDistance.Cosine);
            using var store = new DocumentStore(opts);

            await store.Insert(new Doc { Id = "a", Embedding = new float[] { 1, 0, 0, 0 } });
            await store.Insert(new Doc { Id = "b", Embedding = new float[] { 0, 1, 0, 0 } });
            await store.Insert(new Doc { Id = "c", Embedding = new float[] { 0.9f, 0.1f, 0, 0 } });

            await using var session = store.OpenSession();
            Assert.True(session.Store.SupportsVector);
            var hits = await session.NearestVectors<Doc>(new float[] { 1, 0, 0, 0 }, k: 2);

            Assert.Equal(2, hits.Count);
            Assert.Equal("a", hits[0].Document.Id);
            Assert.Equal("c", hits[1].Document.Id);
        }
        finally
        {
            if (File.Exists(db)) File.Delete(db);
        }
    }
}
