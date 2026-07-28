using Shiny.DocumentDb;
using Xunit;

namespace Shiny.DocumentDb.Sqlite.VectorSupport.Tests;

/// <summary>
/// Re-embedding a document that already has a vector.
/// </summary>
/// <remarks>
/// This used to throw <c>UNIQUE constraint failed on {table}_vec_{type} primary key</c>: the upsert
/// leaned on <c>INSERT OR REPLACE</c>, and vec0 does not implement SQLite's conflict-resolution
/// clauses, so replacing an existing rowid is a hard constraint failure rather than a replace. Only
/// the first write to a document ever worked. The upsert now deletes the row before re-inserting it.
/// </remarks>
public class SqliteVectorUpdateTests
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

    static async Task WithStore(Func<IDocumentStore, Task> body)
    {
        var db = Path.Combine(Path.GetTempPath(), $"vecupd_{Guid.NewGuid():N}.db");
        try
        {
            var options = new DocumentStoreOptions
            {
                DatabaseProvider = SqliteVec.CreateProvider($"Data Source={db}")
            };
            options.MapVectorProperty<Doc>(d => d.Embedding, dimensions: 4, metric: VectorDistance.Cosine);

            using var store = new DocumentStore(options);
            await body(store);
        }
        finally
        {
            if (File.Exists(db))
                File.Delete(db);
        }
    }

    [Fact]
    public async Task UpdatingAMappedVector_ReIndexesIt()
    {
        if (!VecAvailable) { Assert.Skip("bundled sqlite-vec loadable not present next to test assembly."); return; }

        await WithStore(async store =>
        {
            await store.Insert(new Doc { Id = "a", Embedding = new float[] { 1, 0, 0, 0 } });
            await store.Update(new Doc { Id = "a", Embedding = new float[] { 0, 1, 0, 0 } });

            // The search reads the sidecar, so it only finds "a" here if the update reached it.
            var hits = await store.NearestVectors<Doc>(new float[] { 0, 1, 0, 0 }, k: 1);

            Assert.Equal("a", Assert.Single(hits).Document.Id);
        });
    }

    [Fact]
    public async Task RepeatedUpserts_LeaveOneRowPerDocument()
    {
        if (!VecAvailable) { Assert.Skip("bundled sqlite-vec loadable not present next to test assembly."); return; }

        await WithStore(async store =>
        {
            for (var i = 0; i < 5; i++)
                await store.Upsert(new Doc { Id = "a", Embedding = new float[] { i, 1, 0, 0 } });

            await store.Insert(new Doc { Id = "b", Embedding = new float[] { 0, 0, 1, 0 } });

            // k = 10 over two documents: a duplicated rowid would come back as repeats.
            var hits = await store.NearestVectors<Doc>(new float[] { 4, 1, 0, 0 }, k: 10);

            Assert.Equal(2, hits.Count);
            Assert.Equal("a", hits[0].Document.Id);
        });
    }
}
