using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using SQLite;

namespace Shiny.SqliteDocumentDb.Benchmarks;

[MemoryDiagnoser]
public class InsertBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    string storePath = null!;
    string sqlitePath = null!;

    [Params(10, 100, 1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_store_{Guid.NewGuid():N}.db");
        sqlitePath = Path.Combine(Path.GetTempPath(), $"bench_sqlite_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        db = new SQLiteAsyncConnection(sqlitePath);
        await db.CreateTableAsync<SqliteUser>();

        // Force DocumentStore to initialize its table
        await store.Clear<BenchmarkUser>();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        using var conn = new SqliteConnection($"Data Source={storePath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM documents;";
        cmd.ExecuteNonQuery();

        db.GetConnection().DeleteAll<SqliteUser>();
    }

    [Benchmark(Description = "DocumentStore Insert")]
    public async Task DocumentStore_Insert()
    {
        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < Count; i++)
        {
            var user = new BenchmarkUser { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            await store.Insert(user, ctx.BenchmarkUser);
        }
    }

    [Benchmark(Description = "sqlite-net Insert")]
    public async Task SqliteNet_Insert()
    {
        for (var i = 0; i < Count; i++)
        {
            var user = new SqliteUser { DocId = Guid.NewGuid().ToString("N"), Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            await db.InsertAsync(user);
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        store.Dispose();
        db.GetConnection().Close();
        File.Delete(storePath);
        File.Delete(sqlitePath);
    }
}
