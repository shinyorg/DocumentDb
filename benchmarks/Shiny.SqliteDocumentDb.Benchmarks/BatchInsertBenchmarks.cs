using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using SQLite;

namespace Shiny.SqliteDocumentDb.Benchmarks;

[MemoryDiagnoser]
public class BatchInsertBenchmarks
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

    [Benchmark(Description = "DocumentStore BatchInsert")]
    public async Task DocumentStore_BatchInsert()
    {
        var ctx = BenchmarkJsonContext.Default;
        var users = Enumerable.Range(0, Count).Select(i =>
            new BenchmarkUser { Id = Guid.NewGuid().ToString("N"), Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" }
        );
        await store.BatchInsert(users, ctx.BenchmarkUser);
    }

    [Benchmark(Description = "sqlite-net InsertAllAsync")]
    public async Task SqliteNet_InsertAll()
    {
        var users = Enumerable.Range(0, Count).Select(i =>
            new SqliteUser { DocId = Guid.NewGuid().ToString("N"), Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" }
        ).ToList();
        await db.InsertAllAsync(users);
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
