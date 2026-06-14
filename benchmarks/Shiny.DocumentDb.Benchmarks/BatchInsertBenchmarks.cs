using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using SQLite;

namespace Shiny.DocumentDb.Benchmarks;

[MemoryDiagnoser]
public class BatchInsertBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    BenchDbContext efContext = null!;
    string storePath = null!;
    string sqlitePath = null!;
    string efPath = null!;

    [Params(10, 100, 1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_store_{Guid.NewGuid():N}.db");
        sqlitePath = Path.Combine(Path.GetTempPath(), $"bench_sqlite_{Guid.NewGuid():N}.db");
        efPath = Path.Combine(Path.GetTempPath(), $"bench_ef_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        db = new SQLiteAsyncConnection(sqlitePath);
        await db.CreateTableAsync<SqliteUser>();

        efContext = BenchDbContext.Create(efPath);

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

        efContext.Users.ExecuteDelete();
        efContext.ChangeTracker.Clear();
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

    [Benchmark(Description = "EF Core AddRange")]
    public async Task EfCore_AddRange()
    {
        var users = Enumerable.Range(0, Count).Select(i =>
            new EfUser { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" }
        ).ToList();
        efContext.Users.AddRange(users);
        await efContext.SaveChangesAsync();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        store.Dispose();
        db.GetConnection().Close();
        await efContext.DisposeAsync();
        File.Delete(storePath);
        File.Delete(sqlitePath);
        File.Delete(efPath);
    }
}
