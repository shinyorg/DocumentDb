using BenchmarkDotNet.Attributes;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using SQLite;

namespace Shiny.SqliteDocumentDb.Benchmarks;

[MemoryDiagnoser]
public class QueryBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    string storePath = null!;
    string sqlitePath = null!;

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

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < 1000; i++)
        {
            var user = new BenchmarkUser { Name = $"Alice_{i}", Age = 20 + (i % 50), Email = $"alice{i}@test.com" };
            await store.Insert(user, ctx.BenchmarkUser);

            var sqliteUser = new SqliteUser { DocId = Guid.NewGuid().ToString("N"), Name = $"Alice_{i}", Age = 20 + (i % 50), Email = $"alice{i}@test.com" };
            await db.InsertAsync(sqliteUser);
        }
    }

    [Benchmark(Description = "DocumentStore Query")]
    public async Task<IReadOnlyList<BenchmarkUser>> DocumentStore_Query()
    {
        return await store.Query(BenchmarkJsonContext.Default.BenchmarkUser)
            .Where(u => u.Name == "Alice_500")
            .ToList();
    }

    [Benchmark(Description = "sqlite-net Query")]
    public async Task<List<SqliteUser>> SqliteNet_Query()
    {
        return await db.Table<SqliteUser>().Where(u => u.Name == "Alice_500").ToListAsync();
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
