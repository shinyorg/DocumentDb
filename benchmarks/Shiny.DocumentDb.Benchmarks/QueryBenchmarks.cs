using BenchmarkDotNet.Attributes;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using SQLite;

namespace Shiny.DocumentDb.Benchmarks;

[MemoryDiagnoser]
public class QueryBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    BenchDbContext efContext = null!;
    string storePath = null!;
    string sqlitePath = null!;
    string efPath = null!;

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

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < 1000; i++)
        {
            var user = new BenchmarkUser { Name = $"Alice_{i}", Age = 20 + (i % 50), Email = $"alice{i}@test.com" };
            await store.Insert(user, ctx.BenchmarkUser);

            var sqliteUser = new SqliteUser { DocId = Guid.NewGuid().ToString("N"), Name = $"Alice_{i}", Age = 20 + (i % 50), Email = $"alice{i}@test.com" };
            await db.InsertAsync(sqliteUser);

            efContext.Users.Add(new EfUser { Name = $"Alice_{i}", Age = 20 + (i % 50), Email = $"alice{i}@test.com" });
        }
        await efContext.SaveChangesAsync();
        efContext.ChangeTracker.Clear();
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

    [Benchmark(Description = "EF Core Query (compiled)")]
    public async Task<List<EfUser>> EfCore_Query()
    {
        return await BenchDbContext.QueryUsersByName(efContext, "Alice_500").ToListAsync();
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
