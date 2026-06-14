using BenchmarkDotNet.Attributes;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using SQLite;

namespace Shiny.DocumentDb.Benchmarks;

[MemoryDiagnoser]
public class GetAllBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    BenchDbContext efContext = null!;
    string storePath = null!;
    string sqlitePath = null!;
    string efPath = null!;

    [Params(100, 1000)]
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

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < Count; i++)
        {
            var user = new BenchmarkUser { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            await store.Insert(user, ctx.BenchmarkUser);

            var sqliteUser = new SqliteUser { DocId = Guid.NewGuid().ToString("N"), Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            await db.InsertAsync(sqliteUser);

            efContext.Users.Add(new EfUser { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" });
        }
        await efContext.SaveChangesAsync();
        efContext.ChangeTracker.Clear();
    }

    [Benchmark(Description = "DocumentStore GetAll")]
    public async Task<IReadOnlyList<BenchmarkUser>> DocumentStore_GetAll()
    {
        return await store.Query(BenchmarkJsonContext.Default.BenchmarkUser).ToList();
    }

    [Benchmark(Description = "sqlite-net GetAll")]
    public async Task<List<SqliteUser>> SqliteNet_GetAll()
    {
        return await db.Table<SqliteUser>().ToListAsync();
    }

    [Benchmark(Description = "EF Core GetAll (compiled)")]
    public async Task<List<EfUser>> EfCore_GetAll()
    {
        return await BenchDbContext.GetAllUsers(efContext).ToListAsync();
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
