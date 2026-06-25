using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Data.Sqlite;
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
    SqliteConnection dapper = null!;
    string storePath = null!;
    string sqlitePath = null!;
    string efPath = null!;
    string dapperPath = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        storePath = Path.Combine(Path.GetTempPath(), $"bench_store_{Guid.NewGuid():N}.db");
        sqlitePath = Path.Combine(Path.GetTempPath(), $"bench_sqlite_{Guid.NewGuid():N}.db");
        efPath = Path.Combine(Path.GetTempPath(), $"bench_ef_{Guid.NewGuid():N}.db");
        dapperPath = Path.Combine(Path.GetTempPath(), $"bench_dapper_{Guid.NewGuid():N}.db");

        store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={storePath}")
        });

        db = new SQLiteAsyncConnection(sqlitePath);
        await db.CreateTableAsync<SqliteUser>();

        efContext = BenchDbContext.Create(efPath);

        dapper = DapperSetup.CreateFlat(dapperPath);

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < 1000; i++)
        {
            var user = new BenchmarkUser { Name = $"Alice_{i}", Age = 20 + (i % 50), Email = $"alice{i}@test.com" };
            await store.Insert(user, ctx.BenchmarkUser);

            var sqliteUser = new SqliteUser { DocId = Guid.NewGuid().ToString("N"), Name = $"Alice_{i}", Age = 20 + (i % 50), Email = $"alice{i}@test.com" };
            await db.InsertAsync(sqliteUser);

            efContext.Users.Add(new EfUser { Name = $"Alice_{i}", Age = 20 + (i % 50), Email = $"alice{i}@test.com" });

            await dapper.ExecuteAsync(
                "INSERT INTO Users (Name, Age, Email) VALUES (@Name, @Age, @Email);",
                new { Name = $"Alice_{i}", Age = 20 + (i % 50), Email = $"alice{i}@test.com" });
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

    [Benchmark(Description = "Dapper Query")]
    public async Task<List<DapperUser>> Dapper_Query()
    {
        var rows = await dapper.QueryAsync<DapperUser>(
            "SELECT Id, Name, Age, Email FROM Users WHERE Name = @name;",
            new { name = "Alice_500" });
        return rows.AsList();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        store.Dispose();
        db.GetConnection().Close();
        await efContext.DisposeAsync();
        dapper.Dispose();
        File.Delete(storePath);
        File.Delete(sqlitePath);
        File.Delete(efPath);
        File.Delete(dapperPath);
    }
}
