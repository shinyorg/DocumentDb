using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using SQLite;

namespace Shiny.DocumentDb.Benchmarks;

[MemoryDiagnoser]
public class GetByIdBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    BenchDbContext efContext = null!;
    SqliteConnection dapper = null!;
    string storePath = null!;
    string sqlitePath = null!;
    string efPath = null!;
    string dapperPath = null!;
    string knownDocId = null!;
    int knownSqliteId;
    int knownEfId;
    long knownDapperId;

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
            var user = new BenchmarkUser { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            await store.Insert(user, ctx.BenchmarkUser);
            if (i == 500) knownDocId = user.Id;

            var sqliteUser = new SqliteUser { DocId = Guid.NewGuid().ToString("N"), Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            await db.InsertAsync(sqliteUser);
            if (i == 500) knownSqliteId = sqliteUser.Id;

            var efUser = new EfUser { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            efContext.Users.Add(efUser);
            await efContext.SaveChangesAsync();
            if (i == 500) knownEfId = efUser.Id;

            var dapperId = await dapper.ExecuteScalarAsync<long>(
                "INSERT INTO Users (Name, Age, Email) VALUES (@Name, @Age, @Email); SELECT last_insert_rowid();",
                new { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" });
            if (i == 500) knownDapperId = dapperId;
        }
        efContext.ChangeTracker.Clear();
    }

    [Benchmark(Description = "DocumentStore GetById")]
    public async Task<BenchmarkUser?> DocumentStore_GetById()
    {
        return await store.Get<BenchmarkUser>(knownDocId, BenchmarkJsonContext.Default.BenchmarkUser);
    }

    [Benchmark(Description = "sqlite-net GetById")]
    public async Task<SqliteUser?> SqliteNet_GetById()
    {
        return await db.GetAsync<SqliteUser>(knownSqliteId);
    }

    [Benchmark(Description = "EF Core GetById (compiled)")]
    public async Task<EfUser?> EfCore_GetById()
    {
        return await BenchDbContext.GetUserById(efContext, knownEfId);
    }

    [Benchmark(Description = "Dapper GetById")]
    public async Task<DapperUser?> Dapper_GetById()
    {
        return await dapper.QuerySingleOrDefaultAsync<DapperUser>(
            "SELECT Id, Name, Age, Email FROM Users WHERE Id = @id;",
            new { id = knownDapperId });
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
