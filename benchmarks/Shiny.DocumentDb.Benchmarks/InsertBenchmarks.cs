using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using SQLite;

namespace Shiny.DocumentDb.Benchmarks;

[MemoryDiagnoser]
public class InsertBenchmarks
{
    SqliteDocumentStore store = null!;
    SQLiteAsyncConnection db = null!;
    BenchDbContext efContext = null!;
    SqliteConnection dapper = null!;
    string storePath = null!;
    string sqlitePath = null!;
    string efPath = null!;
    string dapperPath = null!;

    [Params(10, 100, 1000)]
    public int Count { get; set; }

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

        dapper.Execute("DELETE FROM Users;");
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

    [Benchmark(Description = "EF Core Insert")]
    public async Task EfCore_Insert()
    {
        for (var i = 0; i < Count; i++)
        {
            efContext.Users.Add(new EfUser { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" });
            await efContext.SaveChangesAsync();
        }
    }

    [Benchmark(Description = "Dapper Insert")]
    public async Task Dapper_Insert()
    {
        for (var i = 0; i < Count; i++)
        {
            await dapper.ExecuteAsync(
                "INSERT INTO Users (Name, Age, Email) VALUES (@Name, @Age, @Email);",
                new { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" });
        }
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
