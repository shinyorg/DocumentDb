using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.SqlServer;

namespace Shiny.DocumentDb.Benchmarks.Providers;

[MemoryDiagnoser]
public class BatchInsertBenchmarks
{
    DocumentStore docStore = null!;
    BenchmarkDbContext efContext = null!;
    string efConnectionString = null!;
    string docConnectionString = null!;

    [ParamsSource(nameof(Providers))]
    public string Provider { get; set; } = "";

    public static IEnumerable<string> Providers => ["SQLite", "PostgreSQL", "SqlServer"];

    [Params(10, 100, 1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await DatabaseSetup.AddRef();
        efConnectionString = await DatabaseSetup.GetConnectionString(Provider);
        docConnectionString = DatabaseSetup.GetDocumentDbConnectionString(Provider, efConnectionString);

        efContext = BenchmarkDbContext.Create(Provider, efConnectionString);

        docStore = CreateDocumentStore(Provider, docConnectionString);
        await docStore.Clear<BenchmarkUser>();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        docStore.Clear<BenchmarkUser>().GetAwaiter().GetResult();
        efContext.Users.ExecuteDelete();
        efContext.ChangeTracker.Clear();
    }

    [Benchmark(Description = "DocumentDb BatchInsert")]
    public async Task DocumentDb_BatchInsert()
    {
        var ctx = BenchmarkJsonContext.Default;
        var users = Enumerable.Range(0, Count).Select(i =>
            new BenchmarkUser { Id = Guid.NewGuid().ToString("N"), Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" }
        );
        await docStore.BatchInsert(users, ctx.BenchmarkUser);
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
        docStore.Dispose();
        await efContext.DisposeAsync();
        if (Provider == "SQLite")
        {
            TryDelete(docConnectionString);
            TryDelete(efConnectionString);
        }
        await DatabaseSetup.Release();
    }

    static void TryDelete(string connectionString)
    {
        var path = connectionString.Replace("Data Source=", "");
        try { File.Delete(path); } catch { }
    }

    static DocumentStore CreateDocumentStore(string provider, string connectionString) => provider switch
    {
        "SQLite" => new SqliteDocumentStore(connectionString),
        "PostgreSQL" => new DocumentStore(new DocumentStoreOptions { DatabaseProvider = new PostgreSqlDatabaseProvider(connectionString) }),
        "SqlServer" => new DocumentStore(new DocumentStoreOptions { DatabaseProvider = new SqlServerDatabaseProvider(connectionString) }),
        _ => throw new ArgumentException($"Unknown provider: {provider}")
    };
}
