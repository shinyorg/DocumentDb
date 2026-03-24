using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.SqlServer;

namespace Shiny.DocumentDb.Benchmarks.Providers;

[MemoryDiagnoser]
public class InsertBenchmarks
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

    [Benchmark(Description = "DocumentDb Insert")]
    public async Task DocumentDb_Insert()
    {
        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < Count; i++)
        {
            var user = new BenchmarkUser { Id = Guid.NewGuid().ToString("N"), Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            await docStore.Insert(user, ctx.BenchmarkUser);
        }
    }

    [Benchmark(Description = "EF Core Insert")]
    public async Task EfCore_Insert()
    {
        for (var i = 0; i < Count; i++)
        {
            var user = new EfUser { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            efContext.Users.Add(user);
            await efContext.SaveChangesAsync();
        }
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        docStore.Dispose();
        await efContext.DisposeAsync();
        CleanupSqliteFiles();
        await DatabaseSetup.Release();
    }

    void CleanupSqliteFiles()
    {
        if (Provider != "SQLite") return;
        TryDelete(docConnectionString);
        TryDelete(efConnectionString);
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
