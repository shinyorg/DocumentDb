using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.SqlServer;

namespace Shiny.DocumentDb.Benchmarks.Providers;

[MemoryDiagnoser]
public class QueryBenchmarks
{
    DocumentStore docStore = null!;
    BenchmarkDbContext efContext = null!;
    string efConnectionString = null!;
    string docConnectionString = null!;

    [ParamsSource(nameof(Providers))]
    public string Provider { get; set; } = "";

    public static IEnumerable<string> Providers => ["SQLite", "PostgreSQL", "SqlServer"];

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await DatabaseSetup.AddRef();
        efConnectionString = await DatabaseSetup.GetConnectionString(Provider);
        docConnectionString = DatabaseSetup.GetDocumentDbConnectionString(Provider, efConnectionString);

        efContext = BenchmarkDbContext.Create(Provider, efConnectionString);
        docStore = CreateDocumentStore(Provider, docConnectionString);

        var ctx = BenchmarkJsonContext.Default;
        for (var i = 0; i < 1000; i++)
        {
            var user = new BenchmarkUser { Id = Guid.NewGuid().ToString("N"), Name = $"Alice_{i}", Age = 20 + (i % 50), Email = $"alice{i}@test.com" };
            await docStore.Insert(user, ctx.BenchmarkUser);

            efContext.Users.Add(new EfUser { Name = $"Alice_{i}", Age = 20 + (i % 50), Email = $"alice{i}@test.com" });
        }
        await efContext.SaveChangesAsync();
    }

    [Benchmark(Description = "DocumentDb Query")]
    public async Task<IReadOnlyList<BenchmarkUser>> DocumentDb_Query()
    {
        return await docStore.Query(BenchmarkJsonContext.Default.BenchmarkUser)
            .Where(u => u.Name == "Alice_500")
            .ToList();
    }

    [Benchmark(Description = "EF Core Query")]
    public async Task<List<EfUser>> EfCore_Query()
    {
        return await efContext.Users
            .Where(u => u.Name == "Alice_500")
            .ToListAsync();
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
