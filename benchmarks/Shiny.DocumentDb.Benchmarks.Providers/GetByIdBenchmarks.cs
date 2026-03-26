using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.SqlServer;

namespace Shiny.DocumentDb.Benchmarks.Providers;

[MemoryDiagnoser]
public class GetByIdBenchmarks
{
    DocumentStore docStore = null!;
    BenchmarkDbContext efContext = null!;
    string efConnectionString = null!;
    string docConnectionString = null!;
    string knownDocId = null!;
    int knownEfId;

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
            var user = new BenchmarkUser { Id = Guid.NewGuid().ToString("N"), Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            await docStore.Insert(user, ctx.BenchmarkUser);
            if (i == 500) knownDocId = user.Id;

            var efUser = new EfUser { Name = $"User_{i}", Age = 20 + (i % 50), Email = $"user{i}@test.com" };
            efContext.Users.Add(efUser);
            await efContext.SaveChangesAsync();
            if (i == 500) knownEfId = efUser.Id;
        }
    }

    [Benchmark(Description = "DocumentDb GetById")]
    public async Task<BenchmarkUser?> DocumentDb_GetById()
    {
        return await docStore.Get<BenchmarkUser>(knownDocId, BenchmarkJsonContext.Default.BenchmarkUser);
    }

    [Benchmark(Description = "EF Core FindAsync")]
    public async Task<EfUser?> EfCore_FindAsync()
    {
        return await efContext.Users.FindAsync(knownEfId);
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
