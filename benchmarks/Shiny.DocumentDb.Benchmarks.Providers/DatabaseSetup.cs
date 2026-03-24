using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace Shiny.DocumentDb.Benchmarks.Providers;

public static class DatabaseSetup
{
    static PostgreSqlContainer? postgresContainer;
    static MsSqlContainer? sqlServerContainer;
    static int refCount;
    static readonly SemaphoreSlim semaphore = new(1, 1);

    public static async Task<string> GetConnectionString(string provider)
    {
        switch (provider)
        {
            case "SQLite":
                var path = Path.Combine(Path.GetTempPath(), $"bench_ef_{Guid.NewGuid():N}.db");
                return $"Data Source={path}";

            case "PostgreSQL":
                await EnsureContainersStarted();
                return postgresContainer!.GetConnectionString();

            case "SqlServer":
                await EnsureContainersStarted();
                return sqlServerContainer!.GetConnectionString();

            default:
                throw new ArgumentException($"Unknown provider: {provider}");
        }
    }

    public static string GetDocumentDbConnectionString(string provider, string efConnectionString)
    {
        // DocumentDb uses the same connection strings for PostgreSQL and SqlServer.
        // For SQLite, we use a separate file to avoid sharing the EF Core database.
        if (provider == "SQLite")
        {
            var path = Path.Combine(Path.GetTempPath(), $"bench_docdb_{Guid.NewGuid():N}.db");
            return $"Data Source={path}";
        }
        return efConnectionString;
    }

    static async Task EnsureContainersStarted()
    {
        await semaphore.WaitAsync();
        try
        {
            if (postgresContainer == null)
            {
                postgresContainer = new PostgreSqlBuilder()
                    .WithImage("postgres:16-alpine")
                    .Build();
                await postgresContainer.StartAsync();
            }
            if (sqlServerContainer == null)
            {
                sqlServerContainer = new MsSqlBuilder()
                    .WithImage("mcr.microsoft.com/mssql/server:2025-latest")
                    .Build();
                await sqlServerContainer.StartAsync();
            }
        }
        finally
        {
            semaphore.Release();
        }
    }

    public static async Task AddRef()
    {
        Interlocked.Increment(ref refCount);
        await Task.CompletedTask;
    }

    public static async Task Release()
    {
        if (Interlocked.Decrement(ref refCount) <= 0)
        {
            if (postgresContainer != null)
            {
                await postgresContainer.DisposeAsync();
                postgresContainer = null;
            }
            if (sqlServerContainer != null)
            {
                await sqlServerContainer.DisposeAsync();
                sqlServerContainer = null;
            }
        }
    }
}
