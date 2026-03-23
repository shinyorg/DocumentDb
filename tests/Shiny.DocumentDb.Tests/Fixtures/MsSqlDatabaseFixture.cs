using Shiny.DocumentDb.SqlServer;
using Testcontainers.MsSql;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class MsSqlDatabaseFixture : IDatabaseFixture, IAsyncLifetime
{
    MsSqlContainer container = null!;

    public IDatabaseProvider CreateProvider()
        => new SqlServerDatabaseProvider(container.GetConnectionString());

    public async ValueTask InitializeAsync()
    {
        container = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2025-latest")
            .Build();
        await container.StartAsync();
    }

    public async ValueTask DisposeAsync()
        => await container.DisposeAsync();
}
