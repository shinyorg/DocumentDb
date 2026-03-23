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
        container = new MsSqlBuilder().Build();
        await container.StartAsync();
    }

    public async ValueTask DisposeAsync()
        => await container.DisposeAsync();
}
