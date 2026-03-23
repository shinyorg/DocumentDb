using Shiny.DocumentDb.PostgreSql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class PostgreSqlDatabaseFixture : IDatabaseFixture, IAsyncLifetime
{
    PostgreSqlContainer container = null!;

    public IDatabaseProvider CreateProvider()
        => new PostgreSqlDatabaseProvider(container.GetConnectionString());

    public async ValueTask InitializeAsync()
    {
        container = new PostgreSqlBuilder().Build();
        await container.StartAsync();
    }

    public async ValueTask DisposeAsync()
        => await container.DisposeAsync();
}
