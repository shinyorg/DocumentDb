using Shiny.DocumentDb.MySql;
using Testcontainers.MySql;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class MySqlDatabaseFixture : IDatabaseFixture, IAsyncLifetime
{
    MySqlContainer container = null!;

    public IDatabaseProvider CreateProvider()
        => new MySqlDatabaseProvider(container.GetConnectionString());

    public async ValueTask InitializeAsync()
    {
        container = new MySqlBuilder().Build();
        await container.StartAsync();
    }

    public async ValueTask DisposeAsync()
        => await container.DisposeAsync();
}
