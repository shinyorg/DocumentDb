using Testcontainers.MongoDb;
using Testcontainers.PostgreSql;
using Xunit;

namespace Shiny.DocumentDb.Orleans.Tests;

public sealed class PostgresFixture : IAsyncLifetime
{
    public PostgreSqlContainer Container { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        this.Container = new PostgreSqlBuilder().Build();
        await this.Container.StartAsync();
    }

    public async ValueTask DisposeAsync() => await this.Container.DisposeAsync();
}

public sealed class MongoFixture : IAsyncLifetime
{
    public MongoDbContainer Container { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        this.Container = new MongoDbBuilder().Build();
        await this.Container.StartAsync();
    }

    public async ValueTask DisposeAsync() => await this.Container.DisposeAsync();
}

[CollectionDefinition("Postgres")]
public class PostgresCollection : ICollectionFixture<PostgresFixture>;

[CollectionDefinition("Mongo")]
public class MongoCollection : ICollectionFixture<MongoFixture>;
