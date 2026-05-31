using Shiny.DocumentDb.MongoDb;
using Testcontainers.MongoDb;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class MongoDbDatabaseFixture : IDocumentStoreFixture, IAsyncLifetime
{
    MongoDbContainer container = null!;

    public IDocumentStore CreateStore(string tableName)
        => new MongoDbDocumentStore(new MongoDbDocumentStoreOptions
        {
            ConnectionString = container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName
        });

    public async ValueTask InitializeAsync()
    {
        container = new MongoDbBuilder().Build();
        await container.StartAsync();
    }

    public async ValueTask DisposeAsync()
        => await container.DisposeAsync();
}
