using System.Linq.Expressions;
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

    public IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = new MongoDbDocumentStoreOptions
        {
            ConnectionString = container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName
        };
        opts.AddQueryFilter(filter);
        return new MongoDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = new MongoDbDocumentStoreOptions
        {
            ConnectionString = container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName
        };
        opts.AddQueryFilter(filterName, filter);
        return new MongoDbDocumentStore(opts);
    }

    public async ValueTask InitializeAsync()
    {
        container = new MongoDbBuilder().Build();
        await container.StartAsync();
    }

    public async ValueTask DisposeAsync()
        => await container.DisposeAsync();
}
