using System.Linq.Expressions;
using Shiny.DocumentDb.MySql;
using Testcontainers.MySql;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class MySqlDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, IAsyncLifetime
{
    MySqlContainer container = null!;

    public IDatabaseProvider CreateProvider()
        => new MySqlDatabaseProvider(container.GetConnectionString());

    public async ValueTask InitializeAsync()
    {
        container = new MySqlBuilder().Build();
        await container.StartAsync();
    }

    public IDocumentStore CreateStore(string tableName)
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        });

    public IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.AddQueryFilter(filter);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.AddQueryFilter(filterName, filter);
        return new DocumentStore(opts);
    }

    public async ValueTask DisposeAsync()
        => await container.DisposeAsync();
}
