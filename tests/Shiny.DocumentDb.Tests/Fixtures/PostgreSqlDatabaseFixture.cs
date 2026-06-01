using System.Linq.Expressions;
using Shiny.DocumentDb.PostgreSql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class PostgreSqlDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, ITenantDocumentStoreFixture, IAsyncLifetime
{
    PostgreSqlContainer container = null!;

    public IDatabaseProvider CreateProvider()
        => new PostgreSqlDatabaseProvider(container.GetConnectionString());

    public async ValueTask InitializeAsync()
    {
        container = new PostgreSqlBuilder().Build();
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
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.AddQueryFilter(filter);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.AddQueryFilter(filterName, filter);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.MapVersionProperty(versionProperty);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithTenant(string tableName, Func<string> tenantIdAccessor)
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName,
            TenantIdAccessor = tenantIdAccessor
        });

    public async ValueTask DisposeAsync()
        => await container.DisposeAsync();
}
