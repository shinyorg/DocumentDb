using System.Linq.Expressions;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class SqliteDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, ITenantDocumentStoreFixture
{
    public IDatabaseProvider CreateProvider()
        => new SqliteDatabaseProvider("Data Source=:memory:");

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
}
