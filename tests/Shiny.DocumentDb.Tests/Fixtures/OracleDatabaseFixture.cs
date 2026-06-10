using System.Linq.Expressions;
using Shiny.DocumentDb.Oracle;
using Testcontainers.Oracle;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class OracleDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, ITenantDocumentStoreFixture, IAsyncLifetime
{
    OracleContainer container = null!;

    public IDatabaseProvider CreateProvider()
        => new OracleDatabaseProvider(container.GetConnectionString());

    public async ValueTask InitializeAsync()
    {
        // The provider needs Oracle 23ai+ (multi-row VALUES, CREATE INDEX IF NOT EXISTS, JSON());
        // gvenzl/oracle-free 23 also ships arm64 images for Apple Silicon dev boxes
        container = new OracleBuilder()
            .WithImage("gvenzl/oracle-free:23-slim-faststart")
            .Build();
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
