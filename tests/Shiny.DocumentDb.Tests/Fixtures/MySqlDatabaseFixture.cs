using System.Linq.Expressions;
using Shiny.DocumentDb.MySql;
using Testcontainers.MySql;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class MySqlDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, ITenantDocumentStoreFixture, ITemporalDocumentStoreFixture, IAsyncLifetime
{
    MySqlContainer container = null!;

    public ITemporalDocumentStore CreateTemporalStore(string tableName, Action<TemporalOptions>? configure = null, Func<string>? actor = null)
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.MapTemporal<VersionedUser>(o =>
        {
            configure?.Invoke(o);
            if (actor != null)
                o.CaptureActor = actor;
        });
        opts.MapTemporal<MergeDoc>();
        return new DocumentStore(opts);
    }

    public IDatabaseProvider CreateProvider()
        => new MySqlDatabaseProvider(container.GetConnectionString());

    public IDocumentStore CreateFullTextStore(string tableName)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.MapFullTextProperty<FtArticle>([a => a.Title, a => a.Body]);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateComputedStore(string tableName)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.MapComputedProperty<ComputedSale, int>(s => s.LineTotalCents, s => s.Quantity * s.UnitPriceCents, indexed: true);
        opts.MapComputedProperty<ComputedSale, string>(s => s.FullName, s => s.First + " " + s.Last);
        return new DocumentStore(opts);
    }

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
