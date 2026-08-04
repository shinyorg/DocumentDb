using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Shiny.DocumentDb.DuckDb;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class DuckDbDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, ITenantDocumentStoreFixture, ITemporalDocumentStoreFixture, ISpatialDocumentStoreFixture
{
    public ITemporalDocumentStore CreateTemporalStore(string tableName, Action<TemporalOptions>? configure = null, Func<string>? actor = null)
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.ConfigureDocument<VersionedUser>(cfg => cfg.MapTemporal(o =>
        {
            configure?.Invoke(o);
            if (actor != null)
                o.CaptureActor = actor;
        }));
        opts.ConfigureDocument<MergeDoc>(cfg => cfg.MapTemporal());
        return new DocumentStore(opts);
    }

    public IDatabaseProvider CreateProvider()
        => new DuckDbDatabaseProvider("Data Source=:memory:");

    public IDocumentStore CreateStore(string tableName)
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        });

    /// <summary>Builds a store with caller-supplied provider-agnostic options — the hook the
    /// cross-provider conformance suites configure themselves through.</summary>
    public IDocumentStore CreateStore(string tableName, Action<IDocumentStoreOptions> configure)
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        configure(opts);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = new DocumentStoreOptions { DatabaseProvider = this.CreateProvider(), TableName = tableName };
        opts.ConfigureDocument<T>(cfg => cfg.MapVersionProperty(versionProperty));
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithTenant(string tableName, Func<string> tenantIdAccessor)
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName,
            TenantIdAccessor = tenantIdAccessor
        });

    public IDocumentStore CreateFullTextStore(string tableName)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.ConfigureDocument<FtArticle>(cfg => cfg.MapFullTextProperty([a => a.Title, a => a.Body]));
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateSpatialStore(string tableName)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.ConfigureDocument<GeoZone>(cfg => cfg.MapSpatialProperty(z => z.Area));
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateComputedStore(string tableName)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.ConfigureDocument<ComputedSale>(cfg =>
        {
            cfg.MapComputedProperty<int>(s => s.LineTotalCents, s => s.Quantity * s.UnitPriceCents, indexed: true);
            cfg.MapComputedProperty<string>(s => s.FullName, s => s.First + " " + s.Last);
        });
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateVectorStore(string tableName, int dimensions = 4, VectorDistance metric = VectorDistance.Cosine, VectorIndexKind indexKind = VectorIndexKind.Hnsw)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.ConfigureDocument<VectorDoc>(cfg => cfg.MapVectorProperty(d => d.Embedding, dimensions, metric, indexKind));
        return new DocumentStore(opts);
    }
}
