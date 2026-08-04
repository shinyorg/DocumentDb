using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Shiny.DocumentDb.Oracle;
using Testcontainers.Oracle;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class OracleDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, ITenantDocumentStoreFixture, ITemporalDocumentStoreFixture, ISpatialDocumentStoreFixture, IAsyncLifetime
{
    OracleContainer container = null!;

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
        // The test Oracle image has no Oracle Spatial (MDSYS.SDO_GEOM) — use the dependency-free envelope tier.
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new OracleDatabaseProvider(container.GetConnectionString()) { PortableSpatial = true },
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

    public IDocumentStore CreateVectorStore(string tableName, int dimensions = 4, VectorDistance metric = VectorDistance.Cosine, VectorIndexKind indexKind = VectorIndexKind.None)
    {
        // Default to IndexKind.None — CREATE VECTOR INDEX needs the vector_memory_size pool, which
        // the Free image doesn't configure. NearestVectors works under VECTOR_DISTANCE regardless.
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.ConfigureDocument<VectorDoc>(cfg => cfg.MapVectorProperty(d => d.Embedding, dimensions, metric, indexKind));
        return new DocumentStore(opts);
    }

    public async ValueTask DisposeAsync()
        => await container.DisposeAsync();
}
