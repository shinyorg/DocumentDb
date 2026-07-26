using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Images;
using Shiny.DocumentDb.PostgreSql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class PostgreSqlDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, ITenantDocumentStoreFixture, ITemporalDocumentStoreFixture, ISpatialDocumentStoreFixture, IAsyncLifetime
{
    PostgreSqlContainer container = null!;

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
        => new PostgreSqlDatabaseProvider(container.GetConnectionString());

    public async ValueTask InitializeAsync()
    {
        // Build a combined PostGIS + pgvector image (multi-arch incl. arm64) so the same container serves the
        // vector, full-text, computed, temporal AND native-spatial (PostGIS ST_*) test surfaces. The build is
        // cached by Docker after the first run.
        var image = new ImageFromDockerfileBuilder()
            .WithDockerfileDirectory(CommonDirectoryPath.GetSolutionDirectory(), "tests/Shiny.DocumentDb.Tests/Fixtures/postgis-pgvector")
            .WithName("shiny-postgis-pgvector:pg17")
            .WithDeleteIfExists(false)
            .Build();
        await image.CreateAsync();

        container = new PostgreSqlBuilder()
            .WithImage(image)
            .Build();
        await container.StartAsync();
    }

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

    public IDocumentStore CreateSpatialStore(string tableName)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.MapSpatialProperty<GeoZone>(z => z.Area);
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

    public IDocumentStore CreateVectorStore(string tableName, int dimensions = 4, VectorDistance metric = VectorDistance.Cosine, VectorIndexKind indexKind = VectorIndexKind.Hnsw)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        opts.MapVectorProperty<VectorDoc>(d => d.Embedding, dimensions, metric, indexKind);
        return new DocumentStore(opts);
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
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.CreateProvider(),
            TableName = tableName
        };
        configure(opts);
        return new DocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
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
