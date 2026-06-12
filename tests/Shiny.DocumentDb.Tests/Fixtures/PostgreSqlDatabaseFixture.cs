using System.Linq.Expressions;
using Shiny.DocumentDb.PostgreSql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class PostgreSqlDatabaseFixture : IDatabaseFixture, IDocumentStoreFixture, ITenantDocumentStoreFixture, ITemporalDocumentStoreFixture, IAsyncLifetime
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
        // pgvector image bundles the `vector` extension; the rest of the test surface uses
        // identical SQL so this is a drop-in for PostgreSqlBuilder's default image.
        container = new PostgreSqlBuilder()
            .WithImage("pgvector/pgvector:pg17")
            .Build();
        await container.StartAsync();
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
