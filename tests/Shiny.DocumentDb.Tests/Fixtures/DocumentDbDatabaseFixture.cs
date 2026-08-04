using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Shiny.DocumentDb.DocumentDb;
using Testcontainers.MongoDb;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

/// <summary>
/// Amazon DocumentDB conformance fixture. There is no Amazon DocumentDB container, so this uses the
/// wire-compatible <see cref="Testcontainers.MongoDb"/> container as a proxy — the provider is a thin
/// subclass of the MongoDB provider, so the inherited behaviour is identical over the same wire protocol.
/// TLS is disabled (the local Mongo container serves plain connections); the DocumentDB provider's TLS /
/// <c>retryWrites=false</c> connection handling is exercised by unit assertions in DocumentDbSpecificTests.
/// Full-text and vector are down-flagged by the provider, so those fixture surfaces are intentionally absent.
/// </summary>
public class DocumentDbDatabaseFixture : IDocumentStoreFixture, ITemporalDocumentStoreFixture, ISpatialDocumentStoreFixture, IAsyncLifetime
{
    MongoDbContainer container = null!;

    DocumentDbDocumentStoreOptions NewOptions(string tableName)
        => new()
        {
            ConnectionString = this.container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName,
            UseTls = false
        };

    public ITemporalDocumentStore CreateTemporalStore(string tableName, Action<TemporalOptions>? configure = null, Func<string>? actor = null)
    {
        var opts = this.NewOptions(tableName);
        opts.ConfigureDocument<VersionedUser>(cfg => cfg.MapTemporal(o =>
        {
            configure?.Invoke(o);
            if (actor != null)
                o.CaptureActor = actor;
        }));
        opts.ConfigureDocument<MergeDoc>(cfg => cfg.MapTemporal());
        return new DocumentDbDocumentStore(opts);
    }

    public IDocumentStore CreateStore(string tableName)
        => new DocumentDbDocumentStore(this.NewOptions(tableName));

    public IDocumentStore CreateSpatialStore(string tableName)
    {
        var opts = this.NewOptions(tableName);
        opts.ConfigureDocument<GeoZone>(cfg => cfg.MapSpatialProperty(z => z.Area));
        return new DocumentDbDocumentStore(opts);
    }

    /// <summary>Builds a store with caller-supplied provider-agnostic options — the hook the
    /// cross-provider conformance suites configure themselves through.</summary>
    public IDocumentStore CreateStore(string tableName, Action<IDocumentStoreOptions> configure)
    {
        var opts = this.NewOptions(tableName);
        configure(opts);
        return new DocumentDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = this.NewOptions(tableName);
        opts.ConfigureDocument<T>(cfg => cfg.MapVersionProperty(versionProperty));
        return new DocumentDbDocumentStore(opts);
    }

    public async ValueTask InitializeAsync()
    {
        this.container = new MongoDbBuilder().Build();
        await this.container.StartAsync();
    }

    public async ValueTask DisposeAsync()
        => await this.container.DisposeAsync();
}
