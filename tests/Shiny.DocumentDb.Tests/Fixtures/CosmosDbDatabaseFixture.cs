using System.Linq.Expressions;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.CosmosDb;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class CosmosDbDatabaseFixture : IDocumentStoreFixture, ITemporalDocumentStoreFixture, ISpatialDocumentStoreFixture, IAsyncLifetime
{
    IContainer container = null!;
    string connectionString = null!;
    CosmosClient? sharedClient;

    const int CosmosPort = 8081;
    const string CosmosKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    public ITemporalDocumentStore CreateTemporalStore(string tableName, Action<TemporalOptions>? configure = null, Func<string>? actor = null)
    {
        var opts = new CosmosDbDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            DatabaseName = "test",
            ContainerName = tableName,
            CosmosClient = this.sharedClient
        };
        opts.MapTemporal<VersionedUser>(o =>
        {
            configure?.Invoke(o);
            if (actor != null)
                o.CaptureActor = actor;
        });
        opts.MapTemporal<MergeDoc>();
        return new CosmosDbDocumentStore(opts);
    }

    public IDocumentStore CreateStore(string tableName)
    {
        return new CosmosDbDocumentStore(new CosmosDbDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            DatabaseName = "test",
            ContainerName = tableName,
            CosmosClient = this.sharedClient
        });
    }

    public IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = new CosmosDbDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            DatabaseName = "test",
            ContainerName = tableName,
            CosmosClient = this.sharedClient
        };
        opts.AddQueryFilter(filter);
        return new CosmosDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = new CosmosDbDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            DatabaseName = "test",
            ContainerName = tableName,
            CosmosClient = this.sharedClient
        };
        opts.AddQueryFilter(filterName, filter);
        return new CosmosDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = new CosmosDbDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            DatabaseName = "test",
            ContainerName = tableName,
            CosmosClient = this.sharedClient
        };
        opts.MapVersionProperty(versionProperty);
        return new CosmosDbDocumentStore(opts);
    }

    public IDocumentStore CreateVectorStore(string tableName, int dimensions = 4, VectorDistance metric = VectorDistance.Cosine, VectorIndexKind indexKind = VectorIndexKind.DiskAnn)
    {
        var opts = new CosmosDbDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            DatabaseName = "test",
            ContainerName = tableName,
            CosmosClient = this.sharedClient
        };
        opts.MapVectorProperty<VectorDoc>(d => d.Embedding, dimensions, metric, indexKind);
        return new CosmosDbDocumentStore(opts);
    }

    public IDocumentStore CreateFullTextStore(string tableName)
    {
        var opts = new CosmosDbDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            DatabaseName = "test",
            ContainerName = tableName,
            CosmosClient = this.sharedClient
        };
        opts.MapFullTextProperty<FtArticle>([a => a.Title, a => a.Body]);
        return new CosmosDbDocumentStore(opts);
    }

    public IDocumentStore CreateComputedStore(string tableName)
    {
        var opts = new CosmosDbDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            DatabaseName = "test",
            ContainerName = tableName,
            CosmosClient = this.sharedClient
        };
        opts.MapComputedProperty<ComputedSale, int>(s => s.LineTotalCents, s => s.Quantity * s.UnitPriceCents);
        opts.MapComputedProperty<ComputedSale, string>(s => s.FullName, s => s.First + " " + s.Last);
        return new CosmosDbDocumentStore(opts);
    }

    public IDocumentStore CreateSpatialStore(string tableName)
    {
        var opts = new CosmosDbDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            DatabaseName = "test",
            ContainerName = tableName,
            CosmosClient = this.sharedClient
        };
        opts.MapSpatialProperty<GeoZone>(z => z.Area);
        return new CosmosDbDocumentStore(opts);
    }

    public async ValueTask InitializeAsync()
    {
        container = new ContainerBuilder()
            .WithImage("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview")
            .WithPortBinding(CosmosPort, CosmosPort)
            .WithCommand("--protocol", "https")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilMessageIsLogged("Gateway=OK"))
            .Build();

        await container.StartAsync();

        this.connectionString = $"AccountEndpoint=https://127.0.0.1:{CosmosPort}/;AccountKey={CosmosKey}";

        this.sharedClient = new CosmosClient(this.connectionString, new CosmosClientOptions
        {
            ConnectionMode = ConnectionMode.Gateway,
            HttpClientFactory = () =>
            {
                var handler = new HttpClientHandler
                {
                    ServerCertificateCustomValidationCallback =
                        HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
                };
                return new HttpClient(handler);
            },
            SerializerOptions = new CosmosSerializationOptions
            {
                PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase
            }
        });
    }

    public async ValueTask DisposeAsync()
    {
        this.sharedClient?.Dispose();
        await container.DisposeAsync();
    }
}
