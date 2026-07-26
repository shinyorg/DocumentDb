using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using MongoDB.Bson;
using MongoDB.Driver;
using Shiny.DocumentDb.MongoDb;
using Testcontainers.MongoDb;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class MongoDbDatabaseFixture : IDocumentStoreFixture, ITemporalDocumentStoreFixture, ISpatialDocumentStoreFixture, IAsyncLifetime
{
    MongoDbContainer container = null!;
    IContainer? atlasContainer;
    string? atlasConnectionString;
    bool atlasReady;

    /// <summary>Builds a store with caller-supplied option tweaks (e.g. blob mappings).</summary>
    public MongoDbDocumentStore CreateConfiguredStore(string collection, Action<MongoDbDocumentStoreOptions> configure)
    {
        var opts = new MongoDbDocumentStoreOptions
        {
            ConnectionString = this.container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = collection
        };
        configure(opts);
        return new MongoDbDocumentStore(opts);
    }

    public ITemporalDocumentStore CreateTemporalStore(string tableName, Action<TemporalOptions>? configure = null, Func<string>? actor = null)
    {
        var opts = new MongoDbDocumentStoreOptions
        {
            ConnectionString = container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName
        };
        opts.MapTemporal<VersionedUser>(o =>
        {
            configure?.Invoke(o);
            if (actor != null)
                o.CaptureActor = actor;
        });
        opts.MapTemporal<MergeDoc>();
        return new MongoDbDocumentStore(opts);
    }

    public IDocumentStore CreateStore(string tableName)
        => new MongoDbDocumentStore(new MongoDbDocumentStoreOptions
        {
            ConnectionString = container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName
        });

    public IDocumentStore CreateStoreWithJsonOptions(string tableName, System.Text.Json.JsonSerializerOptions jsonOptions)
        => new MongoDbDocumentStore(new MongoDbDocumentStoreOptions
        {
            ConnectionString = container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName,
            JsonSerializerOptions = jsonOptions
        });

    public IDocumentStore CreateFullTextStore(string tableName)
    {
        var opts = new MongoDbDocumentStoreOptions
        {
            ConnectionString = container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName
        };
        opts.MapFullTextProperty<FtArticle>([a => a.Title, a => a.Body]);
        return new MongoDbDocumentStore(opts);
    }

    public IDocumentStore CreateComputedStore(string tableName)
    {
        var opts = new MongoDbDocumentStoreOptions
        {
            ConnectionString = container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName
        };
        opts.MapComputedProperty<ComputedSale, int>(s => s.LineTotalCents, s => s.Quantity * s.UnitPriceCents);
        opts.MapComputedProperty<ComputedSale, string>(s => s.FullName, s => s.First + " " + s.Last);
        return new MongoDbDocumentStore(opts);
    }

    public IDocumentStore CreateSpatialStore(string tableName)
    {
        var opts = new MongoDbDocumentStoreOptions
        {
            ConnectionString = container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName
        };
        opts.MapSpatialProperty<GeoZone>(z => z.Area);
        return new MongoDbDocumentStore(opts);
    }

    /// <summary>Builds a store with caller-supplied provider-agnostic options — the hook the
    /// cross-provider conformance suites configure themselves through.</summary>
    public IDocumentStore CreateStore(string tableName, Action<IDocumentStoreOptions> configure)
    {
        var opts = new MongoDbDocumentStoreOptions
        {
            ConnectionString = container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName
        };
        configure(opts);
        return new MongoDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = new MongoDbDocumentStoreOptions
        {
            ConnectionString = container.GetConnectionString(),
            DatabaseName = "test",
            CollectionName = tableName
        };
        opts.MapVersionProperty(versionProperty);
        return new MongoDbDocumentStore(opts);
    }

    public async ValueTask InitializeAsync()
    {
        container = new MongoDbBuilder().Build();
        await container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await container.DisposeAsync();
        if (this.atlasContainer != null)
            await this.atlasContainer.DisposeAsync();
    }

    /// <summary>
    /// Starts the mongodb-atlas-local image on demand for vector tests. The image bundles
    /// mongot (Atlas Search) so `$vectorSearch` works locally — vanilla MongoDB cannot run
    /// vector search aggregations.
    /// </summary>
    public async Task<string> EnsureAtlasLocalAsync()
    {
        if (this.atlasReady) return this.atlasConnectionString!;

        // The "Atlas Local successfully (re)started" log line was dropped in 8.x — wait on
        // the port being reachable instead, which is the universal signal mongod accepts traffic.
        this.atlasContainer = new ContainerBuilder()
            .WithImage("mongodb/mongodb-atlas-local:latest")
            .WithPortBinding(0, 27017)
            .WithWaitStrategy(Wait.ForUnixContainer()
                // The atlas-local image declares HEALTHCHECK that probes mongosh + mongot —
                // wait for it to report healthy (much more reliable than a log marker).
                .UntilContainerIsHealthy())
            .Build();

        await this.atlasContainer.StartAsync();
        var host = this.atlasContainer.Hostname;
        var port = this.atlasContainer.GetMappedPublicPort(27017);
        this.atlasConnectionString = $"mongodb://{host}:{port}/?directConnection=true";
        this.atlasReady = true;
        return this.atlasConnectionString;
    }

    public IDocumentStore CreateAtlasVectorStore(string collectionName, int dimensions, VectorDistance metric = VectorDistance.Cosine)
    {
        if (!this.atlasReady)
            throw new InvalidOperationException("Call EnsureAtlasLocalAsync() before CreateAtlasVectorStore().");

        var opts = new MongoDbDocumentStoreOptions
        {
            ConnectionString = this.atlasConnectionString!,
            DatabaseName = "test",
            CollectionName = collectionName
        };
        opts.MapVectorProperty<VectorDoc>(d => d.Embedding, dimensions, metric);
        return new MongoDbDocumentStore(opts);
    }

    /// <summary>
    /// Atlas Local needs the vector search index created server-side before $vectorSearch
    /// works. The driver exposes this via CreateSearchIndexes.
    /// </summary>
    public async Task CreateAtlasVectorIndexAsync(string collectionName, string indexName, int dimensions, string similarity = "cosine")
    {
        var client = new MongoClient(this.atlasConnectionString);
        var db = client.GetDatabase("test");
        var coll = db.GetCollection<BsonDocument>(collectionName);

        // Make sure the collection exists.
        try { await db.CreateCollectionAsync(collectionName); } catch { /* exists */ }

        var indexDef = new BsonDocument
        {
            { "name", indexName },
            { "type", "vectorSearch" },
            { "definition", new BsonDocument
                {
                    { "fields", new BsonArray
                        {
                            new BsonDocument
                            {
                                { "type", "vector" },
                                { "path", $"data.embedding" },
                                { "numDimensions", dimensions },
                                { "similarity", similarity }
                            },
                            new BsonDocument
                            {
                                { "type", "filter" },
                                { "path", "typeName" }
                            }
                        }
                    }
                }
            }
        };

        await db.RunCommandAsync<BsonDocument>(new BsonDocument
        {
            { "createSearchIndexes", collectionName },
            { "indexes", new BsonArray { indexDef } }
        });

        // Atlas Local needs a moment for the index to become queryable.
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            var listed = await db.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                { "aggregate", collectionName },
                { "pipeline", new BsonArray { new BsonDocument { { "$listSearchIndexes", new BsonDocument() } } } },
                { "cursor", new BsonDocument() }
            });
            var batch = listed["cursor"]["firstBatch"].AsBsonArray;
            var match = batch.Cast<BsonDocument>().FirstOrDefault(d => d["name"] == indexName);
            if (match != null && match.GetValue("queryable", false).ToBoolean())
                return;

            await Task.Delay(500);
        }
        throw new TimeoutException($"Vector index {indexName} did not become queryable within 60s.");
    }
}
