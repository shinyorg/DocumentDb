using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Shiny.DocumentDb.DynamoDb;
using Testcontainers.DynamoDb;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class DynamoDbDatabaseFixture : IDocumentStoreFixture, IAsyncLifetime
{
    DynamoDbContainer container = null!;
    string serviceUrl = null!;

    IAmazonDynamoDB CreateClient()
        => new AmazonDynamoDBClient(
            new BasicAWSCredentials("dummy", "dummy"),
            new AmazonDynamoDBConfig
            {
                ServiceURL = this.serviceUrl,
                AuthenticationRegion = "us-east-1"
            });

    DynamoDbDocumentStoreOptions BaseOptions(string tableName) => new()
    {
        Client = this.CreateClient(),
        TableName = tableName,
        AutoCreateTable = true
    };

    public IDocumentStore CreateStore(string tableName)
        => new DynamoDbDocumentStore(this.BaseOptions(tableName));

    /// <summary>Builds a store with caller-supplied option tweaks (e.g. blob mappings).</summary>
    public DynamoDbDocumentStore CreateConfiguredStore(string tableName, Action<DynamoDbDocumentStoreOptions> configure)
    {
        var opts = this.BaseOptions(tableName);
        configure(opts);
        return new DynamoDbDocumentStore(opts);
    }

    /// <summary>Builds a store with caller-supplied provider-agnostic options — the hook the
    /// cross-provider conformance suites configure themselves through.</summary>
    public IDocumentStore CreateStore(string tableName, Action<IDocumentStoreOptions> configure)
    {
        var opts = this.BaseOptions(tableName);
        configure(opts);
        return new DynamoDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.ConfigureDocument<T>(cfg => cfg.MapVersionProperty(versionProperty));
        return new DynamoDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithIndexed<T>(string tableName, Expression<Func<T, object>> indexedProperty) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.MapIndexedProperty(indexedProperty);
        return new DynamoDbDocumentStore(opts);
    }

    public async ValueTask InitializeAsync()
    {
        this.container = new DynamoDbBuilder()
            .WithImage("amazon/dynamodb-local:latest")
            .Build();
        await this.container.StartAsync();
        this.serviceUrl = this.container.GetConnectionString();
    }

    public async ValueTask DisposeAsync() => await this.container.DisposeAsync();
}
