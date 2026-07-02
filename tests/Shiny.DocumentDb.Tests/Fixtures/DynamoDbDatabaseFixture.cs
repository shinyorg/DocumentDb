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

    public IDocumentStore CreateStoreWithFilter<T>(string tableName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.AddQueryFilter(filter);
        return new DynamoDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithNamedFilter<T>(string tableName, string filterName, Expression<Func<T, bool>> filter) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.AddQueryFilter(filterName, filter);
        return new DynamoDbDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = this.BaseOptions(tableName);
        opts.MapVersionProperty(versionProperty);
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
