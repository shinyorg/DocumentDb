using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.CosmosDb;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class CosmosDbDatabaseFixture : IDocumentStoreFixture, IAsyncLifetime
{
    IContainer container = null!;
    string connectionString = null!;
    CosmosClient? sharedClient;

    const int CosmosPort = 8081;
    const string CosmosKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

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
