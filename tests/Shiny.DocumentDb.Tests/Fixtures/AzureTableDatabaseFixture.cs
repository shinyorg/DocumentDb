using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Shiny.DocumentDb.AzureTable;
using Testcontainers.Azurite;
using Xunit;

namespace Shiny.DocumentDb.Tests.Fixtures;

public class AzureTableDatabaseFixture : IDocumentStoreFixture, IAsyncLifetime
{
    AzuriteContainer container = null!;
    string connectionString = null!;

    public IDocumentStore CreateStore(string tableName)
        => new AzureTableDocumentStore(new AzureTableDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            TableName = tableName
        });

    /// <summary>Builds a store with caller-supplied option tweaks (e.g. blob mappings).</summary>
    public AzureTableDocumentStore CreateConfiguredStore(string tableName, Action<AzureTableDocumentStoreOptions> configure)
    {
        var opts = new AzureTableDocumentStoreOptions { ConnectionString = this.connectionString, TableName = tableName };
        configure(opts);
        return new AzureTableDocumentStore(opts);
    }

    /// <summary>Builds a store with caller-supplied provider-agnostic options — the hook the
    /// cross-provider conformance suites configure themselves through.</summary>
    public IDocumentStore CreateStore(string tableName, Action<IDocumentStoreOptions> configure)
    {
        var opts = new AzureTableDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            TableName = tableName
        };
        configure(opts);
        return new AzureTableDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithVersion<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string tableName, Expression<Func<T, int>> versionProperty) where T : class
    {
        var opts = new AzureTableDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            TableName = tableName
        };
        opts.MapVersionProperty(versionProperty);
        return new AzureTableDocumentStore(opts);
    }

    public IDocumentStore CreateStoreWithIndexed<T>(string tableName, Expression<Func<T, object>> indexedProperty) where T : class
    {
        var opts = new AzureTableDocumentStoreOptions
        {
            ConnectionString = this.connectionString,
            TableName = tableName
        };
        opts.MapIndexedProperty(indexedProperty);
        return new AzureTableDocumentStore(opts);
    }

    public async ValueTask InitializeAsync()
    {
        this.container = new AzuriteBuilder()
            .WithImage("mcr.microsoft.com/azure-storage/azurite:latest")
            .Build();
        await this.container.StartAsync();
        this.connectionString = this.container.GetConnectionString();
    }

    public async ValueTask DisposeAsync() => await this.container.DisposeAsync();
}
