using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Shiny.DocumentDb.Aspire.Hosting;

namespace Shiny.DocumentDb.Aspire.Tests;

public class HostingTests
{
    // Model-only assertions: DistributedApplication.CreateBuilder() yields a real Aspire app builder
    // without requiring an AppHost entry point or spinning containers.
    static IDistributedApplicationBuilder CreateBuilder()
        => DistributedApplication.CreateBuilder();

    [Fact]
    public void AsDocumentStore_DetectsPostgresKind()
    {
        var builder = CreateBuilder();
        var db = builder.AddPostgres("pg").AddDatabase("orders-db");

        var store = ((IResourceBuilder<IResourceWithConnectionString>)db).AsDocumentStore("orders");

        Assert.Equal(DocumentProviderKind.Postgres, store.Resource.Kind);
    }

    [Fact]
    public void AsDocumentStore_DetectsSqlServerKind()
    {
        var builder = CreateBuilder();
        var db = builder.AddSqlServer("sql").AddDatabase("orders-db");

        var store = ((IResourceBuilder<IResourceWithConnectionString>)db).AsDocumentStore("orders");

        Assert.Equal(DocumentProviderKind.SqlServer, store.Resource.Kind);
    }

    [Fact]
    public void AsDocumentStore_ExplicitKind_Wins()
    {
        var builder = CreateBuilder();
        var cs = builder.AddConnectionString("orders-cs");

        var store = cs.AsDocumentStore("orders", DocumentProviderKind.Sqlite);

        Assert.Equal(DocumentProviderKind.Sqlite, store.Resource.Kind);
    }

    [Fact]
    public async Task AddSqliteDocumentStore_ModelsConnectionStringWithoutContainer()
    {
        var builder = CreateBuilder();

        var store = builder.AddSqliteDocumentStore("orders", "/tmp/orders.db");

        Assert.Equal(DocumentProviderKind.Sqlite, store.Resource.Kind);
        var cs = await ((IResourceWithConnectionString)store.Resource).GetConnectionStringAsync(CancellationToken.None);
        Assert.Equal("Data Source=/tmp/orders.db", cs);
    }

    [Fact]
    public void AddPostgresDocumentStore_ProvisionsBackingResource()
    {
        var builder = CreateBuilder();

        var store = builder.AddPostgresDocumentStore("orders");

        Assert.Equal(DocumentProviderKind.Postgres, store.Resource.Kind);
        Assert.Contains(builder.Resources, r => r.Name == "orders" && r is DocumentStoreResource);
        Assert.Contains(builder.Resources, r => r.Name == "orders-server");
        Assert.Contains(builder.Resources, r => r.Name == "orders-db");
    }

    [Fact]
    public async Task WithReference_InjectsProviderDiscriminatorEnvVar()
    {
        var builder = CreateBuilder();
        var store = builder.AddSqliteDocumentStore("orders", "/tmp/orders.db");

        var consumer = builder
            .AddContainer("api", "nginx")
            .WithReference(store);

#pragma warning disable CS0618 // GetEnvironmentVariableValuesAsync is the simplest model-only resolution for tests
        var env = await ((IResourceWithEnvironment)consumer.Resource)
            .GetEnvironmentVariableValuesAsync(DistributedApplicationOperation.Publish);
#pragma warning restore CS0618

        Assert.True(env.ContainsKey("Shiny__DocumentDb__orders__Provider"));
        Assert.Equal("Sqlite", env["Shiny__DocumentDb__orders__Provider"]);
    }

    [Fact]
    public async Task WithReference_InjectsConnectionString()
    {
        var builder = CreateBuilder();
        var store = builder.AddSqliteDocumentStore("orders", "/tmp/orders.db");

        var consumer = builder
            .AddContainer("api", "nginx")
            .WithReference(store);

#pragma warning disable CS0618
        var env = await ((IResourceWithEnvironment)consumer.Resource)
            .GetEnvironmentVariableValuesAsync(DistributedApplicationOperation.Publish);
#pragma warning restore CS0618

        // In Publish mode the value is a manifest expression that references the backing resource's
        // connection string; the key being present proves WithReference wired the connection string.
        Assert.True(env.ContainsKey("ConnectionStrings__orders"));
        Assert.Contains("orders", env["ConnectionStrings__orders"]);
    }

    [Fact]
    public void WithSeeder_DoesNotThrowDuringConfiguration()
    {
        var builder = CreateBuilder();

        var store = builder
            .AddSqliteDocumentStore("orders", "/tmp/orders.db")
            .WithSeeder((_, _) => Task.CompletedTask);

        Assert.NotNull(store.Resource);
    }
}
