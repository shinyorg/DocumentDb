using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shiny;
using Shiny.Data.Sync;
using Xunit;

namespace Shiny.DocumentDb.AppDataSync.Tests;


/// <summary>
/// End-to-end wiring via the real <c>SyncDocumentStore</c> extension: registry population, forwarder
/// resolution as a DI interceptor, and the startup serializer-equivalence validator.
/// </summary>
public class SyncWiringTests
{
    static ServiceProvider BuildProvider(out FakeDataSyncManager manager)
    {
        var fake = new FakeDataSyncManager();
        manager = fake;

        var services = new ServiceCollection();
        services.AddLogging();
        // Stand in for AddDocumentStore's registrations (options + store) and AddDataSync's manager.
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = "documents"
        };
        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore>(sp => new DocumentStore(options, sp));
        services.AddSingleton<IDataSyncManager>(fake);

        services.SyncDocumentStore(sync => sync.Sync<TodoItem>());
        return services.BuildServiceProvider();
    }


    [Fact]
    public async Task Forwarder_Is_Wired_As_Di_Interceptor()
    {
        using var sp = BuildProvider(out var manager);
        var store = sp.GetRequiredService<IDocumentStore>();

        var id = Guid.NewGuid();
        await store.Insert(new TodoItem { Id = id, Title = "wired" });

        var op = Assert.Single(manager.Queued);
        Assert.Equal(SyncVerb.Create, op.Verb);
        Assert.Equal(id.ToString(), op.Identifier);
    }


    [Fact]
    public async Task Startup_Serializer_Validation_Passes_For_Matching_Contract()
    {
        // DocumentDb default = camelCase; Shiny.Json default = camelCase. The validator should not throw.
        using var sp = BuildProvider(out _);
        var validators = sp.GetServices<IHostedService>().ToList();
        Assert.NotEmpty(validators);

        foreach (var v in validators)
            await v.StartAsync(CancellationToken.None);
    }


    [Fact]
    public void Registry_Is_Registered_And_Populated()
    {
        using var sp = BuildProvider(out _);
        var registry = sp.GetRequiredService<SyncDocumentRegistry>();
        Assert.True(registry.IsSynced(typeof(TodoItem)));
    }
}
