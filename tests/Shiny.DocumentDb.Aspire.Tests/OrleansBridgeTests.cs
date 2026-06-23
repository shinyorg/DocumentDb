using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Shiny.DocumentDb.Aspire.Client;
using Shiny.DocumentDb.Orleans;

namespace Shiny.DocumentDb.Aspire.Tests;

public class OrleansBridgeTests
{
    static HostApplicationBuilder CreateBuilder()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:orleans"] = "Data Source=:memory:",
            ["Shiny:DocumentDb:orleans:Provider"] = "Sqlite"
        });
        builder.AddDocumentStore("orleans");   // Aspire client → keyed IDocumentStore "orleans"
        return builder;
    }

    [Fact]
    public void UseAspireDocumentDb_WiresReminderStoreFactoryToKeyedStore()
    {
        var builder = CreateBuilder();
        builder.UseOrleans(silo => silo
            .UseLocalhostClustering()
            .UseAspireDocumentDb("orleans", DocumentDbOrleansFeatures.Reminders | DocumentDbOrleansFeatures.GrainStorage));

        using var sp = builder.Services.BuildServiceProvider();

        var options = sp.GetRequiredService<IOptions<DocumentDbReminderOptions>>().Value;
        Assert.NotNull(options.StoreFactory);

        var fromFactory = options.StoreFactory!(sp);
        Assert.Same(sp.GetRequiredKeyedService<IDocumentStore>("orleans"), fromFactory);
    }

    [Fact]
    public void UseAspireDocumentDb_WiresGrainStorageStoreFactoryToKeyedStore()
    {
        var builder = CreateBuilder();
        builder.UseOrleans(silo => silo
            .UseLocalhostClustering()
            .UseAspireDocumentDb("orleans", DocumentDbOrleansFeatures.GrainStorage));

        using var sp = builder.Services.BuildServiceProvider();

        var options = sp.GetRequiredService<IOptionsMonitor<DocumentDbGrainStorageOptions>>().Get("Default");
        Assert.NotNull(options.StoreFactory);
        Assert.Same(sp.GetRequiredKeyedService<IDocumentStore>("orleans"), options.StoreFactory!(sp));
    }
}
