using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class SeedingDiTests
{
    static async Task RunHostedSeeders(IServiceProvider sp)
    {
        foreach (var hosted in sp.GetServices<IHostedService>())
            await hosted.StartAsync(CancellationToken.None);
    }

    [Fact]
    public async Task SeedsDefaultStoreAtStartup()
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        services.AddDocumentSeeder("users", version: 1, async (store, ct) =>
            await store.Upsert(new User { Id = "u1", Name = "Alice", Age = 25 }, cancellationToken: ct));

        await using var sp = services.BuildServiceProvider();
        await RunHostedSeeders(sp);

        var store = sp.GetRequiredService<IDocumentStore>();
        Assert.Equal(1, await store.Count<User>());
    }

    [Fact]
    public async Task DoesNotReRunOnSecondStartup()
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });

        var runs = 0;
        services.AddDocumentSeeder("users", version: 1, async (store, ct) =>
        {
            runs++;
            await store.Upsert(new User { Id = "u1", Name = "Alice", Age = 25 }, cancellationToken: ct);
        });

        await using var sp = services.BuildServiceProvider();
        await RunHostedSeeders(sp);
        await RunHostedSeeders(sp);

        Assert.Equal(1, runs);
        Assert.Equal(1, await sp.GetRequiredService<IDocumentStore>().Count<User>());
    }

    [Fact]
    public async Task SeedsNamedStoreOnly()
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        services.AddDocumentStore("reporting", o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        services.AddDocumentSeeder("users", version: 1, async (store, ct) =>
            await store.Upsert(new User { Id = "u1", Name = "Alice", Age = 25 }, cancellationToken: ct),
            storeName: "reporting");

        await using var sp = services.BuildServiceProvider();
        await RunHostedSeeders(sp);

        var named = sp.GetRequiredKeyedService<IDocumentStore>("reporting");
        var defaultStore = sp.GetRequiredService<IDocumentStore>();

        Assert.Equal(1, await named.Count<User>());
        Assert.Equal(0, await defaultStore.Count<User>());
    }
}
