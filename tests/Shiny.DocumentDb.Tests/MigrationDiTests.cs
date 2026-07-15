using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class MigrationDiTests
{
    static async Task RunHostedStartup(IServiceProvider sp)
    {
        foreach (var hosted in sp.GetServices<IHostedService>())
            await hosted.StartAsync(CancellationToken.None);
    }

    [Fact]
    public async Task RunsMigrationAtStartupAndTracksVersion()
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        services.AddDocumentMigration(20250101000000, "seed-user", async (store, ct) =>
            await store.Upsert(new User { Id = "u1", Name = "Alice", Age = 25 }, cancellationToken: ct));

        await using var sp = services.BuildServiceProvider();
        await RunHostedStartup(sp);

        var store = sp.GetRequiredService<IDocumentStore>();
        Assert.Equal(1, await store.Count<User>());

        var migrator = sp.GetRequiredService<IDocumentMigrator>();
        Assert.Equal(20250101000000, await migrator.GetCurrentVersionAsync());
    }

    [Fact]
    public async Task MigrationsRunBeforeSeeders()
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });

        var order = new List<string>();
        // Register seeder first to prove ordering is independent of registration order.
        services.AddDocumentSeeder("seeder", version: 1, (store, ct) =>
        {
            order.Add("seeder");
            return Task.CompletedTask;
        });
        services.AddDocumentMigration(20250101000000, "migration", (store, ct) =>
        {
            order.Add("migration");
            return Task.CompletedTask;
        });

        await using var sp = services.BuildServiceProvider();
        await RunHostedStartup(sp);

        Assert.Equal(["migration", "seeder"], order);
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
        services.AddDocumentMigration(20250101000000, "once", (store, ct) => { runs++; return Task.CompletedTask; });

        await using var sp = services.BuildServiceProvider();
        await RunHostedStartup(sp);
        await RunHostedStartup(sp);

        Assert.Equal(1, runs);
    }

    [Fact]
    public async Task MigratesNamedStoreOnly()
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
        services.AddDocumentMigration(20250101000000, "seed-user", async (store, ct) =>
            await store.Upsert(new User { Id = "u1", Name = "Alice", Age = 25 }, cancellationToken: ct),
            storeName: "reporting");

        await using var sp = services.BuildServiceProvider();
        await RunHostedStartup(sp);

        Assert.Equal(1, await sp.GetRequiredKeyedService<IDocumentStore>("reporting").Count<User>());
        Assert.Equal(0, await sp.GetRequiredService<IDocumentStore>().Count<User>());

        var migrator = sp.GetRequiredKeyedService<IDocumentMigrator>("reporting");
        Assert.Equal(20250101000000, await migrator.GetCurrentVersionAsync());
    }

    [Fact]
    public async Task RunsWithExplicitJsonTypeInfo_AotPath()
    {
        using var store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        });

        var migration = new DelegateDocumentMigration(20250101000000, "aot", (s, ct) =>
            s.Upsert(new User { Id = "u1", Name = "Alice" }, TestJsonContext.Default.User, ct));

        var applied = await DocumentMigrationRunner.RunAsync(
            store, [migration], TestJsonContext.Default.DocumentMigrationRecord);

        Assert.Equal(["aot"], applied);
        Assert.Equal(20250101000000, await DocumentMigrationRunner.GetCurrentVersionAsync(
            store, TestJsonContext.Default.DocumentMigrationRecord));
    }
}
