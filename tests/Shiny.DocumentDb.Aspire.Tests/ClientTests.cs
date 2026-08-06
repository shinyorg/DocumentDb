using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shiny.DocumentDb.Aspire.Client;
using Shiny.DocumentDb.PostgreSql;

namespace Shiny.DocumentDb.Aspire.Tests;

public class ClientTests
{
    static HostApplicationBuilder CreateBuilder(IDictionary<string, string?> config)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(config);
        return builder;
    }

    static IDictionary<string, string?> SqliteConfig(string name = "orders") => new Dictionary<string, string?>
    {
        [$"ConnectionStrings:{name}"] = "Data Source=:memory:",
        [$"Shiny:DocumentDb:{name}:Provider"] = "Sqlite"
    };

    [Fact]
    public void Sqlite_RegistersWorkingKeyedStore()
    {
        var builder = CreateBuilder(SqliteConfig());
        builder.AddDocumentStore("orders");

        using var sp = builder.Services.BuildServiceProvider();
        Assert.NotNull(sp.GetKeyedService<IDocumentStore>("orders"));
    }

    [Fact]
    public async Task Sqlite_StoreIsUsable()
    {
        var builder = CreateBuilder(SqliteConfig());
        builder.AddDocumentStore("orders");

        using var sp = builder.Services.BuildServiceProvider();
        var store = sp.GetRequiredKeyedService<IDocumentStore>("orders");

        Assert.Equal(0, await store.Count<Doc>());
    }

    [Theory]
    [InlineData("Postgres", "Host=localhost;Database=test;Username=u;Password=p", typeof(Shiny.DocumentDb.PostgreSql.PostgreSqlDatabaseProvider))]
    [InlineData("CockroachDb", "Host=localhost;Port=26257;Database=test;Username=root;", typeof(Shiny.DocumentDb.CockroachDb.CockroachDbDatabaseProvider))]
    [InlineData("SqlServer", "Server=localhost;Database=test;", typeof(Shiny.DocumentDb.SqlServer.SqlServerDatabaseProvider))]
    [InlineData("MySql", "Server=localhost;Database=test;Uid=u;Pwd=p;", typeof(Shiny.DocumentDb.MySql.MySqlDatabaseProvider))]
    [InlineData("MariaDb", "Server=localhost;Database=test;Uid=u;Pwd=p;", typeof(Shiny.DocumentDb.MariaDb.MariaDbDatabaseProvider))]
    [InlineData("Sqlite", "Data Source=:memory:", typeof(Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider))]
    public void DiscriminatorSelectsCorrectProvider(string discriminator, string connectionString, Type expectedProviderType)
    {
        var cfg = new Dictionary<string, string?>
        {
            ["ConnectionStrings:orders"] = connectionString,
            ["Shiny:DocumentDb:orders:Provider"] = discriminator
        };
        var builder = CreateBuilder(cfg);

        IDatabaseProvider? captured = null;
        builder.AddDocumentStore("orders", configureOptions: o => captured = o.DatabaseProvider);

        using var sp = builder.Services.BuildServiceProvider();
        _ = sp.GetRequiredKeyedService<IDocumentStore>("orders");

        Assert.NotNull(captured);
        Assert.IsType(expectedProviderType, captured);
    }

    [Fact]
    public void SettingsProviderOverride_Wins()
    {
        var cfg = new Dictionary<string, string?>
        {
            ["ConnectionStrings:orders"] = "Data Source=:memory:",
            ["Shiny:DocumentDb:orders:Provider"] = "Postgres" // host says postgres
        };
        var builder = CreateBuilder(cfg);

        IDatabaseProvider? captured = null;
        builder.AddDocumentStore(
            "orders",
            configureSettings: s => s.Provider = DocumentProviderKind.Sqlite, // override
            configureOptions: o => captured = o.DatabaseProvider);

        using var sp = builder.Services.BuildServiceProvider();
        _ = sp.GetRequiredKeyedService<IDocumentStore>("orders");

        Assert.IsType<Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider>(captured);
    }

    [Fact]
    public void SettingsConnectionStringOverride_Wins()
    {
        var cfg = new Dictionary<string, string?>
        {
            ["Shiny:DocumentDb:orders:Provider"] = "Sqlite"
            // no ConnectionStrings:orders — must come from settings
        };
        var builder = CreateBuilder(cfg);
        builder.AddDocumentStore("orders", configureSettings: s => s.ConnectionString = "Data Source=:memory:");

        using var sp = builder.Services.BuildServiceProvider();
        Assert.NotNull(sp.GetKeyedService<IDocumentStore>("orders"));
    }

    [Fact]
    public void HealthCheck_RegisteredByDefault()
    {
        var builder = CreateBuilder(SqliteConfig());
        builder.AddDocumentStore("orders");

        using var sp = builder.Services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;
        Assert.Contains(options.Registrations, r => r.Name == "DocumentDb.orders");
    }

    [Fact]
    public void HealthCheck_NotRegisteredWhenDisabled()
    {
        var builder = CreateBuilder(SqliteConfig());
        builder.AddDocumentStore("orders", configureSettings: s => s.DisableHealthChecks = true);

        using var sp = builder.Services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;
        Assert.DoesNotContain(options.Registrations, r => r.Name == "DocumentDb.orders");
    }

    [Fact]
    public async Task HealthCheck_ReturnsHealthyForSqlite()
    {
        var builder = CreateBuilder(SqliteConfig());
        builder.AddDocumentStore("orders");

        using var sp = builder.Services.BuildServiceProvider();
        var svc = sp.GetRequiredService<HealthCheckService>();
        var report = await svc.CheckHealthAsync(r => r.Name == "DocumentDb.orders");

        Assert.Equal(HealthStatus.Healthy, report.Status);
    }

    [Fact]
    public void MissingDiscriminator_Throws()
    {
        var cfg = new Dictionary<string, string?>
        {
            ["ConnectionStrings:orders"] = "Data Source=:memory:"
            // no provider discriminator anywhere
        };
        var builder = CreateBuilder(cfg);

        Assert.Throws<InvalidOperationException>(() => { builder.AddDocumentStore("orders"); });
    }

    [Fact]
    public void MissingConnectionString_Throws()
    {
        var cfg = new Dictionary<string, string?>
        {
            ["Shiny:DocumentDb:orders:Provider"] = "Sqlite"
        };
        var builder = CreateBuilder(cfg);

        Assert.Throws<InvalidOperationException>(() => { builder.AddDocumentStore("orders"); });
    }

    [Fact]
    public void ConfigureServiceOptions_ReceivesServiceProvider()
    {
        var builder = CreateBuilder(SqliteConfig());
        builder.Services.AddSingleton(new Marker());

        Marker? captured = null;
        builder.AddDocumentStore(
            "orders",
            configureServiceOptions: (sp, _) => captured = sp.GetRequiredService<Marker>());

        using var provider = builder.Services.BuildServiceProvider();
        _ = provider.GetRequiredKeyedService<IDocumentStore>("orders");

        Assert.NotNull(captured);
    }

    [Fact]
    public async Task MultiTenant_IsolatesDocumentsByTenant()
    {
        var builder = CreateBuilder(SqliteConfig());
        var resolver = new MutableTenantResolver { Tenant = "A" };
        builder.Services.AddSingleton<ITenantResolver>(resolver);
        builder.AddDocumentStore("orders", configureSettings: s => s.MultiTenant = true);

        using var provider = builder.Services.BuildServiceProvider();
        var store = provider.GetRequiredKeyedService<IDocumentStore>("orders");

        await store.Upsert(new Doc { Id = "1" });

        resolver.Tenant = "B";
        Assert.Equal(0, await store.Count<Doc>());   // a different tenant sees nothing

        resolver.Tenant = "A";
        Assert.Equal(1, await store.Count<Doc>());   // the owning tenant sees its document
    }

    static IDictionary<string, string?> PostgresConfig(string name = "geo") => new Dictionary<string, string?>
    {
        [$"ConnectionStrings:{name}"] = "Host=localhost;Database=geo;Username=postgres;Password=x",
        [$"Shiny:DocumentDb:{name}:Provider"] = "Postgres"
    };

    [Fact]
    public void PortableSpatial_ReachesTheProvider()
    {
        IDatabaseProvider? captured = null;
        var builder = CreateBuilder(PostgresConfig());
        builder.AddDocumentStore(
            "geo",
            configureSettings: s => s.PortableSpatial = true,
            configureOptions: o => captured = o.DatabaseProvider
        );

        using var provider = builder.Services.BuildServiceProvider();
        _ = provider.GetRequiredKeyedService<IDocumentStore>("geo");

        var pg = Assert.IsType<PostgreSqlDatabaseProvider>(captured);
        Assert.True(pg.PortableSpatial);
    }

    [Fact]
    public void PortableSpatial_DefaultsToNativeTier()
    {
        IDatabaseProvider? captured = null;
        var builder = CreateBuilder(PostgresConfig());
        builder.AddDocumentStore("geo", configureOptions: o => captured = o.DatabaseProvider);

        using var provider = builder.Services.BuildServiceProvider();
        _ = provider.GetRequiredKeyedService<IDocumentStore>("geo");

        var pg = Assert.IsType<PostgreSqlDatabaseProvider>(captured);
        Assert.False(pg.PortableSpatial);
    }

    class Doc
    {
        public string Id { get; set; } = "";
    }

    class Marker;

    class MutableTenantResolver : ITenantResolver
    {
        public string Tenant { get; set; } = "";
        public string GetCurrentTenant() => this.Tenant;
    }
}
