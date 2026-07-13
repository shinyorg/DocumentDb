using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shiny.DocumentDb.Aspire.Client;
using Shiny.DocumentDb.Aspire.Tests.Fixtures;

namespace Shiny.DocumentDb.Aspire.Tests;

// Mirrors ClientTests, but exercises AddDocumentContextProvider composing with a real source-generated
// DocumentContext (OrdersContext / InvoicesContext) rather than the keyed-store AddDocumentStore path.
public class ClientContextTests
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
    public async Task Context_ResolvesAndRoundTrips()
    {
        var builder = CreateBuilder(SqliteConfig());
        builder.Services.AddOrdersContext(builder.AddDocumentContextProvider("orders"));

        using var sp = builder.Services.BuildServiceProvider();
        using var scope = sp.CreateScope();
        var ctx = scope.ServiceProvider.GetRequiredService<OrdersContext>();

        await ctx.OrderRecords.Insert(new OrderRecord { Id = "o1", Customer = "Acme" });
        var fetched = await ctx.OrderRecords.Get("o1");

        Assert.NotNull(fetched);
        Assert.Equal("Acme", fetched!.Customer);
    }

    [Fact]
    public void DiscriminatorSelectsCorrectProvider()
    {
        var cfg = new Dictionary<string, string?>
        {
            ["ConnectionStrings:orders"] = "Data Source=:memory:",
            ["Shiny:DocumentDb:orders:Provider"] = "Sqlite"
        };
        var builder = CreateBuilder(cfg);

        IDatabaseProvider? captured = null;
        builder.Services.AddOrdersContext(
            builder.AddDocumentContextProvider("orders", configureOptions: o => captured = o.DatabaseProvider));

        using var sp = builder.Services.BuildServiceProvider();
        _ = sp.GetRequiredKeyedService<IDocumentStore>(typeof(OrdersContext));

        Assert.IsType<Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider>(captured);
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
        builder.Services.AddOrdersContext(builder.AddDocumentContextProvider(
            "orders",
            configureSettings: s => s.Provider = DocumentProviderKind.Sqlite, // override
            configureOptions: o => captured = o.DatabaseProvider));

        using var sp = builder.Services.BuildServiceProvider();
        _ = sp.GetRequiredKeyedService<IDocumentStore>(typeof(OrdersContext));

        Assert.IsType<Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider>(captured);
    }

    [Fact]
    public async Task SettingsConnectionStringOverride_Wins()
    {
        var cfg = new Dictionary<string, string?>
        {
            ["Shiny:DocumentDb:orders:Provider"] = "Sqlite"
            // no ConnectionStrings:orders — must come from settings
        };
        var builder = CreateBuilder(cfg);
        builder.Services.AddOrdersContext(builder.AddDocumentContextProvider(
            "orders",
            configureSettings: s => s.ConnectionString = "Data Source=:memory:"));

        using var sp = builder.Services.BuildServiceProvider();
        using var scope = sp.CreateScope();
        var ctx = scope.ServiceProvider.GetRequiredService<OrdersContext>();

        Assert.Equal(0, await ctx.OrderRecords.Count());
    }

    [Fact]
    public void HealthCheck_RegisteredByDefault()
    {
        var builder = CreateBuilder(SqliteConfig());
        builder.Services.AddOrdersContext(builder.AddDocumentContextProvider("orders"));

        using var sp = builder.Services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;
        Assert.Contains(options.Registrations, r => r.Name == "DocumentDb.orders");
    }

    [Fact]
    public void HealthCheck_NotRegisteredWhenDisabled()
    {
        var builder = CreateBuilder(SqliteConfig());
        builder.Services.AddOrdersContext(builder.AddDocumentContextProvider(
            "orders",
            configureSettings: s => s.DisableHealthChecks = true));

        using var sp = builder.Services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;
        Assert.DoesNotContain(options.Registrations, r => r.Name == "DocumentDb.orders");
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

        Assert.Throws<InvalidOperationException>(() => builder.AddDocumentContextProvider("orders"));
    }

    [Fact]
    public void MissingConnectionString_Throws()
    {
        var cfg = new Dictionary<string, string?>
        {
            ["Shiny:DocumentDb:orders:Provider"] = "Sqlite"
        };
        var builder = CreateBuilder(cfg);

        Assert.Throws<InvalidOperationException>(() => builder.AddDocumentContextProvider("orders"));
    }

    [Fact]
    public async Task FactoryVariant_ResolvesFactory()
    {
        var builder = CreateBuilder(SqliteConfig());
        builder.Services.AddOrdersContextFactory(builder.AddDocumentContextProvider("orders"));

        using var sp = builder.Services.BuildServiceProvider();
        var factory = sp.GetRequiredService<IDocumentContextFactory<OrdersContext>>();

        using var ctx = factory.Create();
        await ctx.OrderRecords.Insert(new OrderRecord { Id = "o1", Customer = "Factory" });
        var fetched = await ctx.OrderRecords.Get("o1");

        Assert.NotNull(fetched);
        Assert.Equal("Factory", fetched!.Customer);
    }

    [Fact]
    public async Task MultiContext_CoexistWithoutShadowing()
    {
        var cfg = new Dictionary<string, string?>
        {
            ["ConnectionStrings:orders"] = "Data Source=:memory:",
            ["Shiny:DocumentDb:orders:Provider"] = "Sqlite",
            ["ConnectionStrings:invoices"] = "Data Source=:memory:",
            ["Shiny:DocumentDb:invoices:Provider"] = "Sqlite"
        };
        var builder = CreateBuilder(cfg);
        builder.Services.AddOrdersContext(builder.AddDocumentContextProvider("orders"));
        builder.Services.AddInvoicesContext(builder.AddDocumentContextProvider("invoices"));

        using var sp = builder.Services.BuildServiceProvider();
        using var scope = sp.CreateScope();
        var orders = scope.ServiceProvider.GetRequiredService<OrdersContext>();
        var invoices = scope.ServiceProvider.GetRequiredService<InvoicesContext>();

        await orders.OrderRecords.Insert(new OrderRecord { Id = "o1", Customer = "Acme" });
        await invoices.InvoiceRecords.Insert(new InvoiceRecord { Id = "i1", Total = 42m });

        Assert.NotNull(await orders.OrderRecords.Get("o1"));
        Assert.NotNull(await invoices.InvoiceRecords.Get("i1"));
        // Each context has its own keyed store — neither shadows the other.
        Assert.Equal(1, await orders.OrderRecords.Count());
        Assert.Equal(1, await invoices.InvoiceRecords.Count());
    }
}
