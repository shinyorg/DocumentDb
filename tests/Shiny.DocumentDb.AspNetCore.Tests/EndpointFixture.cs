using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.AspNetCore.Tests;

public class Order
{
    public string Id { get; set; } = "";
    public string Status { get; set; } = "open";
    public string CustomerId { get; set; } = "";
    public decimal Total { get; set; }
    public string? Notes { get; set; }
    public int Version { get; set; }
}

[JsonSerializable(typeof(Order))]
public partial class TestJsonContext : JsonSerializerContext;

/// <summary>The per-request service a real app resolves its scope from.</summary>
public interface ICustomerContext
{
    string CustomerId { get; }
    Guid InstanceId { get; }
}

public sealed class CustomerContext(IHttpContextAccessor http) : ICustomerContext
{
    public string CustomerId => http.HttpContext?.Request.Headers["X-Customer"].ToString() ?? "";
    public Guid InstanceId { get; } = Guid.NewGuid();
}

/// <summary>An in-process app with one mapped document resource, over a shared-cache in-memory SQLite store.</summary>
public sealed class EndpointFixture : IAsyncDisposable
{
    readonly WebApplication app;

    public EndpointFixture(
        Action<DocumentEndpointOptions<Order>>? configure = null,
        Action<IEndpointRouteBuilder>? mapExtra = null,
        bool mapOrders = true,
        Action<DocumentStoreOptions>? configureStore = null)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();

        this.JsonContext = new TestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        builder.Services.AddHttpContextAccessor();
        builder.Services.AddScoped<ICustomerContext, CustomerContext>();
        builder.Services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider($"Data Source=rest_{Guid.NewGuid():N};Mode=Memory;Cache=Shared");
            o.JsonSerializerOptions = this.JsonContext.Options;
            o.ConfigureDocument<Order>(cfg => cfg.MapVersionProperty(x => x.Version));
            configureStore?.Invoke(o);
        });

        this.app = builder.Build();

        if (mapOrders)
        {
            this.app.MapDocuments<Order>("/orders", o =>
            {
                o.Operations = DocumentEndpoints.Read | DocumentEndpoints.Write | DocumentEndpoints.Delete | DocumentEndpoints.Count;
                o.TypeInfo = this.JsonContext.Order;
                configure?.Invoke(o);
            });
        }

        mapExtra?.Invoke(this.app);

        this.app.StartAsync().GetAwaiter().GetResult();
        this.Store = this.app.Services.GetRequiredService<IDocumentStore>();
    }

    public TestJsonContext JsonContext { get; }
    public IDocumentStore Store { get; }
    public IServiceProvider Services => this.app.Services;

    public HttpClient Client(string? customer = null)
    {
        var client = this.app.GetTestClient();
        if (customer != null)
            client.DefaultRequestHeaders.Add("X-Customer", customer);

        return client;
    }

    public Task Seed() => this.Store.BatchInsert(new[]
    {
        new Order { Id = "o1", Status = "open", CustomerId = "acme", Total = 100m, Notes = "first" },
        new Order { Id = "o2", Status = "closed", CustomerId = "acme", Total = 700m },
        new Order { Id = "o3", Status = "open", CustomerId = "globex", Total = 900m }
    }, this.JsonContext.Order);

    public async ValueTask DisposeAsync()
    {
        await this.app.StopAsync();
        await this.app.DisposeAsync();
    }
}
