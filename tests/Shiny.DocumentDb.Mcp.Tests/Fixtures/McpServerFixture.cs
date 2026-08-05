using System.ComponentModel;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Client;
using Shiny.DocumentDb.Extensions.AI;
using Shiny.DocumentDb.Mcp;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Mcp.Tests.Fixtures;

[Description("Customer record")]
public class Customer
{
    public string Id { get; set; } = "";
    [Description("Full name")]
    public string Name { get; set; } = "";
    [Description("Age in years")]
    public int Age { get; set; }
    public string Region { get; set; } = "";
    public string? Secret { get; set; }
}

[JsonSerializable(typeof(Customer))]
public partial class McpTestJsonContext : JsonSerializerContext;

/// <summary>The per-request service a scoped filter resolves from — the multi-tenant story, in miniature.</summary>
public interface IRegionScope
{
    string Region { get; set; }
}

public sealed class RegionScope : IRegionScope
{
    public string Region { get; set; } = "";
}

/// <summary>
/// A real MCP server over the Streamable-HTTP transport, hosted in-process, with a real MCP client on the
/// other end. Nothing here fakes the SDK — the point of these tests is what the SDK actually does.
/// </summary>
public sealed class McpServerFixture : IAsyncDisposable
{
    readonly WebApplication app;

    public McpServerFixture(
        Action<IDocumentDbMcpBuilder> configure,
        Action<IServiceCollection>? configureServices = null)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();

        var json = new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        this.JsonContext = json;

        // Populated from the request itself rather than by middleware writing into a scoped object: the MCP
        // SDK may run a tool in a child of the ASP.NET request scope, and this is how a real app does it anyway.
        builder.Services.AddHttpContextAccessor();
        builder.Services.AddSingleton<RequestScopeRecorder>();
        builder.Services.AddScoped<IRegionScope>(sp =>
        {
            var instance = new RegionScope
            {
                Region = sp.GetRequiredService<IHttpContextAccessor>().HttpContext?.Request.Headers["X-Region"].ToString() ?? ""
            };
            sp.GetRequiredService<RequestScopeRecorder>().Record(instance);
            return instance;
        });
        configureServices?.Invoke(builder.Services);

        builder.Services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider($"Data Source=mcp_{Guid.NewGuid():N};Mode=Memory;Cache=Shared");
            o.JsonSerializerOptions = json.Options;
        });

        builder.Services
            .AddDocumentDbMcpServer(configure)
            .WithHttpTransport();

        this.app = builder.Build();
        this.app.MapDocumentDbMcp("/mcp");
        this.app.StartAsync().GetAwaiter().GetResult();

        this.Store = this.app.Services.GetRequiredService<IDocumentStore>();
    }

    public IDocumentStore Store { get; }
    public McpTestJsonContext JsonContext { get; }

    public Task<McpClient> ConnectAsync(string? region = null)
    {
        var http = this.app.GetTestClient();
        if (region != null)
            http.DefaultRequestHeaders.Add("X-Region", region);

        var transport = new HttpClientTransport(
            new HttpClientTransportOptions { Endpoint = new Uri("http://localhost/mcp") },
            http,
            NullLoggerFactory);

        return McpClient.CreateAsync(transport, cancellationToken: TestContext.Current.CancellationToken);
    }

    static readonly ILoggerFactory NullLoggerFactory = Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;

    public Task Seed() => this.Store.BatchInsert(new[]
    {
        new Customer { Id = "c1", Name = "Alice", Age = 30, Region = "west", Secret = "hunter2" },
        new Customer { Id = "c2", Name = "Bob", Age = 25, Region = "east", Secret = "hunter2" },
        new Customer { Id = "c3", Name = "Carol", Age = 35, Region = "west", Secret = "hunter2" }
    }, this.JsonContext.Customer);

    public async ValueTask DisposeAsync()
    {
        await this.app.StopAsync();
        await this.app.DisposeAsync();
    }
}

/// <summary>Records the scoped service instances a tool call actually resolved, to prove the scope is real.</summary>
public sealed class RequestScopeRecorder
{
    readonly List<object> instances = new();

    public IReadOnlyList<object> Instances
    {
        get { lock (this.instances) return this.instances.ToList(); }
    }

    public void Record(object instance)
    {
        lock (this.instances)
            this.instances.Add(instance);
    }
}
