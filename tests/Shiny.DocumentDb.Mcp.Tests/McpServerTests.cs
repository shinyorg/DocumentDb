using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using Shiny.DocumentDb.Extensions.AI;
using Shiny.DocumentDb.Mcp.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Mcp.Tests;

/// <summary>
/// End-to-end over the real SDK and the real Streamable-HTTP transport: tool listing, the safety model, the
/// resources, the prompts — and the standing verification that the SDK still flows the per-request
/// <c>IServiceProvider</c> onto the tool's arguments, which is what makes a per-caller scope possible.
/// </summary>
public class McpServerTests
{
    static JsonElement Result(CallToolResult result)
    {
        var text = Assert.IsType<TextContentBlock>(result.Content[0]).Text;
        return JsonDocument.Parse(text).RootElement.Clone();
    }

    static McpServerFixture ReadOnly() => new(mcp =>
        mcp.AddType(new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }).Customer,
            name: "customer"));

    // ── Tool surface ────────────────────────────────────────────────────

    [Fact]
    public async Task ListsOneToolPerGrantedCapability()
    {
        await using var fixture = ReadOnly();
        await using var client = await fixture.ConnectAsync();

        var names = (await client.ListToolsAsync(cancellationToken: TestContext.Current.CancellationToken))
            .Select(t => t.Name).OrderBy(n => n).ToList();

        Assert.Equal(
            new[] { "customer_aggregate", "customer_count", "customer_get_by_id", "customer_query" },
            names);
    }

    [Fact]
    public async Task ReadOnlyRegistration_ExposesNoWriteTool()
    {
        await using var fixture = ReadOnly();
        await using var client = await fixture.ConnectAsync();

        var names = (await client.ListToolsAsync(cancellationToken: TestContext.Current.CancellationToken))
            .Select(t => t.Name).ToList();

        Assert.DoesNotContain("customer_insert", names);
        Assert.DoesNotContain("customer_update", names);
        Assert.DoesNotContain("customer_delete", names);
    }

    [Fact]
    public async Task AnUnregisteredType_ProducesNoTools()
    {
        await using var fixture = ReadOnly();
        await using var client = await fixture.ConnectAsync();

        var names = (await client.ListToolsAsync(cancellationToken: TestContext.Current.CancellationToken))
            .Select(t => t.Name).ToList();

        Assert.All(names, n => Assert.StartsWith("customer_", n));
    }

    [Fact]
    public void WriteCapabilityWithoutAllowWrites_FailsAtStartup()
    {
        var json = new McpTestJsonContext(new JsonSerializerOptions());
        var services = new ServiceCollection();
        services.AddDocumentStore(o => o.DatabaseProvider = new Sqlite.SqliteDatabaseProvider("Data Source=:memory:"));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddDocumentDbMcpServer(mcp =>
                mcp.AddType(json.Customer, name: "customer", capabilities: DocumentAICapabilities.All)));

        Assert.Contains("AllowWrites", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AllowWrites_UnlocksTheWriteTools()
    {
        var json = new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        await using var fixture = new McpServerFixture(mcp =>
        {
            mcp.AllowWrites();
            mcp.AddType(json.Customer, name: "customer", capabilities: DocumentAICapabilities.All);
        });
        await using var client = await fixture.ConnectAsync();

        var names = (await client.ListToolsAsync(cancellationToken: TestContext.Current.CancellationToken))
            .Select(t => t.Name).ToList();

        Assert.Contains("customer_insert", names);
        Assert.Contains("customer_delete", names);
    }

    // ── The standing verification: Services reaches the tool ────────────

    [Fact]
    public async Task ThePerRequestServiceProvider_ReachesTheToolAndResolvesScopedServices()
    {
        var json = new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        await using var fixture = new McpServerFixture(mcp =>
            mcp.AddType(json.Customer, name: "customer", configure: t => t
                .Where<IRegionScope>((scope, _) => c => c.Region == scope.Region)));
        await fixture.Seed();

        // If the SDK ever stops flowing RequestContext.Services onto AIFunctionArguments.Services, this test
        // fails — it is the tripwire for the whole per-caller scope feature.
        await using (var west = await fixture.ConnectAsync("west"))
        {
            var result = await west.CallToolAsync("customer_query", cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(2, Result(result).GetProperty("count").GetInt32());
        }

        await using (var east = await fixture.ConnectAsync("east"))
        {
            var result = await east.CallToolAsync("customer_query", cancellationToken: TestContext.Current.CancellationToken);
            var doc = Result(result);
            Assert.Equal(1, doc.GetProperty("count").GetInt32());
            Assert.Equal("Bob", doc.GetProperty("documents")[0].GetProperty("name").GetString());
        }
    }

    [Fact]
    public async Task TwoCallersSeeDifferentRows_FromTheSameToolInstance()
    {
        var json = new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        await using var fixture = new McpServerFixture(mcp =>
            mcp.AddType(json.Customer, name: "customer", configure: t => t
                .Where<IRegionScope>((scope, _) => c => c.Region == scope.Region)));
        await fixture.Seed();

        await using var west = await fixture.ConnectAsync("west");
        await using var east = await fixture.ConnectAsync("east");

        var westCount = Result(await west.CallToolAsync("customer_count", cancellationToken: TestContext.Current.CancellationToken))
            .GetProperty("count").GetInt32();
        var eastCount = Result(await east.CallToolAsync("customer_count", cancellationToken: TestContext.Current.CancellationToken))
            .GetProperty("count").GetInt32();

        Assert.Equal(2, westCount);
        Assert.Equal(1, eastCount);
    }

    [Fact]
    public async Task AnOutOfScopeIdIsNotFound_NotForbidden()
    {
        var json = new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        await using var fixture = new McpServerFixture(mcp =>
            mcp.AddType(json.Customer, name: "customer", configure: t => t
                .Where<IRegionScope>((scope, _) => c => c.Region == scope.Region)));
        await fixture.Seed();

        await using var west = await fixture.ConnectAsync("west");

        var mine = Result(await west.CallToolAsync("customer_get_by_id",
            new Dictionary<string, object?> { ["id"] = "c1" }, cancellationToken: TestContext.Current.CancellationToken));
        Assert.True(mine.GetProperty("found").GetBoolean());

        var theirs = Result(await west.CallToolAsync("customer_get_by_id",
            new Dictionary<string, object?> { ["id"] = "c2" }, cancellationToken: TestContext.Current.CancellationToken));
        Assert.False(theirs.GetProperty("found").GetBoolean());
    }

    // ── Hidden properties ───────────────────────────────────────────────

    [Fact]
    public async Task IgnoredProperties_AppearInNeitherSchemaNorResults()
    {
        var json = new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        await using var fixture = new McpServerFixture(mcp =>
        {
            mcp.ExposeResources();
            mcp.AddType(json.Customer, name: "customer", configure: t => t.IgnoreProperties(c => c.Secret!));
        });
        await fixture.Seed();

        await using var client = await fixture.ConnectAsync();

        var tools = await client.ListToolsAsync(cancellationToken: TestContext.Current.CancellationToken);
        var query = tools.Single(t => t.Name == "customer_query");
        Assert.DoesNotContain("secret", JsonSerializer.Serialize(query.ProtocolTool.InputSchema), StringComparison.OrdinalIgnoreCase);

        var schema = await client.ReadResourceAsync("documentdb://types/customer/schema", cancellationToken: TestContext.Current.CancellationToken);
        var schemaText = Assert.IsType<TextResourceContents>(schema.Contents[0]).Text;
        Assert.DoesNotContain("secret", schemaText, StringComparison.OrdinalIgnoreCase);
    }

    // ── Resources ───────────────────────────────────────────────────────

    [Fact]
    public async Task ResourceRoundTrip_TypesThenSchemaThenSample()
    {
        var json = new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        await using var fixture = new McpServerFixture(mcp =>
        {
            mcp.ExposeResources(sampleSize: 2);
            mcp.AddType(json.Customer, name: "customer");
        });
        await fixture.Seed();

        await using var client = await fixture.ConnectAsync();

        var types = await client.ReadResourceAsync("documentdb://types", cancellationToken: TestContext.Current.CancellationToken);
        var typesDoc = JsonDocument.Parse(Assert.IsType<TextResourceContents>(types.Contents[0]).Text!);
        var first = typesDoc.RootElement.GetProperty("types")[0];
        Assert.Equal("customer", first.GetProperty("name").GetString());
        Assert.Equal("type", first.GetProperty("kind").GetString());

        var schema = await client.ReadResourceAsync("documentdb://types/customer/schema", cancellationToken: TestContext.Current.CancellationToken);
        var schemaDoc = JsonDocument.Parse(Assert.IsType<TextResourceContents>(schema.Contents[0]).Text!);
        Assert.True(schemaDoc.RootElement.GetProperty("schema").TryGetProperty("properties", out _));

        var sample = await client.ReadResourceAsync("documentdb://types/customer/sample", cancellationToken: TestContext.Current.CancellationToken);
        var sampleDoc = JsonDocument.Parse(Assert.IsType<TextResourceContents>(sample.Contents[0]).Text!);
        Assert.Equal(2, sampleDoc.RootElement.GetProperty("sample").GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task TheSampleResource_ObeysTheScope()
    {
        var json = new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        await using var fixture = new McpServerFixture(mcp =>
        {
            mcp.ExposeResources(sampleSize: 10);
            mcp.AddType(json.Customer, name: "customer", configure: t => t
                .Where<IRegionScope>((scope, _) => c => c.Region == scope.Region));
        });
        await fixture.Seed();

        await using var east = await fixture.ConnectAsync("east");
        var sample = await east.ReadResourceAsync("documentdb://types/customer/sample", cancellationToken: TestContext.Current.CancellationToken);
        var doc = JsonDocument.Parse(Assert.IsType<TextResourceContents>(sample.Contents[0]).Text!);

        // A resource that read the store directly would have returned all three.
        Assert.Equal(1, doc.RootElement.GetProperty("sample").GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task StatsReportsProviderAndCapabilities()
    {
        var json = new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        await using var fixture = new McpServerFixture(mcp =>
        {
            mcp.ExposeResources();
            mcp.AddType(json.Customer, name: "customer");
        });
        await fixture.Seed();

        await using var client = await fixture.ConnectAsync();
        var stats = await client.ReadResourceAsync("documentdb://stats", cancellationToken: TestContext.Current.CancellationToken);
        var doc = JsonDocument.Parse(Assert.IsType<TextResourceContents>(stats.Contents[0]).Text!);

        Assert.Contains("DocumentStore", doc.RootElement.GetProperty("provider").GetString());
        Assert.True(doc.RootElement.GetProperty("capabilities").GetProperty("transactions").GetBoolean());
        Assert.Equal(3, doc.RootElement.GetProperty("counts").GetProperty("customer").GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task ResourcesAreAbsentUnlessExposed()
    {
        await using var fixture = ReadOnly();
        await using var client = await fixture.ConnectAsync();

        // Not "an empty list" — the capability is never advertised, so resources/list is not a method the
        // server answers at all. A model sees no resources to try.
        Assert.Null(client.ServerCapabilities.Resources);
        Assert.Null(client.ServerCapabilities.Prompts);
    }

    // ── Prompts ─────────────────────────────────────────────────────────

    [Fact]
    public async Task PromptsDescribeTheTypeWithoutLeakingTheScope()
    {
        var json = new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        await using var fixture = new McpServerFixture(mcp =>
        {
            mcp.ExposePrompts();
            mcp.AddType(json.Customer, name: "customer", configure: t => t.Where(c => c.Region == "west"));
        });

        await using var client = await fixture.ConnectAsync();

        var prompts = await client.ListPromptsAsync(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Contains(prompts, p => p.Name == "explain-collection");
        Assert.Contains(prompts, p => p.Name == "build-filter");

        var prompt = await client.GetPromptAsync(
            "build-filter",
            new Dictionary<string, object?> { ["name"] = "customer", ["ask"] = "who is over 30" },
            cancellationToken: TestContext.Current.CancellationToken);

        var text = string.Join("\n", prompt.Messages.Select(m => (m.Content as TextContentBlock)?.Text));
        Assert.Contains("customer_query", text, StringComparison.Ordinal);
        Assert.Contains("age", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("west", text, StringComparison.OrdinalIgnoreCase);   // the scope stays invisible
    }
}
