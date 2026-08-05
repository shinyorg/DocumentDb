using System.Text.Json;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using Shiny.DocumentDb.Mcp.Tests.Fixtures;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Mcp.Tests;

/// <summary>
/// The <c>shiny-documentdb-mcp</c> tool, driven the way a desktop client drives it: launch the process, speak
/// MCP over its stdin/stdout, list the tools it discovered from the data, and call one.
/// </summary>
/// <remarks>
/// The tool has no compiled document classes, so what it exposes comes from the stored <c>TypeName</c>
/// discriminators — that discovery is the thing worth testing end to end.
/// </remarks>
public class StdioToolTests : IDisposable
{
    readonly string databasePath = Path.Combine(Path.GetTempPath(), $"mcptool_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        if (File.Exists(this.databasePath))
            File.Delete(this.databasePath);
    }

    /// <summary>The tool's built entry assembly, or null when it has not been built (skip rather than fail).</summary>
    static string? ToolPath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "DocumentDb.slnx")))
            dir = dir.Parent;

        if (dir is null)
            return null;

        // .../bin/<Configuration>/<Tfm>/ — mirror the same two segments under the tool's own output.
        var tfm = Path.GetFileName(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar))!;
        var configuration = Path.GetFileName(
            Path.GetDirectoryName(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar)))!;

        var candidate = Path.Combine(dir.FullName, "src", "ShinyDocDbMcp", "bin", configuration, tfm, "ShinyDocDbMcp.dll");
        return File.Exists(candidate) ? candidate : null;
    }

    async Task SeedDatabase()
    {
        var json = new McpTestJsonContext(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.databasePath}"),
            JsonSerializerOptions = json.Options
        });

        await store.BatchInsert(new[]
        {
            new Customer { Id = "c1", Name = "Alice", Age = 30, Region = "west" },
            new Customer { Id = "c2", Name = "Bob", Age = 25, Region = "east" }
        }, json.Customer, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task InitializeListToolsAndCall_OverStdio()
    {
        var tool = ToolPath();
        Assert.SkipWhen(tool is null, "ShinyDocDbMcp has not been built in this configuration.");

        await this.SeedDatabase();

        var transport = new StdioClientTransport(new StdioClientTransportOptions
        {
            Name = "documentdb",
            Command = "dotnet",
            Arguments =
            [
                tool!,
                "--provider", "Sqlite",
                "--connection", $"Data Source={this.databasePath}"
            ]
        });

        await using var client = await McpClient.CreateAsync(transport, cancellationToken: TestContext.Current.CancellationToken);

        // Discovered from the data: the stored TypeName is "Customer", so the slug is "customer".
        var tools = await client.ListToolsAsync(cancellationToken: TestContext.Current.CancellationToken);
        var names = tools.Select(t => t.Name).ToList();
        Assert.Contains("customer_query", names);
        Assert.Contains("customer_count", names);
        Assert.DoesNotContain("customer_insert", names);   // read-only without --allow-writes

        var result = await client.CallToolAsync(
            "customer_count",
            cancellationToken: TestContext.Current.CancellationToken);

        var text = Assert.IsType<TextContentBlock>(result.Content[0]).Text!;
        Assert.Equal(2, JsonDocument.Parse(text).RootElement.GetProperty("count").GetInt32());
    }
}
