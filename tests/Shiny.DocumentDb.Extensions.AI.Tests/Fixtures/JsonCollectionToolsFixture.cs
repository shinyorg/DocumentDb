using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;

/// <summary>
/// A store with the AI tools built over a <b>schema-free</b> collection — no CLR type anywhere, which is
/// the whole point of the lane under test.
/// </summary>
public class JsonCollectionToolsFixture : IDisposable
{
    public IDocumentStore Store { get; }
    public DocumentStoreAITools AITools { get; }

    public JsonCollectionToolsFixture(
        DocumentAICapabilities capabilities = DocumentAICapabilities.All,
        Action<IDocumentAICollectionBuilder>? configure = null)
    {
        configure ??= b => b
            .Description("Customer orders")
            .Field("id", DocumentFieldType.String)
            .Field("status", DocumentFieldType.String, "open, shipped or cancelled")
            .Field("total", DocumentFieldType.Number)
            .Field("quantity", DocumentFieldType.Int)
            .Field("tenantId", DocumentFieldType.String)
            .Field("customer.name", DocumentFieldType.String);

        var services = new ServiceCollection();
        services.AddDocumentStore(opts =>
        {
            opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
        });
        services.AddDocumentStoreAITools(builder => builder.AddCollection(
            "orders",
            capabilities: capabilities,
            configure: configure));

        var sp = services.BuildServiceProvider();
        this.Store = sp.GetRequiredService<IDocumentStore>();
        this.AITools = sp.GetRequiredService<DocumentStoreAITools>();
    }

    public IJsonDocumentCollection Collection => this.Store.Collection("orders");

    public AIFunction GetTool(string name) =>
        (AIFunction)this.AITools.Tools.First(t => t is AIFunction fn && fn.Name == name);

    public Task<object?> InvokeTool(string name, Dictionary<string, object?> args) =>
        this.GetTool(name).InvokeAsync(new AIFunctionArguments(args)).AsTask();

    public static JsonObject Order(string id, string status, double total, int quantity, string customer, string tenant = "acme") =>
        new()
        {
            ["id"] = id,
            ["status"] = status,
            ["total"] = total,
            ["quantity"] = quantity,
            ["tenantId"] = tenant,
            ["customer"] = new JsonObject { ["name"] = customer }
        };

    public Task Seed() => this.Collection.Insert(new JsonArray(
        Order("o1", "open", 100.5, 2, "Alice"),
        Order("o2", "shipped", 20, 1, "Bob"),
        Order("o3", "open", 9.25, 9, "Carol"),
        Order("o4", "cancelled", 300, 3, "Dave", tenant: "globex")));

    public static JsonDocument Parse(object? result) =>
        JsonDocument.Parse(JsonSerializer.Serialize(result));

    public void Dispose() => (this.Store as IDisposable)?.Dispose();
}
