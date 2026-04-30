using System.Text.Json;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Extensions.AI.Tests.Fixtures;

public class AIToolsFixture : IDisposable
{
    public IDocumentStore Store { get; }
    public DocumentStoreAITools AITools { get; }
    public TestJsonContext JsonContext { get; }

    public AIToolsFixture()
        : this(DocumentAICapabilities.All) { }

    public AIToolsFixture(
        DocumentAICapabilities customerCaps = DocumentAICapabilities.All,
        DocumentAICapabilities productCaps = DocumentAICapabilities.All,
        Action<IDocumentAITypeBuilder<Customer>>? configureCustomer = null,
        Action<IDocumentAITypeBuilder<Product>>? configureProduct = null)
    {
        JsonContext = new TestJsonContext(new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        var services = new ServiceCollection();
        services.AddDocumentStore(opts =>
        {
            opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            opts.JsonSerializerOptions = JsonContext.Options;
        });
        services.AddDocumentStoreAITools(builder =>
        {
            builder.AddType(
                JsonContext.Customer,
                capabilities: customerCaps,
                configure: configureCustomer
            );
            builder.AddType(
                JsonContext.Product,
                capabilities: productCaps,
                configure: configureProduct
            );
        });

        var sp = services.BuildServiceProvider();
        Store = sp.GetRequiredService<IDocumentStore>();
        AITools = sp.GetRequiredService<DocumentStoreAITools>();
    }

    public AIFunction GetTool(string name) =>
        (AIFunction)AITools.Tools.First(t => t is AIFunction fn && fn.Name == name);

    public async Task<object?> InvokeTool(string name, Dictionary<string, object?> args)
    {
        var tool = GetTool(name);
        return await tool.InvokeAsync(new AIFunctionArguments(args));
    }

    public async Task SeedCustomers()
    {
        await Store.BatchInsert(new[]
        {
            new Customer { Id = "cust-1", Name = "Alice", Age = 30, Email = "alice@test.com" },
            new Customer { Id = "cust-2", Name = "Bob", Age = 25, Email = "bob@test.com" },
            new Customer { Id = "cust-3", Name = "Carol", Age = 35, Email = "carol@test.com" },
        }, JsonContext.Customer);
    }

    public async Task SeedProducts()
    {
        await Store.BatchInsert(new[]
        {
            new Product { Id = "prod-1", Title = "Widget", Price = 9.99m, Category = "Tools" },
            new Product { Id = "prod-2", Title = "Gadget", Price = 24.99m, Category = "Electronics" },
            new Product { Id = "prod-3", Title = "Doohickey", Price = 4.99m, Category = "Tools" },
        }, JsonContext.Product);
    }

    public void Dispose() => (Store as IDisposable)?.Dispose();
}
