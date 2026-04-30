using System.ClientModel;
using System.ClientModel.Primitives;
using System.Text.Json;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenAI;
using Sample.CopilotConsole;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Extensions.AI;
using Shiny.DocumentDb.Sqlite;

var builder = Host.CreateApplicationBuilder(args);

// ── DocumentDb setup ───────────────────────────────────────────────
var jsonContext = new SampleJsonContext(new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
});

builder.Services.AddDocumentStore(opts =>
{
    opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=copilot-sample.db");
    opts.JsonSerializerOptions = jsonContext.Options;
});

// ── Register AI tools for DocumentDb types ─────────────────────────
builder.Services.AddDocumentStoreAITools(tools =>
{
    tools.AddType(
        jsonContext.Customer,
        capabilities: DocumentAICapabilities.All,
        configure: b => b
            .Description("Customer records with contact information")
            .MaxPageSize(50)
    );

    tools.AddType(
        jsonContext.Order,
        capabilities: DocumentAICapabilities.All,
        configure: b => b
            .Description("Customer orders with shipping and line items")
            .Property(o => o.Status, "Order status: Pending, Shipped, Delivered, or Cancelled")
            .MaxPageSize(100)
    );
});

var host = builder.Build();

// ── Seed sample data ───────────────────────────────────────────────
var store = host.Services.GetRequiredService<IDocumentStore>();
if (await store.Count<Customer>() == 0)
{
    await store.BatchInsert(new[]
    {
        new Customer { Id = "cust-1", Name = "Alice Smith", Age = 32, Email = "alice@example.com" },
        new Customer { Id = "cust-2", Name = "Bob Jones", Age = 45, Email = "bob@example.com" },
        new Customer { Id = "cust-3", Name = "Carol White", Age = 28, Email = "carol@example.com" }
    });

    await store.BatchInsert(new[]
    {
        new Order
        {
            Id = "ord-1", CustomerName = "Alice Smith", Status = "Shipped",
            ShippingAddress = new Address { Street = "123 Main St", City = "Portland", State = "OR" },
            Lines = [ new OrderLine { ProductName = "Widget", Quantity = 2, UnitPrice = 9.99m } ],
            Tags = ["priority", "wholesale"]
        },
        new Order
        {
            Id = "ord-2", CustomerName = "Bob Jones", Status = "Pending",
            ShippingAddress = new Address { Street = "456 Oak Ave", City = "Seattle", State = "WA" },
            Lines = [
                new OrderLine { ProductName = "Gadget", Quantity = 1, UnitPrice = 24.99m },
                new OrderLine { ProductName = "Doohickey", Quantity = 3, UnitPrice = 4.99m }
            ],
            Tags = ["retail"]
        },
        new Order
        {
            Id = "ord-3", CustomerName = "Alice Smith", Status = "Delivered",
            ShippingAddress = new Address { Street = "123 Main St", City = "Portland", State = "OR" },
            Lines = [ new OrderLine { ProductName = "Thingamajig", Quantity = 5, UnitPrice = 14.99m } ],
            Tags = ["priority"]
        }
    });

    Console.WriteLine("Seeded sample data: 3 customers, 3 orders");
}

// ── Authenticate with GitHub Copilot ───────────────────────────────
using var http = new HttpClient();
Console.WriteLine("Authenticating with GitHub Copilot...");
var session = await GitHubCopilotAuth.GetSessionAsync(http);

var transport = new CopilotTokenHandler(session.Token)
{
    InnerHandler = new HttpClientHandler()
};

var openAiClient = new OpenAIClient(
    new ApiKeyCredential("copilot-placeholder"),
    new OpenAIClientOptions
    {
        Transport = new HttpClientPipelineTransport(new HttpClient(transport)),
        Endpoint = new Uri("https://api.githubcopilot.com")
    }
);

IChatClient chatClient = openAiClient
    .GetChatClient("gpt-4.1")
    .AsIChatClient()
    .AsBuilder()
    .UseFunctionInvocation()
    .Build(host.Services);

// ── Get AI tools from the DocumentDb registration ──────────────────
var aiTools = host.Services.GetRequiredService<DocumentStoreAITools>();

Console.WriteLine();
Console.WriteLine($"Registered {aiTools.Tools.Count} AI tool(s):");
foreach (var tool in aiTools.Tools)
{
    if (tool is AIFunction fn)
        Console.WriteLine($"  - {fn.Name}: {fn.Description}");
}

Console.WriteLine();
Console.WriteLine("Chat with GitHub Copilot (type 'exit' to quit)");
Console.WriteLine("The AI can query and manage your customers and orders.");
Console.WriteLine("Try: 'How many customers do we have?', 'Show me all pending orders', 'Add a new customer named Dave'");
Console.WriteLine(new string('-', 60));

var history = new List<ChatMessage>();
var options = new ChatOptions { Tools = aiTools.Tools.ToList() };

while (true)
{
    Console.Write("\nYou: ");
    var input = Console.ReadLine();
    if (string.IsNullOrWhiteSpace(input) || input.Equals("exit", StringComparison.OrdinalIgnoreCase))
        break;

    history.Add(new ChatMessage(ChatRole.User, input));

    try
    {
        var response = await chatClient.GetResponseAsync(history, options);
        history.AddRange(response.Messages);

        var text = response.Text;
        if (!string.IsNullOrWhiteSpace(text))
            Console.WriteLine($"\nCopilot: {text}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\nError: {ex.Message}");
    }
}

Console.WriteLine("\nGoodbye!");
