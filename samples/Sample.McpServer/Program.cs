using Sample.McpServer;
using Sample.ODataApi;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Extensions.AI;
using Shiny.DocumentDb.Mcp;
using Shiny.DocumentDb.Sqlite;

// ═══════════════════════════════════════════════════════════════════════════
// Shiny.DocumentDb — Sample MCP server (SQLite-backed, Streamable HTTP)
//
//   • The same model, JSON context and seeder as samples/Sample.ODataApi and samples/Sample.RestApi,
//     linked rather than copied — three protocols over byte-identical data.
//   • /mcp speaks the Model Context Protocol. Point Claude Code, Claude Desktop, Copilot or any other
//     MCP client at it and ask questions about the data.
//   • Customers are read-only with a hidden property; orders are read-only AND scoped per caller, which
//     is the whole point of hosting the library instead of running the stdio tool.
//
// Browse http://localhost:5097/ for client configuration and things to ask.
// ═══════════════════════════════════════════════════════════════════════════

var builder = WebApplication.CreateBuilder(args);

var dbPath = Path.Combine(Path.GetTempPath(), "sample-mcp-server.db");

builder.Services
    .AddDocumentStore(opts =>
    {
        opts.DatabaseProvider = new SqliteDatabaseProvider($"Data Source={dbPath}");
        opts.JsonSerializerOptions = SampleJsonContext.Default.Options;
        opts.UseReflectionFallback = false;
        opts.ConfigureDocument<Customer>(cfg => cfg.Table = "customers");
        opts.ConfigureDocument<Order>(cfg => cfg.Table = "orders");
    })
    .AddDocumentSeeder("sample-data", version: 2, seed: SeedData.SeedAsync);

// Where "which rows may this caller see" comes from. A real deployment reads it off the authenticated
// principal; the sample reads a header so you can switch callers with curl and watch the answers change.
builder.Services.AddHttpContextAccessor();
builder.Services.AddScoped<ICallerContext, HeaderCallerContext>();

builder.Services
    .AddDocumentDbMcpServer(mcp =>
    {
        // Read-only is the default. Writes would need BOTH mcp.AllowWrites() and a per-type write
        // capability — two locks, so an agent cannot be handed a delete tool by a one-word config change.
        mcp.AddType(
            SampleJsonContext.Default.Customer,
            name: "customer",
            configure: t => t
                .Description("Customers, one per account")
                .Property(c => c.Country, "ISO-ish two-letter country code, e.g. 'CA' or 'US'")
                .IgnoreProperties(c => c.Email)     // never reaches the model: not in the schema, not in results
                .MaxPageSize(50));

        mcp.AddType(
            SampleJsonContext.Default.Order,
            name: "order",
            configure: t => t
                .Description("Orders placed by customers")
                .Property(o => o.Status, "Pending, Shipped, Delivered or Cancelled")

                // Resolved on EVERY tool call from that call's service provider — the per-caller scope.
                // The model cannot see it, remove it, or widen past it, and it is enforced on every tool.
                .Where<ICallerContext>((caller, _) => o => o.Country == caller.Country)

                .MaxPageSize(100));

        // documentdb://types, .../{name}/schema, .../{name}/sample, documentdb://stats.
        // The two that read data run through the tools above, so the scope applies to them too.
        mcp.ExposeResources(sampleSize: 3);

        // explain-collection, build-filter
        mcp.ExposePrompts();
    })
    .WithHttpTransport();

var app = builder.Build();

app.MapDocumentDbMcp("/mcp");
// A real deployment adds authorization here, and the scope above reads the authenticated principal:
//     .RequireAuthorization("mcp");

app.MapGet("/", () => Results.Content(Explorer.Html, "text/html"));

app.Run();
