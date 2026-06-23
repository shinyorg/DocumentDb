using Sample.ODataApi;
using Shiny.DocumentDb;
using Shiny.DocumentDb.AspNetCore.OData;
using Shiny.DocumentDb.Sqlite;

// ═══════════════════════════════════════════════════════════════════════════
// Shiny.DocumentDb — Sample OData v4 API (SQLite-backed)
//
//   • A SQLite document store holds Customers and Orders.
//   • A registered seeder fills the store once at startup (run-once marker).
//   • Two OData entity sets are exposed: /odata/customers and /odata/orders.
//
// Browse http://localhost:5099/ for a list of example query URLs to try.
// ═══════════════════════════════════════════════════════════════════════════

var builder = WebApplication.CreateBuilder(args);

var dbPath = Path.Combine(Path.GetTempPath(), "sample-odata-api.db");

builder.Services
    // 1. SQLite-backed document store (default, unnamed). Reflection fallback keeps the sample
    //    free of a JsonSerializerContext — the OData host is JIT-only anyway.
    .AddDocumentStore(opts =>
    {
        opts.DatabaseProvider = new SqliteDatabaseProvider($"Data Source={dbPath}");
        opts.UseReflectionFallback = true;
        opts.MapTypeToTable<Customer>("customers");
        opts.MapTypeToTable<Order>("orders");
    })
    // 2. Seed sample data once at startup. Bump the version to force a re-seed after editing SeedData.
    .AddDocumentSeeder("sample-data", version: 1, seed: SeedData.SeedAsync)
    // 3. Expose the two document types as OData entity sets (key defaults to "Id").
    .AddDocumentODataEndpoints(edm =>
    {
        edm.EntitySet<Customer>("customers");
        edm.EntitySet<Order>("orders");
    });

var app = builder.Build();

app.MapDocumentODataEntitySet<Customer>("odata/customers");
app.MapDocumentODataEntitySet<Order>("odata/orders");

app.MapGet("/", () => Results.Content(Pages.Landing, "text/html"));

app.Run();
