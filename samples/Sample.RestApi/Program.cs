using System.Threading.RateLimiting;
using Microsoft.AspNetCore.RateLimiting;
using Sample.ODataApi;
using Shiny.DocumentDb;
using Shiny.DocumentDb.AspNetCore;
using Shiny.DocumentDb.Sqlite;

// ═══════════════════════════════════════════════════════════════════════════
// Shiny.DocumentDb — Sample REST + SSE API (SQLite-backed)
//
//   • The same model, JSON context and seeder as samples/Sample.ODataApi, linked rather than copied — so
//     the two APIs sit over byte-identical data and can be compared request for request.
//   • /customers and /orders are full REST resources: list, by-id, count, create, replace, merge-patch,
//     delete, and a live SSE tail at /orders/stream.
//
// Browse http://localhost:5098/ for example requests.
// ═══════════════════════════════════════════════════════════════════════════

var builder = WebApplication.CreateBuilder(args);

var dbPath = Path.Combine(Path.GetTempPath(), "sample-rest-api.db");

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

// A live tail is one long-lived request per client per resource. Without a cap, a browser-tab storm
// exhausts the connection budget — so /orders/stream sits behind a small concurrency limiter.
builder.Services.AddRateLimiter(o => o.AddConcurrencyLimiter("stream", limiter =>
{
    limiter.PermitLimit = 20;
    limiter.QueueLimit = 0;
}));

builder.Services.AddOpenApi();

var app = builder.Build();
app.UseRateLimiter();
app.MapOpenApi();

app.MapDocuments<Customer>("/customers", o =>
{
    o.Operations = DocumentEndpoints.Read | DocumentEndpoints.Count | DocumentEndpoints.Write | DocumentEndpoints.Delete;
    o.TypeInfo = SampleJsonContext.Default.Customer;

    // A public resource names what may be filtered. Anything else is a 400 rather than a table scan.
    o.AllowFilterOn(x => x.Name, x => x.Country, x => x.Age, x => x.IsActive, x => x.Created, x => x.Email);
    o.DefaultOrderBy = x => x.Id;
    o.MaxPageSize = 100;
});

app.MapDocuments<Order>("/orders", o =>
{
    o.Operations = DocumentEndpoints.All;
    o.TypeInfo = SampleJsonContext.Default.Order;
    o.AllowFilterOn(x => x.CustomerId, x => x.CustomerName, x => x.Status, x => x.Total, x => x.Country, x => x.Placed);
    o.DefaultOrderBy = x => x.Id;
    o.MaxPageSize = 100;
})
.RequireRateLimiting("stream");

app.MapGet("/", () => Results.Content(Explorer.Html, "text/html"));

app.Run();

static class Explorer
{
    public const string Html =
        """
        <!doctype html>
        <meta charset="utf-8">
        <title>Shiny.DocumentDb — REST sample</title>
        <style>
          body { font: 15px/1.6 system-ui, sans-serif; max-width: 60rem; margin: 3rem auto; padding: 0 1rem; }
          code { background: #f4f4f5; padding: .1rem .3rem; border-radius: .2rem; }
          li { margin: .3rem 0; }
        </style>
        <h1>Shiny.DocumentDb — REST + SSE sample</h1>
        <p>Same data as <code>samples/Sample.ODataApi</code>, exposed as plain REST.</p>
        <h2>Reads</h2>
        <ul>
          <li><a href="/customers">/customers</a></li>
          <li><a href="/customers?filter=country%20%3D%3D%20'CA'%20and%20age%20%3E%2030">/customers?filter=country == 'CA' and age &gt; 30</a></li>
          <li><a href="/customers?orderby=age%20desc&take=5">/customers?orderby=age desc&amp;take=5</a></li>
          <li><a href="/customers?fields=id,name,country">/customers?fields=id,name,country</a> — sparse fieldset</li>
          <li><a href="/customers?cursor=&take=25">/customers?cursor=&amp;take=25</a> — cursor paging</li>
          <li><a href="/orders/count?filter=status%20%3D%3D%20'Shipped'">/orders/count?filter=status == 'Shipped'</a></li>
          <li><a href="/customers?filter=notes%20%3D%3D%20'x'">/customers?filter=notes == 'x'</a> — <b>400</b>: not on the allowlist</li>
        </ul>
        <h2>Live tail</h2>
        <pre><code>curl -N http://localhost:5098/orders/stream?filter=status%20==%20'Pending'</code></pre>
        <p>Then write something and watch it arrive:</p>
        <pre><code>curl -X POST http://localhost:5098/orders -H 'content-type: application/json' \
             -d '{"id":"demo-1","customerId":"c1","customerName":"Alice Johnson","status":"Pending","total":42,"country":"CA","placed":"2026-01-01T00:00:00Z"}'</code></pre>
        <h2>Writes</h2>
        <pre><code>curl -X PATCH http://localhost:5098/orders/demo-1 -H 'content-type: application/json' -d '{"status":"Shipped"}'
        curl -i http://localhost:5098/orders/demo-1          # note the ETag
        curl -X DELETE http://localhost:5098/orders/demo-1 -H 'If-Match: "1"'</code></pre>
        <p><a href="/openapi/v1.json">OpenAPI document</a></p>
        """;
}
