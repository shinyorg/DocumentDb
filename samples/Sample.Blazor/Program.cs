using System.Text.Json;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using Shiny.DocumentDb;
using Shiny.DocumentDb.IndexedDb;
using Shiny.DocumentDb.Sqlite;
using Sample.Blazor;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

var jsonContext = new AppJsonContext(new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
});

// IndexedDB store (primary)
var indexedDbOptions = new IndexedDbDocumentStoreOptions
{
    DatabaseName = "SampleApp",
    JsonSerializerOptions = jsonContext.Options,
    UseReflectionFallback = false
}
.MapTypeToStore<Customer>()
.MapTypeToStore<Order>();
builder.Services.AddSingleton(indexedDbOptions);
builder.Services.AddSingleton<IDocumentStore, IndexedDbDocumentStore>();

// SQLite store (named, in-memory for testing)
builder.Services.AddDocumentStore("sqlite", opts =>
{
    opts.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
    opts.JsonSerializerOptions = jsonContext.Options;
    opts.UseReflectionFallback = false;
    opts.MapTypeToTable<Customer>();
    opts.MapTypeToTable<Order>();
});

await builder.Build().RunAsync();
