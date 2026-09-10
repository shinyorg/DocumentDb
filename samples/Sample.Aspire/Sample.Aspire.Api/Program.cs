using Microsoft.AspNetCore.Mvc;
using Sample.Aspire.Domain;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Aspire.Client;

var builder = WebApplication.CreateBuilder(args);

// Provider-agnostic: the AppHost injected both the connection string and the provider discriminator,
// so swapping the backend up there needs no change down here. Same ConfigureCatalog mapping the
// AppHost's seeder used, which is why this service can read what the seeder wrote.
builder.AddDocumentStore("catalog", configureOptions: o => o.ConfigureCatalog());

var app = builder.Build();

app.MapGet("/products", (
    [FromKeyedServices("catalog")] IDocumentStore store,
    string? categoryId,
    CancellationToken ct) =>
{
    var query = store.Query<Product>();
    if (categoryId is not null)
        query = query.Where(x => x.CategoryId == categoryId);

    return query.OrderBy(x => x.Name).ToList(ct);
});

app.MapGet("/categories", (
    [FromKeyedServices("catalog")] IDocumentStore store,
    CancellationToken ct)
    => store.Query<Category>().OrderBy(x => x.Name).ToList(ct));

app.MapGet("/", () => Results.Redirect("/products"));

app.Run();
