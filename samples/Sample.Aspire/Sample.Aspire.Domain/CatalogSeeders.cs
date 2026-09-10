using Shiny.DocumentDb;

namespace Sample.Aspire.Domain;

/// <summary>
/// Ordinary <see cref="IDocumentSeeder"/>s — nothing Aspire-specific about them. They work unchanged
/// against every provider, and against a plain <c>AddDocumentSeeder</c> registration in a service that
/// isn't running under an AppHost at all.
/// </summary>
public static class CatalogSeeders
{
    public static IReadOnlyList<IDocumentSeeder> All { get; } =
    [
        new CategorySeeder(),
        new ProductSeeder()
    ];
}

public sealed class CategorySeeder : IDocumentSeeder
{
    public string Name => "catalog.categories";
    public int Version => 1;

    public Task SeedAsync(IDocumentStore store, CancellationToken cancellationToken)
        => store.BatchUpsert<Category>(
            [
                new() { Id = "tools", Name = "Tools" },
                new() { Id = "toys", Name = "Toys" }
            ],
            cancellationToken: cancellationToken
        );
}

public sealed class ProductSeeder : IDocumentSeeder
{
    public string Name => "catalog.products";
    public int Version => 1;

    // Upsert on known ids, so a re-run after a bumped Version converges instead of duplicating.
    public Task SeedAsync(IDocumentStore store, CancellationToken cancellationToken)
        => store.BatchUpsert<Product>(
            [
                new() { Id = "widget", Name = "Widget", CategoryId = "tools", Price = 9.99m, Stock = 120 },
                new() { Id = "gadget", Name = "Gadget", CategoryId = "tools", Price = 24.50m, Stock = 45 },
                new() { Id = "gizmo", Name = "Gizmo", CategoryId = "toys", Price = 5.00m, Stock = 300 },
                new() { Id = "doohickey", Name = "Doohickey", CategoryId = "toys", Price = 14.25m, Stock = 8 }
            ],
            cancellationToken: cancellationToken
        );
}
