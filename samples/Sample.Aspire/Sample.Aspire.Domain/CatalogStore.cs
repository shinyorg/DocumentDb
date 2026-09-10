using Shiny.DocumentDb;

namespace Sample.Aspire.Domain;

/// <summary>
/// The store's mapping, in ONE place both halves of the sample use.
/// <para>
/// This matters more than it looks. The AppHost seeder and the API each build their own
/// <see cref="IDocumentStore"/> over the same database — if their mapping disagrees (a different
/// <c>Table</c>, say) the seeder happily fills tables the API never reads, and the API comes up
/// looking empty. Sharing one configure method is the cheap way to make that impossible.
/// </para>
/// </summary>
public static class CatalogStore
{
    public static IDocumentStoreOptions ConfigureCatalog(this IDocumentStoreOptions options)
        => options
            .ConfigureDocument<Category>(cfg => cfg.Table = "categories")
            .ConfigureDocument<Product>(cfg => cfg.Table = "products");
}
