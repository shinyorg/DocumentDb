namespace Shiny.DocumentDb;

/// <summary>
/// Internal pairing of a seeder with the store it targets. <see cref="StoreName"/> null means the
/// default <see cref="IDocumentStore"/>; otherwise the keyed store registered with that name.
/// </summary>
sealed class DocumentSeederRegistration
{
    public string? StoreName { get; init; }
    public required IDocumentSeeder Seeder { get; init; }
}
