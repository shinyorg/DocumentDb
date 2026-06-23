namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// Context passed to a <see cref="DocumentStoreResourceBuilderExtensions.WithSeeder"/> callback once
/// the backing database is ready.
/// </summary>
/// <param name="StoreName">The DocumentDb store (resource) name.</param>
/// <param name="Provider">The backing provider kind.</param>
/// <param name="ConnectionString">The resolved connection string for the backing database.</param>
public sealed record DocumentStoreSeedContext(
    string StoreName,
    DocumentProviderKind Provider,
    string ConnectionString);
