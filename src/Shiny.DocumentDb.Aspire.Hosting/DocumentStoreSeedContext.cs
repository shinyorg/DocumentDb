namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// Context passed to a <see cref="DocumentStoreResourceBuilderExtensions.WithSeeder"/> callback once
/// the backing database is ready and the seed gate has decided the callback should run.
/// </summary>
/// <param name="StoreName">The DocumentDb store (resource) name.</param>
/// <param name="Provider">The backing provider kind.</param>
/// <param name="ConnectionString">The resolved connection string for the backing database.</param>
/// <param name="Recreate">
/// Which of the two seed triggers fired. <c>false</c> means first-time setup — the database carried no
/// seed marker, so there is nothing to clear. <c>true</c> means destructive recreation — the store was
/// already seeded and <see cref="DocumentStoreSeedMode.Recreate"/> asked for it to be rebuilt, so the
/// callback should wipe the existing data (e.g. <c>IDocumentMaintenance.ClearAll</c>) before seeding.
/// </param>
public sealed record DocumentStoreSeedContext(
    string StoreName,
    DocumentProviderKind Provider,
    string ConnectionString,
    bool Recreate);
