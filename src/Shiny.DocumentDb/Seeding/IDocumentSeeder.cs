namespace Shiny.DocumentDb;

/// <summary>
/// A unit of initial data seeded into a document store once (per <see cref="Version"/>). The store is
/// schema-free, so seeding is simply a set of idempotent writes — there is no schema to reconcile.
/// <para>
/// Run-once semantics are tracked by a <see cref="DocumentSeedMarker"/> document keyed on <see cref="Name"/>:
/// a seeder runs when no marker exists, or when its <see cref="Version"/> is greater than the recorded
/// version. Bump <see cref="Version"/> to re-run after changing the seed data.
/// </para>
/// <para>
/// Seeders are provider-agnostic — they operate only against <see cref="IDocumentStore"/>, so the same
/// seeder works against every backend. Register them with <c>AddDocumentSeeder</c> (run automatically at
/// startup via a hosted service) or invoke <see cref="DocumentSeedRunner"/> directly (e.g. on mobile,
/// where no generic host is present).
/// </para>
/// </summary>
public interface IDocumentSeeder
{
    /// <summary>
    /// Stable, unique name for this seeder. Used as the id of the <see cref="DocumentSeedMarker"/> that
    /// records whether/which version has been applied. Renaming a seeder causes it to run again.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Monotonically increasing version. The seeder runs when no marker exists or the recorded marker
    /// version is lower than this value. Bump it to re-run after changing seed data.
    /// </summary>
    int Version { get; }

    /// <summary>
    /// Writes the seed data. Make writes idempotent (e.g. <see cref="IDocumentStore.Upsert"/> on known
    /// ids) so a re-run after a bumped version converges cleanly.
    /// </summary>
    Task SeedAsync(IDocumentStore store, CancellationToken cancellationToken);
}
