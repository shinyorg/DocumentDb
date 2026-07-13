namespace Shiny.DocumentDb.Geo;

/// <summary>
/// Seeds the embedded reference geographic dataset (<see cref="GeoDataSets"/>) — US states, Canadian
/// provinces, and US &amp; Canadian cities — into a document store. Provider-agnostic and idempotent:
/// every record is upserted on a deterministic id, so a re-run (after a version bump) converges cleanly.
/// </summary>
public sealed class GeoReferenceSeeder : IDocumentSeeder
{
    /// <summary>The stable seeder name, also used as the run-once marker id.</summary>
    public const string SeederName = "shiny-geo-reference";

    /// <inheritdoc />
    public string Name => SeederName;

    /// <inheritdoc />
    public int Version => 1;

    /// <inheritdoc />
    public async Task SeedAsync(IDocumentStore store, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(store);

        await store.BatchUpsert(GeoDataSets.Regions, cancellationToken: cancellationToken).ConfigureAwait(false);
        await store.BatchUpsert(GeoDataSets.Cities, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
