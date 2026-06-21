using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// Runs <see cref="IDocumentSeeder"/>s against a store with versioned run-once semantics. The DI
/// integration calls this from a hosted service at startup; call it directly when there's no generic
/// host (mobile apps) or to seed a specific named store.
/// </summary>
public static class DocumentSeedRunner
{
    /// <summary>
    /// Runs each seeder whose <see cref="IDocumentSeeder.Version"/> is newer than the recorded
    /// <see cref="DocumentSeedMarker"/> (or that has never run), then records the new version. Seeders
    /// run in the order supplied. Returns the names of the seeders that actually ran.
    /// </summary>
    /// <param name="markerTypeInfo">
    /// Optional AOT-safe metadata for the internal <see cref="DocumentSeedMarker"/>. Required when the
    /// store is configured without reflection fallback; otherwise leave null.
    /// </param>
    public static async Task<IReadOnlyList<string>> RunAsync(
        IDocumentStore store,
        IEnumerable<IDocumentSeeder> seeders,
        JsonTypeInfo<DocumentSeedMarker>? markerTypeInfo = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(seeders);

        var applied = new List<string>();
        foreach (var seeder in seeders)
        {
            var marker = await store
                .Get(seeder.Name, markerTypeInfo, cancellationToken)
                .ConfigureAwait(false);

            var alreadyApplied = marker != null && marker.Version >= seeder.Version;
            if (!alreadyApplied)
            {
                await seeder.SeedAsync(store, cancellationToken).ConfigureAwait(false);
                await store
                    .Upsert(
                        new DocumentSeedMarker
                        {
                            Id = seeder.Name,
                            Version = seeder.Version,
                            AppliedAtUtc = DateTimeOffset.UtcNow
                        },
                        markerTypeInfo,
                        cancellationToken)
                    .ConfigureAwait(false);
                applied.Add(seeder.Name);
            }
        }
        return applied;
    }
}
