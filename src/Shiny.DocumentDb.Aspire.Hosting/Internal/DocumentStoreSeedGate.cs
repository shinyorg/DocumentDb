using Microsoft.Extensions.Logging;

namespace Shiny.DocumentDb.Aspire.Hosting.Internal;

/// <summary>
/// Decides whether a <c>WithSeeder</c> callback runs on this AppHost start, and on which of the two
/// triggers: first-time setup, or destructive recreation. Split out of the eventing subscription so
/// the decision is testable without spinning an Aspire application.
/// </summary>
internal static class DocumentStoreSeedGate
{
    public static async Task RunAsync(
        string storeName,
        DocumentProviderKind kind,
        string connectionString,
        DocumentStoreSeedMode mode,
        Func<DocumentStoreSeedContext, CancellationToken, Task> seed,
        ILogger logger,
        CancellationToken ct)
    {
        var marker = new SeedMarkerStore(kind, connectionString);
        var seededAt = await marker.ReadMarkerAsync(storeName, ct);
        var alreadySeeded = seededAt is not null;

        if (alreadySeeded && mode == DocumentStoreSeedMode.FirstTimeOnly)
        {
            logger.LogInformation(
                "DocumentDb store '{StoreName}' was already seeded at {SeededAtUtc} — skipping (mode {Mode})",
                storeName,
                seededAt,
                mode);

            return;
        }

        // The two triggers: no marker => first-time setup; marker + Recreate mode => destructive rebuild.
        var recreate = alreadySeeded;
        if (recreate)
            logger.LogWarning(
                "Recreating DocumentDb store '{StoreName}' ({Provider}) — it was seeded at {SeededAtUtc} and the seeder is in {Mode} mode. Existing data will be destroyed by the seed callback.",
                storeName,
                kind,
                seededAt,
                mode);
        else
            logger.LogInformation(
                "Seeding DocumentDb store '{StoreName}' ({Provider}) for the first time",
                storeName,
                kind);

        await seed(new DocumentStoreSeedContext(storeName, kind, connectionString, recreate), ct);

        // Written only on success, so a failed seed is retried on the next start rather than silently skipped.
        await marker.WriteMarkerAsync(storeName, DateTimeOffset.UtcNow, ct);

        logger.LogInformation("DocumentDb seeder completed for store '{StoreName}'", storeName);
    }
}
