using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// Applies <see cref="IDocumentMigration"/>s to a store with versioned, run-once semantics, and reverts
/// them. The DI integration calls this from a hosted service at startup (before seeders); call it
/// directly when there's no generic host (mobile apps) or to migrate a specific named store.
/// </summary>
public static class DocumentMigrationRunner
{
    /// <summary>
    /// Runs each migration whose <see cref="IDocumentMigration.Version"/> has no
    /// <see cref="DocumentMigrationRecord"/> yet, in ascending version order, recording each after its
    /// <see cref="IDocumentMigration.Up"/> succeeds. Returns the names of the migrations that ran.
    /// </summary>
    /// <param name="markerTypeInfo">
    /// Optional AOT-safe metadata for the internal <see cref="DocumentMigrationRecord"/>. Required when
    /// the store is configured without reflection fallback; otherwise leave null.
    /// </param>
    public static async Task<IReadOnlyList<string>> RunAsync(
        IDocumentStore store,
        IEnumerable<IDocumentMigration> migrations,
        JsonTypeInfo<DocumentMigrationRecord>? markerTypeInfo = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(migrations);

        var appliedVersions = await GetAppliedVersionsAsync(store, markerTypeInfo, cancellationToken)
            .ConfigureAwait(false);

        var applied = new List<string>();
        var pending = migrations
            .Where(m => !appliedVersions.Contains(m.Version))
            .OrderBy(m => m.Version);

        foreach (var migration in pending)
        {
            await migration.Up(store, cancellationToken).ConfigureAwait(false);
            await store
                .Upsert(
                    new DocumentMigrationRecord
                    {
                        Id = migration.Version.ToString(),
                        Version = migration.Version,
                        Name = migration.Name,
                        AppliedAtUtc = DateTimeOffset.UtcNow
                    },
                    markerTypeInfo,
                    cancellationToken)
                .ConfigureAwait(false);
            applied.Add(migration.Name);
        }
        return applied;
    }

    /// <summary>
    /// Reverts every applied migration with <see cref="IDocumentMigration.Version"/> greater than
    /// <paramref name="targetVersion"/>, in descending version order, calling
    /// <see cref="IDocumentMigration.Down"/> then removing its ledger record. Pass <c>0</c> to revert
    /// everything. Throws <see cref="NotSupportedException"/> if a migration in range has no rollback.
    /// Returns the names of the migrations that were reverted.
    /// </summary>
    public static async Task<IReadOnlyList<string>> RollbackToAsync(
        IDocumentStore store,
        IEnumerable<IDocumentMigration> migrations,
        long targetVersion,
        JsonTypeInfo<DocumentMigrationRecord>? markerTypeInfo = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(migrations);

        var appliedVersions = await GetAppliedVersionsAsync(store, markerTypeInfo, cancellationToken)
            .ConfigureAwait(false);

        var reverted = new List<string>();
        var toRevert = migrations
            .Where(m => m.Version > targetVersion && appliedVersions.Contains(m.Version))
            .OrderByDescending(m => m.Version);

        foreach (var migration in toRevert)
        {
            await migration.Down(store, cancellationToken).ConfigureAwait(false);
            await store
                .Remove<DocumentMigrationRecord>(migration.Version.ToString(), cancellationToken)
                .ConfigureAwait(false);
            reverted.Add(migration.Name);
        }
        return reverted;
    }

    /// <summary>
    /// Returns the applied-migration ledger, ordered by version.
    /// </summary>
    public static async Task<IReadOnlyList<DocumentMigrationRecord>> GetAppliedAsync(
        IDocumentStore store,
        JsonTypeInfo<DocumentMigrationRecord>? markerTypeInfo = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        var records = await store
            .Query(markerTypeInfo)
            .ToList(cancellationToken)
            .ConfigureAwait(false);
        return records.OrderBy(r => r.Version).ToList();
    }

    /// <summary>
    /// Returns the store's current data version — the highest applied migration version, or <c>0</c> if
    /// none have run.
    /// </summary>
    public static async Task<long> GetCurrentVersionAsync(
        IDocumentStore store,
        JsonTypeInfo<DocumentMigrationRecord>? markerTypeInfo = null,
        CancellationToken cancellationToken = default)
    {
        var applied = await GetAppliedAsync(store, markerTypeInfo, cancellationToken).ConfigureAwait(false);
        return applied.Count == 0 ? 0 : applied.Max(r => r.Version);
    }

    static async Task<HashSet<long>> GetAppliedVersionsAsync(
        IDocumentStore store,
        JsonTypeInfo<DocumentMigrationRecord>? markerTypeInfo,
        CancellationToken cancellationToken)
    {
        var records = await store
            .Query(markerTypeInfo)
            .ToList(cancellationToken)
            .ConfigureAwait(false);
        return records.Select(r => r.Version).ToHashSet();
    }
}
