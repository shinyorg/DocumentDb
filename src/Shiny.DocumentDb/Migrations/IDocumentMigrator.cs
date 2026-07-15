namespace Shiny.DocumentDb;

/// <summary>
/// A DI-resolvable façade over <see cref="DocumentMigrationRunner"/> bound to a store and its registered
/// migrations. Resolve it to inspect or drive migrations manually (the hosted service applies pending
/// migrations at startup automatically). For a named store, resolve the keyed
/// <see cref="IDocumentMigrator"/> registered under that store's name.
/// </summary>
public interface IDocumentMigrator
{
    /// <summary>Applies all pending migrations, in ascending version order. Returns the names that ran.</summary>
    Task<IReadOnlyList<string>> MigrateAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reverts every applied migration newer than <paramref name="targetVersion"/> (pass <c>0</c> to
    /// revert all), in descending order. Returns the names that were reverted.
    /// </summary>
    Task<IReadOnlyList<string>> RollbackToAsync(long targetVersion, CancellationToken cancellationToken = default);

    /// <summary>The current data version — the highest applied migration version, or <c>0</c> if none.</summary>
    Task<long> GetCurrentVersionAsync(CancellationToken cancellationToken = default);

    /// <summary>The applied-migration ledger, ordered by version.</summary>
    Task<IReadOnlyList<DocumentMigrationRecord>> GetAppliedAsync(CancellationToken cancellationToken = default);
}
