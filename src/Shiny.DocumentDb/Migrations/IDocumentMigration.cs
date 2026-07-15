namespace Shiny.DocumentDb;

/// <summary>
/// A versioned, run-once transform applied to an existing store's data — the store is schema-free, so a
/// "migration" is a data backfill/reshape (split a field, recompute a denormalized value, re-key
/// documents), not a DDL change.
/// <para>
/// Applied history is tracked by a ledger of <see cref="DocumentMigrationRecord"/> documents (one per
/// applied migration). A migration runs when no ledger record exists for its <see cref="Version"/>; the
/// store's current version is the highest applied <see cref="Version"/>. Because membership — not a
/// high-water mark — decides what runs, migrations authored out of order (e.g. on separate branches)
/// still apply correctly.
/// </para>
/// <para>
/// Migrations are provider-agnostic — they operate only against <see cref="IDocumentStore"/>, so the
/// same migration works against every backend. Register them with <c>AddDocumentMigration</c> (run
/// automatically at startup, before seeders, via a hosted service) or invoke
/// <see cref="DocumentMigrationRunner"/> directly (e.g. on mobile, where no generic host is present).
/// </para>
/// </summary>
public interface IDocumentMigration
{
    /// <summary>
    /// Unique, monotonically meaningful version id. Use an EF-style UTC timestamp such as
    /// <c>20260714093000</c> (yyyyMMddHHmmss) to avoid collisions when migrations are authored across
    /// branches. Recorded in the ledger; the store's current version is the maximum applied value.
    /// </summary>
    long Version { get; }

    /// <summary>
    /// Human-readable name for logs and the ledger record. Not used for run-once identity (that is
    /// <see cref="Version"/>), so renaming a migration does not cause it to re-run.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Applies the migration. Make writes idempotent — the ledger record is written only after this
    /// completes, so a mid-migration failure re-runs the whole migration on the next start (at-least-once).
    /// </summary>
    Task Up(IDocumentStore store, CancellationToken cancellationToken);

    /// <summary>
    /// Reverts the migration. Optional — the default throws <see cref="NotSupportedException"/>. Override
    /// it to make the migration reversible via <see cref="DocumentMigrationRunner.RollbackToAsync"/> /
    /// <see cref="IDocumentMigrator.RollbackToAsync"/>.
    /// </summary>
    Task Down(IDocumentStore store, CancellationToken cancellationToken)
        => throw new NotSupportedException($"Migration '{this.Name}' ({this.Version}) does not support rollback (no Down implementation).");
}
