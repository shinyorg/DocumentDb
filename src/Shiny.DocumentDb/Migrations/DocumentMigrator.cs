namespace Shiny.DocumentDb;

/// <summary>
/// Default <see cref="IDocumentMigrator"/> — binds a store to its registered migrations and delegates to
/// <see cref="DocumentMigrationRunner"/>.
/// </summary>
public sealed class DocumentMigrator : IDocumentMigrator
{
    readonly IDocumentStore store;
    readonly IReadOnlyList<IDocumentMigration> migrations;

    public DocumentMigrator(IDocumentStore store, IEnumerable<IDocumentMigration> migrations)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(migrations);
        this.store = store;
        this.migrations = migrations.ToList();
    }

    public Task<IReadOnlyList<string>> MigrateAsync(CancellationToken cancellationToken = default)
        => DocumentMigrationRunner.RunAsync(this.store, this.migrations, cancellationToken: cancellationToken);

    public Task<IReadOnlyList<string>> RollbackToAsync(long targetVersion, CancellationToken cancellationToken = default)
        => DocumentMigrationRunner.RollbackToAsync(this.store, this.migrations, targetVersion, cancellationToken: cancellationToken);

    public Task<long> GetCurrentVersionAsync(CancellationToken cancellationToken = default)
        => DocumentMigrationRunner.GetCurrentVersionAsync(this.store, cancellationToken: cancellationToken);

    public Task<IReadOnlyList<DocumentMigrationRecord>> GetAppliedAsync(CancellationToken cancellationToken = default)
        => DocumentMigrationRunner.GetAppliedAsync(this.store, cancellationToken: cancellationToken);
}
