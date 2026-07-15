namespace Shiny.DocumentDb;

/// <summary>
/// The ledger record written by <see cref="DocumentMigrationRunner"/> after a migration's
/// <see cref="IDocumentMigration.Up"/> succeeds (one record per applied migration). Stored as an
/// ordinary document in the same store, so it inherits the store's persistence and needs no special
/// provider support. The <see cref="Id"/> is the migration's <see cref="IDocumentMigration.Version"/>
/// as a string.
/// </summary>
public class DocumentMigrationRecord
{
    /// <summary>The applied migration's version, as a string (used as the document id).</summary>
    public string Id { get; set; } = null!;

    /// <summary>The applied migration's <see cref="IDocumentMigration.Version"/>.</summary>
    public long Version { get; set; }

    /// <summary>The applied migration's <see cref="IDocumentMigration.Name"/>, for readability.</summary>
    public string Name { get; set; } = null!;

    /// <summary>When the migration was applied (UTC).</summary>
    public DateTimeOffset AppliedAtUtc { get; set; }
}
