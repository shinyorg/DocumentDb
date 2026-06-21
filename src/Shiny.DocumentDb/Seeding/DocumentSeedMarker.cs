namespace Shiny.DocumentDb;

/// <summary>
/// The run-once bookkeeping record written by <see cref="DocumentSeedRunner"/> after a seeder runs.
/// Stored as an ordinary document in the same store, so it inherits the store's persistence and needs
/// no special provider support. The <see cref="Id"/> equals the seeder's <see cref="IDocumentSeeder.Name"/>.
/// </summary>
public class DocumentSeedMarker
{
    /// <summary>The seeder name this marker tracks.</summary>
    public string Id { get; set; } = null!;

    /// <summary>The highest <see cref="IDocumentSeeder.Version"/> that has been applied.</summary>
    public int Version { get; set; }

    /// <summary>When the recorded version was applied (UTC).</summary>
    public DateTimeOffset AppliedAtUtc { get; set; }
}
