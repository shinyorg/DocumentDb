namespace Shiny.DocumentDb;

/// <summary>Per-mapping configuration for <c>MapBlob</c> / <c>MapBlobCollection</c>.</summary>
public class BlobOptions
{
    /// <summary>
    /// Overrides the sidecar key for a single-blob property (default: the property name). Not valid on a
    /// collection mapping — collection items carry their own keys.
    /// </summary>
    public string? Key { get; set; }

    /// <summary>
    /// Computes a SHA-256 of each payload on write and exposes it as <see cref="DocumentBlob.Hash"/>.
    /// Off by default — hashing every write is real CPU on large payloads and most callers never read it.
    /// </summary>
    public bool ComputeHash { get; set; }

    /// <summary>
    /// Per-mapping size ceiling in bytes. 0 (the default) defers entirely to the provider's
    /// <see cref="IDocumentStore.MaxBlobSize"/>; a non-zero value is additionally enforced, and can only make
    /// the limit smaller.
    /// </summary>
    public long MaxSize { get; set; }
}
