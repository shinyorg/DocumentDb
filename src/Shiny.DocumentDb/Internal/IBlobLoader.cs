namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Fetches blob payloads on demand for the document that owns them. Stamped onto every
/// <see cref="DocumentBlob"/> (and <see cref="DocumentBlobCollection"/>) by the store during hydration, so a
/// blob can load itself without a separate store call. One instance serves a single document.
/// </summary>
public interface IBlobLoader
{
    /// <summary>Reads one payload by its key. Null when the sidecar row is absent.</summary>
    Task<byte[]?> LoadOneAsync(string blobKey, CancellationToken cancellationToken);

    /// <summary>Reads the payloads for the given blobs in one round trip and attaches each in place.</summary>
    Task LoadManyAsync(IReadOnlyList<DocumentBlob> blobs, CancellationToken cancellationToken);
}
