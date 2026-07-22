namespace Shiny.DocumentDb;

/// <summary>
/// Store-level access to blob payloads for the two cases a blob instance cannot serve itself. For everything
/// else, blobs self-load: <see cref="DocumentBlob.LoadAsync"/> pulls one payload and
/// <see cref="DocumentBlobCollection.LoadAllAsync"/> pulls a whole collection in one round trip. Implemented
/// by stores whose provider reports a non-zero <see cref="IDocumentStore.MaxBlobSize"/>; probe with
/// <c>store is IBlobDocumentStore</c>.
/// </summary>
/// <remarks>
/// <b>Reads only, by design.</b> There is no <c>SetBlob</c>/<c>DeleteBlob</c>: payloads are written by
/// assigning the document's blob member and saving the document (<c>invoice.Pdf = DocumentBlob.FromBytes(…)</c>,
/// then <c>Upsert</c>; <c>invoice.Pdf = null</c> to drop it). Routing every mutation through the document is
/// what keeps the metadata carried in the document body from ever disagreeing with the stored bytes.
/// </remarks>
public interface IBlobDocumentStore
{
    /// <summary>
    /// Fetches every mapped payload across a set of documents in a single round trip — the page-level answer,
    /// since awaiting <see cref="DocumentBlobCollection.LoadAllAsync"/> per document over a page is an N+1.
    /// </summary>
    Task BatchLoadBlobs<T>(IEnumerable<T> documents, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Reads one payload by document id and blob key without materializing the document. Returns null when
    /// the document or the key has no stored payload.
    /// </summary>
    Task<byte[]?> GetBlob<T>(object id, string key, CancellationToken cancellationToken = default) where T : class;
}
