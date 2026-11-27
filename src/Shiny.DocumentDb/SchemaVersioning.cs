using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>Maintenance for document types with upcasters (<see cref="DocumentTypeBuilder{T}.AddUpcaster"/>).</summary>
public static class SchemaVersioningExtensions
{
    /// <summary>
    /// The schema version documents of <typeparamref name="T"/> are written at, or null when the type has no
    /// upcasters.
    /// </summary>
    public static int? GetSchemaVersion<T>(this IDocumentStore store) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        return DocumentShapes.CurrentVersion(typeof(T));
    }

    /// <summary>
    /// Rewrites every stored <typeparamref name="T"/> that is older than the current schema version, so queries,
    /// indexes and other readers of the raw JSON see the upgraded shape. Documents are read page by page (upcast on
    /// the way in) and the ones that needed an upcast are written back with <c>BatchUpdate</c>; current documents are
    /// left alone. Returns how many were rewritten.
    /// <para>
    /// It runs through the store's normal API, so query filters apply (a soft-deleted or filtered-out document is not
    /// upgraded, but still upcasts whenever it is read), and write interceptors, version checks and temporal history
    /// see an ordinary update. Safe to re-run; a second run finds nothing to do.
    /// </para>
    /// </summary>
    public static async Task<int> UpgradeDocuments<T>(this IDocumentStore store, int batchSize = 500, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        if (DocumentShapes.CurrentVersion(typeof(T)) == null)
            return 0;

        var total = 0;
        string? cursor = null;
        do
        {
            var page = await store.Query<T>().ToCursorPage(cursor, batchSize, cancellationToken).ConfigureAwait(false);
            var stale = page.Items.Where(DocumentShapes.WasUpcast).ToList();
            if (stale.Count > 0)
            {
                total += await store.BatchUpdate(stale, cancellationToken: cancellationToken).ConfigureAwait(false);
                stale.ForEach(DocumentShapes.ClearUpcast);
            }
            cursor = page.NextCursor;
        }
        while (cursor != null);

        return total;
    }
}
