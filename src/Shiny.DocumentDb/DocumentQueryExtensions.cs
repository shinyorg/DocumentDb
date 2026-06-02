namespace Shiny.DocumentDb;

/// <summary>
/// Convenience helpers for executing common patterns against <see cref="IDocumentQuery{T}"/>.
/// </summary>
public static class DocumentQueryExtensions
{
    /// <summary>
    /// Executes the query for a specific page and returns the records along with the total count
    /// across all pages. Any previous <c>Paginate</c> call on the query is overridden.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="query">The query builder.</param>
    /// <param name="page">
    /// The page coordinate. By default this is 1-based (page 1 is the first page). Set
    /// <paramref name="zeroBased"/> to <c>true</c> to treat it as a 0-based index.
    /// </param>
    /// <param name="pageSize">The maximum number of records to return per page. Must be greater than zero.</param>
    /// <param name="zeroBased">
    /// When <c>false</c> (default), <paramref name="page"/> is 1-based. When <c>true</c>,
    /// <paramref name="page"/> is a 0-based index.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task<PagedResults<T>> PageResult<T>(
        this IDocumentQuery<T> query,
        int page,
        int pageSize,
        bool zeroBased = false,
        CancellationToken ct = default
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        var minPage = zeroBased ? 0 : 1;
        if (page < minPage)
            throw new ArgumentOutOfRangeException(nameof(page), page, $"Page must be >= {minPage}.");

        var offset = zeroBased ? page * pageSize : (page - 1) * pageSize;

        var totalCount = await query.Count(ct).ConfigureAwait(false);
        var records = await query.Paginate(offset, pageSize).ToList(ct).ConfigureAwait(false);

        return new PagedResults<T>(
            records,
            checked((int)totalCount),
            page,
            pageSize
        );
    }
}
