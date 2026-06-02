namespace Shiny.DocumentDb;

/// <summary>
/// A page of query results along with the total number of matching documents and the page coordinates.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
/// <param name="Records">The documents on the current page.</param>
/// <param name="TotalCount">The total number of documents matching the query (across all pages).</param>
/// <param name="Page">The page coordinate that was requested.</param>
/// <param name="PageSize">The page size that was requested.</param>
public record PagedResults<T>(
    IEnumerable<T> Records,
    int TotalCount,
    int Page,
    int PageSize
);
