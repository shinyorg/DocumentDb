namespace Shiny.DocumentDb;

/// <summary>One forward page of a cursor/keyset paginated query.</summary>
/// <typeparam name="T">The document type.</typeparam>
/// <param name="Items">The documents in this page (at most the requested <c>take</c>).</param>
/// <param name="NextCursor">
/// The opaque continuation token to pass to the next <see cref="IDocumentQuery{T}.ToCursorPage"/> call,
/// or <c>null</c> when this is the last page.
/// </param>
public sealed record CursorPage<T>(
    IReadOnlyList<T> Items,
    string? NextCursor)
{
    /// <summary>True when another page follows (i.e. <see cref="NextCursor"/> is non-null).</summary>
    public bool HasMore => this.NextCursor != null;
}
