namespace Shiny.DocumentDb;

/// <summary>
/// Represents a single change to a document of type <typeparamref name="T"/>, delivered to
/// observers registered via <see cref="IObservableDocumentStore.NotifyOnChange{T}"/>.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public sealed class DocumentChange<T> where T : class
{
    /// <summary>The kind of change that occurred.</summary>
    public DocumentChangeType ChangeType { get; init; }

    /// <summary>
    /// The string Id of the affected document. Empty for <see cref="DocumentChangeType.Cleared"/>,
    /// which affects every document of the type.
    /// </summary>
    public string Id { get; init; } = "";

    /// <summary>
    /// The document instance that was written, when available. This is populated for
    /// <see cref="DocumentChangeType.Inserted"/> and the full-document
    /// <see cref="DocumentChangeType.Updated"/> paths (<c>Update</c>/<c>Upsert</c>). It is
    /// <c>null</c> for <see cref="DocumentChangeType.Removed"/>, <see cref="DocumentChangeType.Cleared"/>,
    /// and the property-level update paths (<c>SetProperty</c>/<c>RemoveProperty</c>) where the
    /// full document is not materialized.
    /// </summary>
    public T? Document { get; init; }
}
