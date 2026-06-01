namespace Shiny.DocumentDb;

/// <summary>
/// An optional capability implemented by document stores that can notify observers when
/// documents change.
/// <para>
/// Notifications are <b>in-process</b>: they are raised for inserts, updates, removes and clears
/// performed through <i>this</i> store instance. Changes made by other processes, other store
/// instances, or directly against the underlying database are not observed. This is the common
/// case for local-first apps (for example .NET MAUI) where the app is the single writer and wants
/// to drive reactive UI from its own writes.
/// </para>
/// <para>
/// Changes performed inside <see cref="IDocumentStore.RunInTransaction"/> are buffered and only
/// emitted once the transaction commits; a rollback discards them.
/// </para>
/// </summary>
public interface IObservableDocumentStore
{
    /// <summary>
    /// Returns an async stream of <see cref="DocumentChange{T}"/> for every change to a document of
    /// type <typeparamref name="T"/> made through this store. Subscribers only receive changes that
    /// occur after the enumeration starts. Cancel the token (or break the <c>await foreach</c>) to
    /// unsubscribe.
    /// </summary>
    /// <typeparam name="T">The document type to observe.</typeparam>
    /// <param name="cancellationToken">Cancels the subscription.</param>
    IAsyncEnumerable<DocumentChange<T>> NotifyOnChange<T>(CancellationToken cancellationToken = default) where T : class;
}
