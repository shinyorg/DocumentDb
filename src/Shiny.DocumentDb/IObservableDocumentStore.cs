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
    /// Returns a hot observable that emits a <see cref="DocumentChange{T}"/> each time a document
    /// of type <typeparamref name="T"/> is changed through this store. Subscribers only receive
    /// changes that occur after they subscribe.
    /// </summary>
    /// <typeparam name="T">The document type to observe.</typeparam>
    IObservable<DocumentChange<T>> WhenChanged<T>() where T : class;
}
