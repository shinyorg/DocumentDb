namespace Shiny.DocumentDb;

/// <summary>
/// An optional capability implemented by stores that can deliver <b>native</b>, server-side change
/// notifications — including changes made by other processes, connections, or writers (not just
/// this store instance). This is backed by the database's own change mechanism:
/// PostgreSQL <c>LISTEN</c>/<c>NOTIFY</c>, SQL Server Change Tracking / Query Notifications, and
/// Azure Cosmos DB Change Feed.
/// <para>
/// Unlike <see cref="IObservableDocumentStore"/> (in-process notifications for this instance's own
/// writes), a change feed observes the underlying data itself and therefore requires server-side
/// provisioning (triggers, change tracking, a lease/continuation) and a background connection.
/// </para>
/// </summary>
public interface IChangeFeedDocumentStore
{
    /// <summary>
    /// Begins a native change-feed subscription for documents of type <typeparamref name="T"/>.
    /// The <paramref name="onChange"/> handler is invoked sequentially for each change as it
    /// arrives. Dispose the returned handle (<c>await using</c>) to stop the subscription and
    /// release its resources.
    /// </summary>
    /// <param name="onChange">Async handler invoked for each change. Receives a cancellation token tied to the subscription lifetime.</param>
    /// <param name="cancellationToken">Cancels subscription setup; the subscription itself is stopped by disposing the returned handle.</param>
    Task<IAsyncDisposable> SubscribeChanges<T>(
        Func<DocumentChange<T>, CancellationToken, Task> onChange,
        CancellationToken cancellationToken = default) where T : class;
}
