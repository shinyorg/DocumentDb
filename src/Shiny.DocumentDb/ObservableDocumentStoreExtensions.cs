using System.Runtime.CompilerServices;

namespace Shiny.DocumentDb;

/// <summary>
/// Convenience helpers for consuming change streams from <see cref="IObservableDocumentStore"/>
/// and <see cref="IChangeFeedDocumentStore"/>.
/// </summary>
public static class ObservableDocumentStoreExtensions
{
    /// <summary>
    /// Observes changes for documents of type <typeparamref name="T"/>. Throws
    /// <see cref="NotSupportedException"/> if the underlying store does not implement
    /// <see cref="IObservableDocumentStore"/>.
    /// </summary>
    public static IAsyncEnumerable<DocumentChange<T>> NotifyOnChange<T>(this IDocumentStore store, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        if (store is IObservableDocumentStore observable)
            return observable.NotifyOnChange<T>(cancellationToken);

        throw new NotSupportedException(
            $"'{store.GetType().Name}' does not support change observation (IObservableDocumentStore).");
    }

    /// <summary>
    /// Begins a native change-feed subscription (observing all writers). Throws
    /// <see cref="NotSupportedException"/> if the store does not implement
    /// <see cref="IChangeFeedDocumentStore"/>.
    /// </summary>
    public static Task<IAsyncDisposable> SubscribeChanges<T>(this IDocumentStore store, Func<DocumentChange<T>, CancellationToken, Task> onChange, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        if (store is IChangeFeedDocumentStore feed)
            return feed.SubscribeChanges(onChange, cancellationToken);

        throw new NotSupportedException(
            $"'{store.GetType().Name}' does not support native change feeds (IChangeFeedDocumentStore).");
    }

    /// <summary>Begins a native change-feed subscription with a simple async handler.</summary>
    public static Task<IAsyncDisposable> SubscribeChanges<T>(this IChangeFeedDocumentStore store, Func<DocumentChange<T>, Task> onChange, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(onChange);
        return store.SubscribeChanges<T>((change, _) => onChange(change), cancellationToken);
    }

    /// <summary>Observes changes for a single document, filtered by its string Id.</summary>
    public static async IAsyncEnumerable<DocumentChange<T>> WhenDocumentChanged<T>(this IObservableDocumentStore store, string id, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(id);

        await foreach (var change in store.NotifyOnChange<T>(cancellationToken).ConfigureAwait(false))
        {
            if (change.ChangeType == DocumentChangeType.Cleared || change.Id == id)
                yield return change;
        }
    }
}
