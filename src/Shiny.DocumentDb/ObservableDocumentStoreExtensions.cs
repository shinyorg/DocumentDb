namespace Shiny.DocumentDb;

/// <summary>
/// Convenience helpers for consuming <see cref="IObservableDocumentStore"/> change streams without
/// taking a dependency on System.Reactive.
/// </summary>
public static class ObservableDocumentStoreExtensions
{
    /// <summary>
    /// Observes changes for documents of type <typeparamref name="T"/>. Throws
    /// <see cref="NotSupportedException"/> if the underlying store does not implement
    /// <see cref="IObservableDocumentStore"/>.
    /// </summary>
    public static IObservable<DocumentChange<T>> WhenChanged<T>(this IDocumentStore store) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        if (store is IObservableDocumentStore observable)
            return observable.WhenChanged<T>();

        throw new NotSupportedException(
            $"'{store.GetType().Name}' does not support change observation (IObservableDocumentStore).");
    }

    /// <summary>Observes changes for a single document, filtered by its string Id.</summary>
    public static IObservable<DocumentChange<T>> WhenDocumentChanged<T>(this IObservableDocumentStore store, string id) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(id);
        return store
            .WhenChanged<T>()
            .Where(change => change.ChangeType == DocumentChangeType.Cleared || change.Id == id);
    }

    /// <summary>Filters an observable change stream by an arbitrary predicate.</summary>
    public static IObservable<TChange> Where<TChange>(this IObservable<TChange> source, Func<TChange, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(predicate);
        return new WhereObservable<TChange>(source, predicate);
    }

    /// <summary>Subscribes to an observable with a simple callback, returning the unsubscribe handle.</summary>
    public static IDisposable Subscribe<TChange>(this IObservable<TChange> source, Action<TChange> onNext)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(onNext);
        return source.Subscribe(new ActionObserver<TChange>(onNext));
    }

    sealed class WhereObservable<TChange>(IObservable<TChange> source, Func<TChange, bool> predicate) : IObservable<TChange>
    {
        public IDisposable Subscribe(IObserver<TChange> observer)
            => source.Subscribe(new ActionObserver<TChange>(
                value => { if (predicate(value)) observer.OnNext(value); },
                observer.OnError,
                observer.OnCompleted));
    }

    sealed class ActionObserver<TChange>(Action<TChange> onNext, Action<Exception>? onError = null, Action? onCompleted = null) : IObserver<TChange>
    {
        public void OnNext(TChange value) => onNext(value);
        public void OnError(Exception error) => onError?.Invoke(error);
        public void OnCompleted() => onCompleted?.Invoke();
    }
}
