using System.Collections.Concurrent;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Per-store fan-out of <see cref="DocumentChange{T}"/> notifications, keyed by document type.
/// A subject (and its observer list) is created lazily the first time a type is observed, so
/// stores incur only a dictionary lookup on the write path when nothing is being watched.
/// </summary>
public sealed class ChangeBroadcaster
{
    readonly ConcurrentDictionary<Type, object> subjects = new();

    /// <summary>Returns the observable stream for <typeparamref name="T"/>, creating it if needed.</summary>
    public IObservable<DocumentChange<T>> Observe<T>() where T : class
        => (ChangeSubject<T>)this.subjects.GetOrAdd(typeof(T), static _ => new ChangeSubject<T>());

    /// <summary>True if at least one observer is currently subscribed for <typeparamref name="T"/>.</summary>
    public bool HasObservers<T>() where T : class
        => this.subjects.TryGetValue(typeof(T), out var s) && ((ChangeSubject<T>)s).HasObservers;

    /// <summary>Delivers a change to all current observers of <typeparamref name="T"/> (no-op if none).</summary>
    public void Publish<T>(DocumentChange<T> change) where T : class
    {
        if (this.subjects.TryGetValue(typeof(T), out var s))
            ((ChangeSubject<T>)s).Emit(change);
    }

    sealed class ChangeSubject<T> : IObservable<DocumentChange<T>> where T : class
    {
        readonly object gate = new();
        IObserver<DocumentChange<T>>[] observers = [];

        public bool HasObservers => Volatile.Read(ref this.observers).Length > 0;

        public IDisposable Subscribe(IObserver<DocumentChange<T>> observer)
        {
            ArgumentNullException.ThrowIfNull(observer);
            lock (this.gate)
                this.observers = [.. this.observers, observer];

            return new Subscription(this, observer);
        }

        public void Emit(DocumentChange<T> change)
        {
            // Snapshot under volatile read so emission never blocks on (un)subscribe.
            foreach (var observer in Volatile.Read(ref this.observers))
                observer.OnNext(change);
        }

        void Remove(IObserver<DocumentChange<T>> observer)
        {
            lock (this.gate)
                this.observers = this.observers.Where(o => !ReferenceEquals(o, observer)).ToArray();
        }

        sealed class Subscription(ChangeSubject<T> subject, IObserver<DocumentChange<T>> observer) : IDisposable
        {
            IObserver<DocumentChange<T>>? observer = observer;

            public void Dispose()
            {
                var o = Interlocked.Exchange(ref this.observer, null);
                if (o != null)
                    subject.Remove(o);
            }
        }
    }
}
