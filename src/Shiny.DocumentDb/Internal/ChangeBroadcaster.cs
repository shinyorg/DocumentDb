using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Per-store fan-out of <see cref="DocumentChange{T}"/> notifications, keyed by document type.
/// A subject (and its writer list) is created lazily the first time a type is observed, so stores
/// incur only a dictionary lookup on the write path when nothing is being watched.
/// </summary>
/// <remarks>
/// Each subscriber receives its own unbounded <see cref="Channel{T}"/>; <see cref="Publish{T}"/>
/// writes to every active channel non-blocking. The channel is unregistered and completed when
/// the consumer's <c>await foreach</c> exits or its <see cref="CancellationToken"/> fires.
/// </remarks>
public sealed class ChangeBroadcaster
{
    readonly ConcurrentDictionary<Type, object> subjects = new();

    /// <summary>Returns an async stream of changes for <typeparamref name="T"/>.</summary>
    public IAsyncEnumerable<DocumentChange<T>> Observe<T>(CancellationToken cancellationToken) where T : class
    {
        var subject = (Subject<T>)this.subjects.GetOrAdd(typeof(T), static _ => new Subject<T>());
        return Iterate(subject, cancellationToken);
    }

    /// <summary>True if at least one subscriber is currently consuming changes for <typeparamref name="T"/>.</summary>
    public bool HasSubscribers<T>() where T : class
        => this.subjects.TryGetValue(typeof(T), out var s) && ((Subject<T>)s).HasSubscribers;

    /// <summary>Delivers a change to all current subscribers of <typeparamref name="T"/> (no-op if none).</summary>
    public void Publish<T>(DocumentChange<T> change) where T : class
    {
        if (this.subjects.TryGetValue(typeof(T), out var s))
            ((Subject<T>)s).Emit(change);
    }

    static async IAsyncEnumerable<DocumentChange<T>> Iterate<T>(
        Subject<T> subject,
        [EnumeratorCancellation] CancellationToken ct) where T : class
    {
        var reader = subject.Subscribe(out var unregister);
        try
        {
            await foreach (var change in reader.ReadAllAsync(ct).ConfigureAwait(false))
                yield return change;
        }
        finally
        {
            unregister();
        }
    }

    sealed class Subject<T> where T : class
    {
        readonly object gate = new();
        ChannelWriter<DocumentChange<T>>[] writers = [];

        public bool HasSubscribers => Volatile.Read(ref this.writers).Length > 0;

        public ChannelReader<DocumentChange<T>> Subscribe(out Action unregister)
        {
            var channel = Channel.CreateUnbounded<DocumentChange<T>>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
            var writer = channel.Writer;
            lock (this.gate)
                this.writers = [.. this.writers, writer];

            unregister = () =>
            {
                lock (this.gate)
                    this.writers = this.writers.Where(w => !ReferenceEquals(w, writer)).ToArray();
                writer.TryComplete();
            };
            return channel.Reader;
        }

        public void Emit(DocumentChange<T> change)
        {
            foreach (var writer in Volatile.Read(ref this.writers))
                writer.TryWrite(change);
        }
    }
}
