using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Channels;

namespace Shiny.DocumentDb.Internal;

/// <summary>Non-generic view of a <c>Subject&lt;T&gt;</c> so the late-bound JSON lane can publish by
/// <see cref="Type"/> without a compile-time <c>T</c>.</summary>
interface IChangeSubject
{
    bool HasSubscribers { get; }
    void EmitJson(DocumentChangeType changeType, string id, string? json, JsonSerializerOptions jsonOptions);
}

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

    /// <summary>
    /// Publishes a change for the late-bound JSON lane, keyed by <paramref name="type"/> instead of a
    /// compile-time <c>T</c>. No-op unless a subject exists for the type and has subscribers; only then is
    /// <paramref name="json"/> lazily deserialized to the subscriber's <c>T</c>. If the type has no resolvable
    /// <see cref="JsonTypeInfo"/> (reflection-disabled AOT), a JSON-only change with a null document is emitted.
    /// </summary>
    public void Publish(Type type, DocumentChangeType changeType, string id, string? json, JsonSerializerOptions jsonOptions)
    {
        if (this.subjects.TryGetValue(type, out var s) && s is IChangeSubject subject)
            subject.EmitJson(changeType, id, json, jsonOptions);
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

    sealed class Subject<T> : IChangeSubject where T : class
    {
        readonly object gate = new();
        ChannelWriter<DocumentChange<T>>[] writers = [];

        public bool HasSubscribers => Volatile.Read(ref this.writers).Length > 0;

        public void EmitJson(DocumentChangeType changeType, string id, string? json, JsonSerializerOptions jsonOptions)
        {
            if (!this.HasSubscribers)
                return;

            T? document = null;
            if (json != null)
            {
                try
                {
                    document = jsonOptions.GetTypeInfo(typeof(T)) is JsonTypeInfo<T> typed
                        ? JsonSerializer.Deserialize(json, typed)
                        : JsonSerializer.Deserialize<T>(json, jsonOptions);
                }
                catch
                {
                    // No resolvable typeInfo (reflection-disabled AOT) — publish a JSON-only change.
                    document = null;
                }
            }
            this.Emit(new DocumentChange<T> { ChangeType = changeType, Id = id, Document = document });
        }

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
