using System.Runtime.CompilerServices;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Channels;
using Google.Cloud.Firestore;

namespace Shiny.DocumentDb.Firestore;

// Native change notifications backed by Firestore snapshot listeners. A listener's first snapshot is the
// current result set (every existing doc as "Added") — we skip it so subscribers see only changes that occur
// after they start observing, matching the IObservableDocumentStore / IChangeFeedDocumentStore contract.
public partial class FirestoreDocumentStore
{
    /// <inheritdoc />
    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange<T>(CancellationToken cancellationToken = default) where T : class
        => this.ListenChanges<T>(null, this.FindTypeInfo<T>(null), cancellationToken);

    /// <inheritdoc />
    /// <remarks>Backed by a Firestore query snapshot listener over the type's collection. Delivers inserts,
    /// updates, and deletes made by any writer, including other processes.</remarks>
    public Task<IAsyncDisposable> SubscribeChanges<T>(
        Func<DocumentChange<T>, CancellationToken, Task> onChange,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(onChange);
        var typeInfo = this.FindTypeInfo<T>(null);
        var query = (Query)this.GetCollection<T>();
        var first = true;

        this.Log($"Firestore LISTEN {this.ResolveCollectionName<T>()}");
        var listener = query.Listen(async (snapshot, token) =>
        {
            if (first)
            {
                first = false;
                return;
            }
            foreach (var change in snapshot.Changes)
            {
                var mapped = this.MapChange(change, typeInfo);
                if (mapped != null)
                    await onChange(mapped, token).ConfigureAwait(false);
            }
        }, cancellationToken);

        return Task.FromResult<IAsyncDisposable>(new ListenerHandle(listener));
    }

    internal async IAsyncEnumerable<DocumentChange<T>> ListenChanges<T>(
        Func<T, bool>? predicate,
        JsonTypeInfo<T>? typeInfo,
        [EnumeratorCancellation] CancellationToken cancellationToken) where T : class
    {
        var channel = Channel.CreateUnbounded<DocumentChange<T>>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });
        var first = true;
        var query = (Query)this.GetCollection<T>();

        this.Log($"Firestore LISTEN {this.ResolveCollectionName<T>()}");
        var listener = query.Listen(snapshot =>
        {
            if (first)
            {
                first = false;
                return;
            }
            foreach (var change in snapshot.Changes)
            {
                var mapped = this.MapChange(change, typeInfo);
                if (mapped == null)
                    continue;
                // Removals (null document) pass through unfiltered so the consumer can re-query.
                if (predicate != null && mapped.Document != null && !predicate(mapped.Document))
                    continue;
                channel.Writer.TryWrite(mapped);
            }
        }, cancellationToken);

        try
        {
            await foreach (var change in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return change;
        }
        finally
        {
            await listener.StopAsync().ConfigureAwait(false);
            channel.Writer.TryComplete();
        }
    }

    DocumentChange<T>? MapChange<T>(Google.Cloud.Firestore.DocumentChange change, JsonTypeInfo<T>? typeInfo) where T : class
    {
        var id = change.Document.Id;
        if (change.ChangeType == Google.Cloud.Firestore.DocumentChange.Type.Removed)
            return new DocumentChange<T> { ChangeType = DocumentChangeType.Removed, Id = id, Document = null };

        var changeType = change.ChangeType == Google.Cloud.Firestore.DocumentChange.Type.Added
            ? DocumentChangeType.Inserted
            : DocumentChangeType.Updated;

        var document = change.Document.Exists ? this.DeserializeSnapshot(change.Document, typeInfo) : null;
        return new DocumentChange<T> { ChangeType = changeType, Id = id, Document = document };
    }

    sealed class ListenerHandle(FirestoreChangeListener listener) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync() => await listener.StopAsync().ConfigureAwait(false);
    }
}
