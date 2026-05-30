using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// Buffers document operations and applies them atomically within a single transaction when <see cref="Commit"/> is called.
/// Construct via <see cref="DocumentStoreExtensions.CreateUnitOfWork(IDocumentStore)"/>. Not thread-safe.
/// </summary>
public sealed class UnitOfWork
{
    readonly IDocumentStore store;
    readonly List<Func<IDocumentStore, CancellationToken, Task>> pending = new();

    internal UnitOfWork(IDocumentStore store) => this.store = store;

    /// <summary>Number of operations currently queued.</summary>
    public int PendingCount => this.pending.Count;

    /// <summary>Queues an Insert.</summary>
    public UnitOfWork Add<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        this.pending.Add((s, ct) => s.Insert(document, jsonTypeInfo, ct));
        return this;
    }

    /// <summary>Queues a full-document Update. Document must have a non-default Id.</summary>
    public UnitOfWork Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        this.pending.Add((s, ct) => s.Update(document, jsonTypeInfo, ct));
        return this;
    }

    /// <summary>Queues a Remove by Id.</summary>
    public UnitOfWork Remove<T>(object id) where T : class
    {
        this.pending.Add((s, ct) => s.Remove<T>(id, ct));
        return this;
    }

    /// <summary>Discards all queued operations without executing them.</summary>
    public void Clear() => this.pending.Clear();

    /// <summary>
    /// Applies all queued operations inside a single transaction, then clears the queue.
    /// If the transaction fails it is rolled back; the queue is preserved so the caller can fix and retry.
    /// </summary>
    public async Task Commit(CancellationToken cancellationToken = default)
    {
        if (this.pending.Count == 0)
            return;

        await this.store.RunInTransaction(async tx =>
        {
            foreach (var op in this.pending)
                await op(tx, cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);

        this.Clear();
    }
}

public static class DocumentStoreExtensions
{
    /// <summary>
    /// Creates a <see cref="UnitOfWork"/> that buffers Add/Update/Remove operations and applies them
    /// atomically on <see cref="UnitOfWork.Commit"/>.
    /// </summary>
    public static UnitOfWork CreateUnitOfWork(this IDocumentStore store) => new(store);
}
