using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// The internal engine a <see cref="UnitOfWork"/> uses to apply its buffered operations inside a single
/// transaction. Every store that supports units of work implements this. It is deliberately NOT part of
/// the public <see cref="IDocumentStore"/> surface — the only public way to group writes is
/// <see cref="IDocumentStore.CreateUnitOfWork"/> + <see cref="UnitOfWork.SaveChanges"/>.
/// </summary>
interface IUnitOfWorkEngine
{
    /// <summary>
    /// Runs <paramref name="work"/> inside a single transaction, passing a transaction-bound store the
    /// unit dispatches each buffered operation against. Commits on success; rolls back (and rethrows) on
    /// failure.
    /// </summary>
    Task RunUnitAsync(Func<IDocumentStore, CancellationToken, Task> work, CancellationToken cancellationToken);
}

/// <summary>
/// Buffers document write operations and applies them atomically within a single transaction when
/// <see cref="SaveChanges"/> is called. Create one via <see cref="IDocumentStore.CreateUnitOfWork"/>.
/// Not thread-safe — use one unit per logical operation.
/// </summary>
/// <remarks>
/// A unit is a write buffer, not a tracking context: reads against the store do not see operations still
/// buffered in a unit until <see cref="SaveChanges"/> commits. Contiguous same-kind, same-type runs are
/// coalesced into the matching batch method (<see cref="IDocumentStore.BatchInsert"/>,
/// <see cref="IDocumentStore.BatchUpsert"/>, <see cref="IDocumentStore.BatchUpdate"/>,
/// <see cref="IDocumentStore.BatchRemove"/>), so grouping like operations in a unit costs nothing versus
/// calling those methods directly.
/// </remarks>
public sealed class UnitOfWork
{
    readonly IUnitOfWorkEngine engine;
    readonly List<UnitOfWorkOp> pending = new();

    internal UnitOfWork(IUnitOfWorkEngine engine) => this.engine = engine;

    /// <summary>Number of operations currently queued.</summary>
    public int PendingCount => this.pending.Count;

    /// <summary>Queues an Insert.</summary>
    public UnitOfWork Add<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        this.pending.Add(new InsertOp<T>(new[] { document }, jsonTypeInfo));
        return this;
    }

    /// <summary>Queues a batch Insert. Contiguous inserts of the same type are coalesced on commit.</summary>
    public UnitOfWork AddRange<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        var list = documents as IReadOnlyList<T> ?? documents.ToList();
        if (list.Count > 0)
            this.pending.Add(new InsertOp<T>(list, jsonTypeInfo));
        return this;
    }

    /// <summary>Queues a full-document Update. Document must have a non-default Id.</summary>
    public UnitOfWork Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        this.pending.Add(new UpdateOp<T>(document, jsonTypeInfo));
        return this;
    }

    /// <summary>Queues an Upsert (RFC 7396 JSON Merge Patch). Patch must have a non-default Id.</summary>
    public UnitOfWork Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        this.pending.Add(new UpsertOp<T>(patch, jsonTypeInfo));
        return this;
    }

    /// <summary>Queues a Remove by Id.</summary>
    public UnitOfWork Remove<T>(object id) where T : class
    {
        this.pending.Add(new RemoveOp<T>(id));
        return this;
    }

    /// <summary>Discards all queued operations without executing them.</summary>
    public void Clear() => this.pending.Clear();

    /// <summary>
    /// Applies all queued operations inside a single transaction, then clears the queue. Contiguous runs
    /// of same-type inserts are coalesced into one batch insert; updates/upserts/removes execute in
    /// buffer order. If the transaction fails it is rolled back and the queue is preserved so the caller
    /// can fix and retry.
    /// </summary>
    public async Task SaveChanges(CancellationToken cancellationToken = default)
    {
        if (this.pending.Count == 0)
            return;

        await this.engine.RunUnitAsync(async (tx, ct) =>
        {
            var i = 0;
            while (i < this.pending.Count)
            {
                var op = this.pending[i];
                // Coalesce the contiguous run of same-kind, same-type ops starting at i into the matching
                // batch method (BatchInsert / BatchUpsert / BatchUpdate / BatchRemove).
                var j = i + 1;
                while (j < this.pending.Count && this.pending[j].Kind == op.Kind && this.pending[j].DocumentType == op.DocumentType)
                    j++;
                if (j - i == 1)
                    await op.ExecuteAsync(tx, ct).ConfigureAwait(false);
                else
                    await op.FlushRunAsync(tx, this.pending.GetRange(i, j - i), ct).ConfigureAwait(false);
                i = j;
            }
        }, cancellationToken).ConfigureAwait(false);

        this.Clear();
    }

    /// <summary>Deprecated alias for <see cref="SaveChanges"/>.</summary>
    [Obsolete("Renamed to SaveChanges. This alias will be removed in a future version.")]
    public Task Commit(CancellationToken cancellationToken = default) => this.SaveChanges(cancellationToken);
}

// ── Buffered operation records ──────────────────────────────────────────────
// Typed and tagged with a Kind so SaveChanges can coalesce contiguous same-kind, same-type runs into the
// matching batch method while preserving the original ordering across kind/type boundaries.

enum UnitOfWorkOpKind { Insert, Update, Upsert, Remove }

abstract class UnitOfWorkOp
{
    public abstract Type DocumentType { get; }
    public abstract UnitOfWorkOpKind Kind { get; }

    /// <summary>Execute this op on its own against the transaction-bound store.</summary>
    public abstract Task ExecuteAsync(IDocumentStore tx, CancellationToken ct);

    /// <summary>
    /// Flush a contiguous run of same-kind, same-type ops (this op is <paramref name="run"/>[0]) as one
    /// batch call.
    /// </summary>
    public abstract Task FlushRunAsync(IDocumentStore tx, IReadOnlyList<UnitOfWorkOp> run, CancellationToken ct);
}

sealed class InsertOp<T> : UnitOfWorkOp where T : class
{
    readonly IReadOnlyList<T> documents;
    readonly JsonTypeInfo<T>? typeInfo;

    public InsertOp(IReadOnlyList<T> documents, JsonTypeInfo<T>? typeInfo)
    {
        this.documents = documents;
        this.typeInfo = typeInfo;
    }

    public override Type DocumentType => typeof(T);
    public override UnitOfWorkOpKind Kind => UnitOfWorkOpKind.Insert;

    public override Task ExecuteAsync(IDocumentStore tx, CancellationToken ct)
        => this.FlushRunAsync(tx, new UnitOfWorkOp[] { this }, ct);

    public override async Task FlushRunAsync(IDocumentStore tx, IReadOnlyList<UnitOfWorkOp> run, CancellationToken ct)
    {
        var all = new List<T>();
        JsonTypeInfo<T>? ti = null;
        foreach (var op in run)
        {
            var insert = (InsertOp<T>)op;
            all.AddRange(insert.documents);
            ti ??= insert.typeInfo;
        }

        if (all.Count == 1)
            await tx.Insert(all[0], ti, ct).ConfigureAwait(false);
        else
            await tx.BatchInsert(all, ti, ct).ConfigureAwait(false);
    }
}

sealed class UpdateOp<T> : UnitOfWorkOp where T : class
{
    readonly T document;
    readonly JsonTypeInfo<T>? typeInfo;

    public UpdateOp(T document, JsonTypeInfo<T>? typeInfo)
    {
        this.document = document;
        this.typeInfo = typeInfo;
    }

    public override Type DocumentType => typeof(T);
    public override UnitOfWorkOpKind Kind => UnitOfWorkOpKind.Update;
    public override Task ExecuteAsync(IDocumentStore tx, CancellationToken ct) => tx.Update(this.document, this.typeInfo, ct);

    public override async Task FlushRunAsync(IDocumentStore tx, IReadOnlyList<UnitOfWorkOp> run, CancellationToken ct)
    {
        var all = new List<T>(run.Count);
        JsonTypeInfo<T>? ti = null;
        foreach (var op in run)
        {
            var update = (UpdateOp<T>)op;
            all.Add(update.document);
            ti ??= update.typeInfo;
        }
        await tx.BatchUpdate(all, ti, ct).ConfigureAwait(false);
    }
}

sealed class UpsertOp<T> : UnitOfWorkOp where T : class
{
    readonly T patch;
    readonly JsonTypeInfo<T>? typeInfo;

    public UpsertOp(T patch, JsonTypeInfo<T>? typeInfo)
    {
        this.patch = patch;
        this.typeInfo = typeInfo;
    }

    public override Type DocumentType => typeof(T);
    public override UnitOfWorkOpKind Kind => UnitOfWorkOpKind.Upsert;
    public override Task ExecuteAsync(IDocumentStore tx, CancellationToken ct) => tx.Upsert(this.patch, this.typeInfo, ct);

    public override async Task FlushRunAsync(IDocumentStore tx, IReadOnlyList<UnitOfWorkOp> run, CancellationToken ct)
    {
        var all = new List<T>(run.Count);
        JsonTypeInfo<T>? ti = null;
        foreach (var op in run)
        {
            var upsert = (UpsertOp<T>)op;
            all.Add(upsert.patch);
            ti ??= upsert.typeInfo;
        }
        await tx.BatchUpsert(all, ti, ct).ConfigureAwait(false);
    }
}

sealed class RemoveOp<T> : UnitOfWorkOp where T : class
{
    readonly object id;

    public RemoveOp(object id) => this.id = id;

    public override Type DocumentType => typeof(T);
    public override UnitOfWorkOpKind Kind => UnitOfWorkOpKind.Remove;
    public override Task ExecuteAsync(IDocumentStore tx, CancellationToken ct) => tx.Remove<T>(this.id, ct);

    public override async Task FlushRunAsync(IDocumentStore tx, IReadOnlyList<UnitOfWorkOp> run, CancellationToken ct)
    {
        var ids = new List<object>(run.Count);
        foreach (var op in run)
            ids.Add(((RemoveOp<T>)op).id);
        await tx.BatchRemove<T>(ids, ct).ConfigureAwait(false);
    }
}
