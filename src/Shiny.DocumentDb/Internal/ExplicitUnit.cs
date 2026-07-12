using System.Data.Common;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// A caller-controlled explicit transaction produced by <see cref="IExplicitTransactionEngine"/>. Its
/// <see cref="Store"/> is a transaction-bound <see cref="IDocumentStore"/> — every op runs on the pinned
/// connection + transaction. Change notifications are buffered and emitted on <see cref="CommitAsync"/> only.
/// Provider-agnostic: the store hands in a <paramref name="release"/> delegate (e.g. release the shared-connection
/// semaphore) so this type never reaches back into store internals.
/// </summary>
sealed class ExplicitUnit : IAsyncDisposable, IDisposable
{
    readonly DbTransaction tx;
    readonly DbConnection conn;
    readonly bool ownsConnection;
    readonly Func<ValueTask> release;
    readonly List<Action> pendingChanges;
    bool completed;

    public ExplicitUnit(
        IDocumentStore store,
        DbTransaction tx,
        DbConnection conn,
        bool ownsConnection,
        Func<ValueTask> release,
        List<Action> pendingChanges)
    {
        this.Store = store;
        this.tx = tx;
        this.conn = conn;
        this.ownsConnection = ownsConnection;
        this.release = release;
        this.pendingChanges = pendingChanges;
    }

    /// <summary>The transaction-bound store — every operation runs on the pinned connection.</summary>
    public IDocumentStore Store { get; }

    public async Task CommitAsync(CancellationToken ct)
    {
        if (this.completed) return;
        await this.tx.CommitAsync(ct).ConfigureAwait(false);
        this.completed = true;
        foreach (var emit in this.pendingChanges)
            emit();
        await this.ReleaseAsync().ConfigureAwait(false);
    }

    public async Task RollbackAsync(CancellationToken ct)
    {
        if (this.completed) return;
        await this.tx.RollbackAsync(ct).ConfigureAwait(false);
        this.completed = true;
        this.pendingChanges.Clear();
        await this.ReleaseAsync().ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (this.completed) return;
        try { await this.tx.RollbackAsync().ConfigureAwait(false); } catch { /* best-effort */ }
        this.completed = true;
        this.pendingChanges.Clear();
        await this.ReleaseAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (this.completed) return;
        try { this.tx.Rollback(); } catch { /* best-effort */ }
        this.completed = true;
        this.pendingChanges.Clear();
        this.tx.Dispose();
        if (this.ownsConnection)
            this.conn.Dispose();
        // release completes synchronously (semaphore Release / no-op); safe to drain without blocking.
        this.release().AsTask().GetAwaiter().GetResult();
    }

    async ValueTask ReleaseAsync()
    {
        await this.tx.DisposeAsync().ConfigureAwait(false);
        if (this.ownsConnection)
            await this.conn.DisposeAsync().ConfigureAwait(false);
        await this.release().ConfigureAwait(false);
    }
}
