using System.Data.Common;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

// Store-as-connection spike (§4f): the explicit-transaction seam. Reuses the private connection-acquisition
// story (shared connection under the semaphore, or a fresh pooled connection) and the transaction-bound
// TransactionalDocumentStore, but hands the transaction's lifecycle to the caller instead of committing inline
// like RunUnitImpl. A partial file so it can reach DocumentStore's private members without touching the main file.
public partial class DocumentStore
{
    /// <summary>
    /// Opens an explicit, caller-controlled transaction. In shared mode the store's single connection is taken
    /// under the semaphore (serializing the whole store until the unit completes); in pooled mode a fresh
    /// connection is opened. Every op on the returned <see cref="ExplicitUnit.Store"/> runs on that pinned
    /// connection + transaction. The caller MUST Commit/Rollback/Dispose the returned unit.
    /// </summary>
    internal async Task<ExplicitUnit> BeginExplicitUnitAsync(CancellationToken ct)
    {
        if (this.sharedMode)
        {
            await this.sharedSemaphore!.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await this.EnsureSharedConnectionInitializedAsync(ct).ConfigureAwait(false);
                // Create the table BEFORE the transaction so its DDL is committed (autocommit) and survives a
                // rollback — otherwise a rollback drops a table the process-wide init cache still believes exists.
                await this.EnsureTableInitializedAsync(new DocumentStoreSession(this.sharedConnection!), this.options.TableName, ct).ConfigureAwait(false);
                var tx = await this.sharedConnection!.BeginTransactionAsync(ct).ConfigureAwait(false);
                return this.CreateExplicitUnit(this.sharedConnection!, tx, releaseSemaphore: true, ownsConnection: false);
            }
            catch
            {
                this.sharedSemaphore.Release();
                throw;
            }
        }

        var conn = this.provider.CreateConnection();
        try
        {
            await conn.OpenAsync(ct).ConfigureAwait(false);
            await this.provider.InitializeConnectionAsync(conn, ct).ConfigureAwait(false);
            if (this.provider.SupportsVector && this.options.vectorMappings.Count > 0)
                await this.provider.LoadVectorExtensionAsync(conn, ct).ConfigureAwait(false);
            // Create the table before the transaction so its DDL is committed and survives a rollback (see shared-mode note).
            await this.EnsureTableInitializedAsync(new DocumentStoreSession(conn), this.options.TableName, ct).ConfigureAwait(false);
            var tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);
            return this.CreateExplicitUnit(conn, tx, releaseSemaphore: false, ownsConnection: true);
        }
        catch
        {
            await conn.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    ExplicitUnit CreateExplicitUnit(DbConnection conn, DbTransaction tx, bool releaseSemaphore, bool ownsConnection)
    {
        var pending = new List<Action>();
        var store = new TransactionalDocumentStore(
            conn, tx, this.options, this.provider, this.jsonOptions, this.logging, this.idCache,
            this.tableInitTasks, this.broadcaster, pending, this);
        return new ExplicitUnit(this, store, tx, conn, pending, releaseSemaphore, ownsConnection);
    }

    /// <summary>Caller-controlled transaction handle. Its <see cref="Store"/> runs every op on the pinned
    /// connection + transaction. Change notifications are buffered and emitted on Commit only.</summary>
    internal sealed class ExplicitUnit : IAsyncDisposable
    {
        readonly DocumentStore parent;
        readonly DbTransaction tx;
        readonly DbConnection conn;
        readonly List<Action> pendingChanges;
        readonly bool releaseSemaphore;
        readonly bool ownsConnection;
        bool completed;

        internal ExplicitUnit(
            DocumentStore parent, IDocumentStore store, DbTransaction tx, DbConnection conn,
            List<Action> pendingChanges, bool releaseSemaphore, bool ownsConnection)
        {
            this.parent = parent;
            this.Store = store;
            this.tx = tx;
            this.conn = conn;
            this.pendingChanges = pendingChanges;
            this.releaseSemaphore = releaseSemaphore;
            this.ownsConnection = ownsConnection;
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

        async Task ReleaseAsync()
        {
            await this.tx.DisposeAsync().ConfigureAwait(false);
            if (this.ownsConnection)
                await this.conn.DisposeAsync().ConfigureAwait(false);
            if (this.releaseSemaphore)
                this.parent.sharedSemaphore!.Release();
        }
    }
}
