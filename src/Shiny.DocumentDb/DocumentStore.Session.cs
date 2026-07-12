using System.Data.Common;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

// Store-as-connection (§4f): the explicit-transaction seam. Reuses the private connection-acquisition story
// (shared connection under the semaphore, or a fresh pooled connection) and the transaction-bound
// TransactionalDocumentStore, but hands the transaction's lifecycle to the caller instead of committing inline
// like RunUnitImpl. A partial file so it can reach DocumentStore's private members without touching the main file.
// Only the relational store implements IExplicitTransactionEngine; document-native providers throw NotSupported.
public partial class DocumentStore : Internal.IExplicitTransactionEngine, Diagnostics.IUnitScopeSource
{
    System.Diagnostics.Activity? Diagnostics.IUnitScopeSource.StartUnitActivity(string operation)
        => Diagnostics.DocumentStoreMetrics.StartActivity(this.tracker.System, operation, "(unit)", this.options.StoreName);

    void Diagnostics.IUnitScopeSource.RecordUnitSize(long operations)
        => Diagnostics.DocumentStoreMetrics.RecordUnitSize(this.tracker.System, operations, this.options.StoreName);

    /// <summary>
    /// Opens an explicit, caller-controlled transaction. In shared mode the store's single connection is taken
    /// under the semaphore (serializing the whole store until the unit completes); in pooled mode a fresh
    /// connection is opened. Table DDL is committed BEFORE the transaction so a rollback cannot drop a table the
    /// process-wide init cache still believes exists.
    /// </summary>
    async Task<ExplicitUnit> Internal.IExplicitTransactionEngine.BeginExplicitUnitAsync(System.Data.IsolationLevel isolationLevel, CancellationToken ct)
    {
        async ValueTask<System.Data.Common.DbTransaction> Begin(System.Data.Common.DbConnection c)
            => isolationLevel == System.Data.IsolationLevel.Unspecified
                ? await c.BeginTransactionAsync(ct).ConfigureAwait(false)
                : await c.BeginTransactionAsync(isolationLevel, ct).ConfigureAwait(false);

        if (this.sharedMode)
        {
            await this.sharedSemaphore!.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await this.EnsureSharedConnectionInitializedAsync(ct).ConfigureAwait(false);
                await this.EnsureTableInitializedAsync(new DocumentStoreSession(this.sharedConnection!), this.options.TableName, ct).ConfigureAwait(false);
                var tx = await Begin(this.sharedConnection!).ConfigureAwait(false);
                return this.CreateExplicitUnit(this.sharedConnection!, tx, ownsConnection: false,
                    release: () => { this.sharedSemaphore.Release(); return ValueTask.CompletedTask; });
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
            await this.EnsureTableInitializedAsync(new DocumentStoreSession(conn), this.options.TableName, ct).ConfigureAwait(false);
            var tx = await Begin(conn).ConfigureAwait(false);
            return this.CreateExplicitUnit(conn, tx, ownsConnection: true, release: () => ValueTask.CompletedTask);
        }
        catch
        {
            await conn.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    ExplicitUnit CreateExplicitUnit(DbConnection conn, DbTransaction tx, bool ownsConnection, Func<ValueTask> release)
    {
        var pending = new List<Action>();
        var store = new TransactionalDocumentStore(
            conn, tx, this.options, this.provider, this.jsonOptions, this.logging, this.idCache,
            this.tableInitTasks, this.broadcaster, pending, this);
        return new ExplicitUnit(store, tx, conn, ownsConnection, release, pending);
    }
}
