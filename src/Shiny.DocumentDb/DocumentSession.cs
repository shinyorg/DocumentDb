using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb;

/// <summary>
/// Default <see cref="IDocumentSession"/> — a unit of work over a root <see cref="DocumentStore"/>. Buffers writes
/// (reusing <see cref="UnitOfWork"/>'s buffer + coalescing), flushes them on <see cref="SaveChanges"/>, and pins a
/// connection for an explicit <see cref="BeginTransaction"/>. Not thread-safe (§4e/§4f).
/// </summary>
public sealed class DocumentSession : IDocumentSession
{
    readonly DocumentStore store;
    readonly UnitOfWork buffer;
    readonly IServiceScope? ownedScope;   // disposed only when the session created it (factory path)

    DocumentStore.ExplicitUnit? unit;
    DocumentTransaction? currentTx;

    internal DocumentSession(DocumentStore store, IServiceProvider services, IServiceScope? ownedScope)
    {
        this.store = store;
        this.Services = services;
        this.ownedScope = ownedScope;
        this.buffer = new UnitOfWork((IUnitOfWorkEngine)store);
    }

    public IServiceProvider Services { get; }
    public IDocumentStore Store => this.store;
    public IDocumentTransaction? CurrentTransaction => this.currentTx;
    public int PendingCount => this.buffer.PendingCount;
    public void ClearPending() => this.buffer.Clear();

    // The store every operation targets: the transaction-bound store while a tx is open, else the root.
    IDocumentStore Target => this.unit?.Store ?? (IDocumentStore)this.store;

    // ── Buffered writes ────────────────────────────────────────────────────
    public IDocumentSession Add<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        this.buffer.Add(document, jsonTypeInfo);
        return this;
    }

    public IDocumentSession AddRange<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        this.buffer.AddRange(documents, jsonTypeInfo);
        return this;
    }

    public IDocumentSession Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        this.buffer.Update(document, jsonTypeInfo);
        return this;
    }

    public IDocumentSession Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        this.buffer.Upsert(patch, jsonTypeInfo);
        return this;
    }

    public IDocumentSession Remove<T>(object id) where T : class
    {
        this.buffer.Remove<T>(id);
        return this;
    }

    // ── SaveChanges ────────────────────────────────────────────────────────
    public Task SaveChanges(CancellationToken cancellationToken = default)
        => this.SaveChanges(false, cancellationToken);

    public async Task SaveChanges(bool suppressInterceptors, CancellationToken cancellationToken = default)
    {
        if (this.buffer.PendingCount == 0)
            return;

        // Flow the session's DI scope into the write pipeline (replaces the AsyncLocal carrier; interceptors read it).
        using var flowScope = DocumentOperationScope.EnterServices(this.Services);

        if (this.unit != null)
        {
            // Join the active transaction: flush into it, do NOT commit (the tx owner commits).
            using var suppression = suppressInterceptors ? DocumentOperationScope.SuppressInterceptors() : null;
            await this.buffer.FlushInto(this.unit.Store, cancellationToken).ConfigureAwait(false);
            this.buffer.Clear();
        }
        else
        {
            // No active transaction: the buffer opens its own transaction, flushes, commits, and clears.
            await this.buffer.SaveChanges(suppressInterceptors, cancellationToken).ConfigureAwait(false);
        }
    }

    // ── Explicit transaction ───────────────────────────────────────────────
    public async Task<IDocumentTransaction> BeginTransaction(CancellationToken cancellationToken = default)
    {
        if (this.currentTx != null)
            throw new InvalidOperationException(
                "A transaction is already active on this session. Only one transaction may be active at a time.");

        this.unit = await this.store.BeginExplicitUnitAsync(cancellationToken).ConfigureAwait(false);
        this.currentTx = new DocumentTransaction(this, this.unit);
        return this.currentTx;
    }

    // Called by DocumentTransaction on Commit/Rollback/Dispose.
    internal void OnTransactionClosed()
    {
        this.currentTx = null;
        this.unit = null;
    }

    // ── Immediate reads ────────────────────────────────────────────────────
    public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => this.Target.Get(id, jsonTypeInfo, cancellationToken);

    public Task<T?> Get<T>(object id, LockMode lockMode, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        // SPIKE: the lock is enforced by the active transaction (SQLite takes a whole-DB write lock). A locking
        // read outside a transaction is meaningless, so require one.
        if (lockMode != LockMode.None && this.unit == null)
            throw new InvalidOperationException("A locking read (LockMode != None) requires an active transaction — call BeginTransaction first.");
        return this.Target.Get(id, jsonTypeInfo, cancellationToken);
    }

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
        => this.Target.Query(jsonTypeInfo);

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.Target.Count<T>(whereClause, parameters, cancellationToken);

    public IDisposable SuppressInterceptors() => DocumentOperationScope.SuppressInterceptors();

    // ── Disposal ───────────────────────────────────────────────────────────
    public async ValueTask DisposeAsync()
    {
        if (this.unit != null)
        {
            await this.unit.DisposeAsync().ConfigureAwait(false);   // rolls back if not committed
            this.unit = null;
            this.currentTx = null;
        }
        this.ownedScope?.Dispose();
    }
}
