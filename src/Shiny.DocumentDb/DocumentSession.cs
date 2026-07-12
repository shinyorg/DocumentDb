using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// Default <see cref="IDocumentSession"/> — a unit of work over any root <see cref="IDocumentStore"/>. Buffers
/// writes (reusing <see cref="UnitOfWork"/>'s buffer + coalescing), flushes them on <see cref="SaveChanges"/>, and
/// pins a connection for an explicit <see cref="BeginTransaction"/>. Explicit transactions require the store to
/// support them (the relational store); other providers throw. Not thread-safe (§4e/§4f).
/// </summary>
public sealed class DocumentSession : IDocumentSession, IDisposable
{
    readonly IDocumentStore store;
    readonly IExplicitTransactionEngine? txEngine;
    readonly UnitOfWork buffer;
    readonly IServiceProvider? services;
    readonly IServiceScope? ownedScope;   // disposed only when the session created it (factory path)

    ExplicitUnit? unit;
    DocumentTransaction? currentTx;

    internal DocumentSession(IDocumentStore store, IServiceProvider? services, IServiceScope? ownedScope)
    {
        this.store = store;
        this.services = services;
        this.ownedScope = ownedScope;
        this.buffer = new UnitOfWork((IUnitOfWorkEngine)store);
        this.txEngine = store as IExplicitTransactionEngine;
    }

    public IServiceProvider Services => this.services ?? EmptyServiceProvider.Instance;
    public IDocumentStore Store => this.store;
    public IDocumentTransaction? CurrentTransaction => this.currentTx;
    public int PendingCount => this.buffer.PendingCount;
    public void ClearPending() => this.buffer.Clear();

    // The store every operation targets: the transaction-bound store while a tx is open, else the root.
    IDocumentStore Target => this.unit?.Store ?? this.store;

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
        using var flowScope = this.services != null ? DocumentOperationScope.EnterServices(this.services) : null;

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
        if (this.txEngine == null)
            throw new NotSupportedException(
                $"Explicit transactions are not supported by '{this.store.GetType().Name}'. They are available on the relational providers (§4f).");

        this.unit = await this.txEngine.BeginExplicitUnitAsync(cancellationToken).ConfigureAwait(false);
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
        // The lock is enforced by the active transaction (SQLite takes a whole-DB write lock). A locking read
        // outside a transaction is meaningless, so require one. Provider-specific FOR UPDATE SQL is future work.
        if (lockMode != LockMode.None && this.unit == null)
            throw new InvalidOperationException("A locking read (LockMode != None) requires an active transaction — call BeginTransaction first.");
        return this.Target.Get(id, jsonTypeInfo, cancellationToken);
    }

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
        => this.Target.Query(jsonTypeInfo);

    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
        => this.Target.Count<T>(whereClause, parameters, cancellationToken);

    public IDisposable SuppressInterceptors() => DocumentOperationScope.SuppressInterceptors();

    // Flows this session's real DI scope (null → scope-less; the store's fallback child-scope applies).
    // Used by DocumentSet to wrap immediate writes so scoped interceptors resolve the caller's services.
    internal IDisposable? EnterScope()
        => this.services != null ? DocumentOperationScope.EnterServices(this.services) : null;

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

    public void Dispose()
    {
        if (this.unit != null)
        {
            this.unit.Dispose();   // sync rollback if not committed
            this.unit = null;
            this.currentTx = null;
        }
        this.ownedScope?.Dispose();
    }
}
