using System.Linq.Expressions;
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
    readonly IDocumentStore? borrowedTx;  // set for a session bound to an already-open (outer-owned) transaction

    ExplicitUnit? unit;
    DocumentTransaction? currentTx;

    // T1/T2: a unit-of-work parent span (opened lazily on first op, held for the session's life). Child op
    // spans nest under it and share its TraceId → a unit of work is one correlated trace subtree. Null when
    // unobserved (zero-cost) or for a borrowed ctx.Session (it's already inside a write's span).
    readonly Guid sessionId = Guid.NewGuid();
    System.Diagnostics.Activity? unitActivity;
    bool unitStarted;

    void EnsureUnitActivity()
    {
        if (this.unitStarted || this.borrowedTx != null)
            return;
        this.unitStarted = true;
        this.unitActivity = (this.store as Diagnostics.IUnitScopeSource)?.StartUnitActivity("unit_of_work");
        this.unitActivity?.SetTag("db.session.id", this.sessionId);
    }

    internal DocumentSession(IDocumentStore store, IServiceProvider? services, IServiceScope? ownedScope)
    {
        this.store = store;
        this.services = services;
        this.ownedScope = ownedScope;
        this.buffer = new UnitOfWork((IUnitOfWorkEngine)store);
        this.txEngine = store as IExplicitTransactionEngine;
    }

    DocumentSession(IDocumentStore txStore, IServiceProvider? services, bool borrowed)
    {
        // Bound to an already-open transaction the outer engine owns: writes flush into txStore (no commit),
        // reads route to it, and BeginTransaction is disallowed (no nesting). Used for ctx.Session.
        this.store = txStore;
        this.services = services;
        this.buffer = new UnitOfWork(txStore as IUnitOfWorkEngine);   // engine unused — flushes via FlushInto
        this.borrowedTx = txStore;
    }

    /// <summary>Creates a session bound to an already-open transaction (the transaction-bound store) — the
    /// interceptor re-entrancy handle exposed as <c>ctx.Session</c>. Its writes flush into that transaction.</summary>
    internal static DocumentSession ForTransaction(IDocumentStore txStore, IServiceProvider? services)
        => new(txStore, services, borrowed: true);

    public IServiceProvider Services => this.services ?? EmptyServiceProvider.Instance;
    public IDocumentStore Store => this.store;
    public IDocumentTransaction? CurrentTransaction => this.currentTx;
    public int PendingCount => this.buffer.PendingCount;
    public void ClearPending() => this.buffer.Clear();

    // The store every operation targets: the transaction-bound store while a tx is open (owned or borrowed),
    // else the root.
    IDocumentStore Target => this.unit?.Store ?? this.borrowedTx ?? this.store;

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
        var count = this.buffer.PendingCount;
        if (count == 0)
            return;
        this.EnsureUnitActivity();
        (this.store as Diagnostics.IUnitScopeSource)?.RecordUnitSize(count);   // T3

        // Flow the session's DI scope into the write pipeline (replaces the AsyncLocal carrier; interceptors read it).
        using var flowScope = this.services != null ? DocumentOperationScope.EnterServices(this.services) : null;

        var txStore = this.unit?.Store ?? this.borrowedTx;
        if (txStore != null)
        {
            // Join the active transaction (owned or borrowed): flush into it, do NOT commit (the owner commits).
            using var suppression = suppressInterceptors ? DocumentOperationScope.SuppressInterceptors() : null;
            await this.buffer.FlushInto(txStore, cancellationToken).ConfigureAwait(false);
            this.buffer.Clear();
        }
        else
        {
            // No active transaction: the buffer opens its own transaction, flushes, commits, and clears.
            await this.buffer.SaveChanges(suppressInterceptors, cancellationToken).ConfigureAwait(false);
        }
    }

    // ── Explicit transaction ───────────────────────────────────────────────
    public Task<IDocumentTransaction> BeginTransaction(CancellationToken cancellationToken = default)
        => this.BeginTransaction(System.Data.IsolationLevel.Unspecified, cancellationToken);

    public async Task<IDocumentTransaction> BeginTransaction(System.Data.IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        if (this.borrowedTx != null)
            throw new InvalidOperationException(
                "This session is already bound to a transaction (ctx.Session). Nested transactions are not supported.");
        if (this.currentTx != null)
            throw new InvalidOperationException(
                "A transaction is already active on this session. Only one transaction may be active at a time.");
        if (this.txEngine == null)
            throw new NotSupportedException(
                $"Explicit transactions are not supported by '{this.store.GetType().Name}'. They are available on the relational providers (§4f).");

        this.EnsureUnitActivity();
        this.unit = await this.txEngine.BeginExplicitUnitAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
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
    // Reads flow the session's DI scope too (via EnterScope), so scope-based features — scope-aware tenancy
    // (a scoped ITenantResolver) — resolve from the caller's scope, not the root.
    public async Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        this.EnsureUnitActivity();
        using var scope = this.EnterScope();
        return await this.Target.Get(id, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
    }

    public async Task<T?> Get<T>(object id, LockMode lockMode, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        // The lock is enforced by the active transaction (SQLite takes a whole-DB write lock). A locking read
        // outside a transaction is meaningless, so require one. Provider-specific FOR UPDATE SQL is future work.
        if (lockMode != LockMode.None && this.unit == null && this.borrowedTx == null)
            throw new InvalidOperationException("A locking read (LockMode != None) requires an active transaction — call BeginTransaction first.");
        this.EnsureUnitActivity();
        using var scope = this.EnterScope();
        return await this.Target.Get(id, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
    }

    public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class
    {
        this.EnsureUnitActivity();   // hold the unit span open so the query's deferred terminals nest under it
        return this.Target.Query(jsonTypeInfo);
    }

    public async Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class
    {
        this.EnsureUnitActivity();
        using var scope = this.EnterScope();
        return await this.Target.Count<T>(whereClause, parameters, cancellationToken).ConfigureAwait(false);
    }

    // ── Vector (ANN) search ────────────────────────────────────────────────
    // Reads delegate to Target (the transaction-bound store while a tx is open → consistent-snapshot vector
    // search), flowing the session's DI scope like the other reads. SupportsVector is a static store capability —
    // reach it via session.Store, it isn't duplicated here.
    public async Task<IReadOnlyList<VectorResult<T>>> NearestVectors<T>(
        ReadOnlyMemory<float> query,
        int k,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
    {
        this.EnsureUnitActivity();
        using var scope = this.EnterScope();
        return await this.Target.NearestVectors(query, k, filter, cancellationToken).ConfigureAwait(false);
    }

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
        this.unitActivity?.Dispose();
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
        this.unitActivity?.Dispose();
        this.ownedScope?.Dispose();
    }
}
