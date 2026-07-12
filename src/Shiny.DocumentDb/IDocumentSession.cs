using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// A short-lived, single-flow <b>unit of work</b> over an <see cref="IDocumentStore"/> — the EF-<c>DbContext</c>
/// analogue. Buffered write verbs (<see cref="Add{T}"/>/<see cref="Update{T}"/>/…) accumulate; <see cref="SaveChanges"/>
/// flushes them atomically. Reads are immediate. Not thread-safe: use one session per logical flow. See the
/// store-as-connection design (§4e/§4f).
/// </summary>
/// <remarks>The typed CRUD/query/transaction surface. The late-bound JSON lane and spatial/vector/full-text
/// families remain on the root store (IDocumentStore) — reach them via session.Store.</remarks>
public interface IDocumentSession : IAsyncDisposable
{
    /// <summary>The session's DI scope — interceptors resolve their scoped services from this (replaces the AsyncLocal carrier).</summary>
    IServiceProvider Services { get; }

    /// <summary>The owning root store, for the root-only surface (change feed, maintenance, backup).
    /// NOTE: writes via <see cref="Store"/> are immediate and do NOT join this session's transaction.</summary>
    IDocumentStore Store { get; }

    // ── Buffered unit-of-work writes (buffer until SaveChanges) ─────────────
    IDocumentSession Add<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class;
    IDocumentSession AddRange<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class;
    IDocumentSession Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class;
    IDocumentSession Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class;
    IDocumentSession Remove<T>(object id) where T : class;

    /// <summary>Number of buffered operations not yet flushed.</summary>
    int PendingCount { get; }

    /// <summary>Discards buffered operations without executing them.</summary>
    void ClearPending();

    /// <summary>Flushes buffered writes. Joins the active transaction (no commit) if one is open,
    /// otherwise opens an implicit transaction, flushes, and commits.</summary>
    Task SaveChanges(CancellationToken cancellationToken = default);

    /// <summary>As <see cref="SaveChanges(CancellationToken)"/>, optionally suppressing all interceptors for the flush.</summary>
    Task SaveChanges(bool suppressInterceptors, CancellationToken cancellationToken = default);

    // ── Explicit transaction (§4f) — one active at a time ──────────────────
    /// <summary>The active transaction, or null when none is open.</summary>
    IDocumentTransaction? CurrentTransaction { get; }

    /// <summary>Opens an explicit transaction, pinning one connection for its duration. Throws if one is already active.</summary>
    Task<IDocumentTransaction> BeginTransaction(CancellationToken cancellationToken = default);

    /// <summary>Opens an explicit transaction at a specific isolation level — e.g.
    /// <see cref="System.Data.IsolationLevel.RepeatableRead"/> / <see cref="System.Data.IsolationLevel.Snapshot"/>
    /// for a <b>consistent-read session</b> (all reads see one snapshot). Relational providers only.</summary>
    Task<IDocumentTransaction> BeginTransaction(System.Data.IsolationLevel isolationLevel, CancellationToken cancellationToken = default);

    // ── Immediate reads (see committed data, not the un-flushed buffer) ────
    Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;
    Task<T?> Get<T>(object id, LockMode lockMode, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;
    IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class;
    Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>Suppresses every interceptor for the duration of the returned scope on the current async flow.</summary>
    IDisposable SuppressInterceptors();
}
