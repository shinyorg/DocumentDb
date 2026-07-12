using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// Optional EF-Core-style typed front-end over an <see cref="IDocumentSession"/>. Derive a <c>partial</c>
/// class from this, decorate it with <c>[Document(typeof(T))]</c> for each aggregate, and the
/// <c>Shiny.DocumentDb.Generators</c> source generator fills in a strongly-typed <see cref="DocumentSet{T}"/>
/// property per declared type plus a <c>ConfigureModel</c> lowering and a DI extension.
/// </summary>
/// <remarks>
/// The context wraps an <see cref="IDocumentSession"/> — it *is* a unit of work. The typed sets forward to the
/// same session (immediate reads/writes); group writes into one transaction with <see cref="Add{T}"/> +
/// <see cref="SaveChanges"/> or an explicit <see cref="BeginTransaction"/>. A context is not thread-safe —
/// register it scoped (ASP.NET) or create one per unit of work via <c>IDocumentContextFactory</c> (MAUI/desktop).
/// The context owns its session and disposes it.
/// </remarks>
public abstract class DocumentContext : IAsyncDisposable, IDisposable
{
    /// <summary>Creates the context over the supplied session (the context owns and disposes it).</summary>
    protected DocumentContext(IDocumentSession session)
    {
        ArgumentNullException.ThrowIfNull(session);
        this.Session = session;
    }

    /// <summary>Convenience: opens a scope-less session over <paramref name="store"/> (no DI scope flow — for
    /// hand-rolled use outside a container). Prefer the DI registrations for scoped-interceptor support.</summary>
    protected DocumentContext(IDocumentStore store) : this((store ?? throw new ArgumentNullException(nameof(store))).OpenSession()) { }

    /// <summary>The underlying unit-of-work session. Use it for anything the typed sets do not surface.</summary>
    public IDocumentSession Session { get; }

    /// <summary>The root store, for the root-only surface (change feed, maintenance, backup).</summary>
    public IDocumentStore Store => this.Session.Store;

    // ── Unit-of-work surface (forwards to the session) ──────────────────────
    /// <summary>Buffers an insert. Commit with <see cref="SaveChanges"/>.</summary>
    public DocumentContext Add<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class { this.Session.Add(document, jsonTypeInfo); return this; }
    /// <summary>Buffers a full-document update.</summary>
    public DocumentContext Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class { this.Session.Update(document, jsonTypeInfo); return this; }
    /// <summary>Buffers an RFC 7396 merge upsert.</summary>
    public DocumentContext Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null) where T : class { this.Session.Upsert(patch, jsonTypeInfo); return this; }
    /// <summary>Buffers a remove by id.</summary>
    public DocumentContext Remove<T>(object id) where T : class { this.Session.Remove<T>(id); return this; }

    /// <summary>Commits buffered writes atomically (joins an active transaction, else opens one).</summary>
    public Task SaveChanges(CancellationToken cancellationToken = default) => this.Session.SaveChanges(cancellationToken);

    /// <summary>Opens an explicit transaction on the session (one active at a time; relational providers only).</summary>
    public Task<IDocumentTransaction> BeginTransaction(CancellationToken cancellationToken = default) => this.Session.BeginTransaction(cancellationToken);

    /// <summary>
    /// Creates a typed set bound to this context's session. The generator calls this for each <c>[Document]</c>
    /// type; <paramref name="typeInfo"/> is normally <c>null</c> (the session's store resolves AOT-safe metadata
    /// from its configured resolver — see the generated <c>ConfigureModel</c>).
    /// </summary>
    protected DocumentSet<T> Set<T>(JsonTypeInfo<T>? typeInfo = null) where T : class
        => new(this.Session, typeInfo, this);

    /// <summary>Disposes the owned session (rolling back any open transaction, discarding un-flushed writes).</summary>
    public ValueTask DisposeAsync() => this.Session.DisposeAsync();

    /// <summary>Synchronous dispose of the owned session (like EF's <c>DbContext</c>, both are supported).</summary>
    public void Dispose() => (this.Session as IDisposable)?.Dispose();
}
