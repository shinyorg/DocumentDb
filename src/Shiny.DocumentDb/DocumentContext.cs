using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// Optional EF-Core-style typed front-end over an <see cref="IDocumentStore"/>. Derive a <c>partial</c>
/// class from this, decorate it with <c>[Document(typeof(T))]</c> for each aggregate, and the
/// <c>Shiny.DocumentDb.Generators</c> source generator fills in a strongly-typed <see cref="DocumentSet{T}"/>
/// property per declared type plus a <c>ConfigureModel</c> lowering and a DI extension.
/// </summary>
/// <remarks>
/// This is a thin ergonomics + discoverability layer — every set member forwards to the same
/// <see cref="IDocumentStore"/> the raw API uses, so it works against all providers and adds no capability.
/// There is <b>no change tracking, identity map, or navigation/<c>Include</c></b>: a document store embeds,
/// it does not join. Writes are immediate (they match <see cref="IDocumentStore"/> 1:1); group them into a
/// transaction with <see cref="CreateUnitOfWork"/>. A context is not thread-safe — register it scoped, like a
/// <c>DbContext</c>.
/// </remarks>
public abstract class DocumentContext
{
    /// <summary>Creates the context over the supplied store.</summary>
    protected DocumentContext(IDocumentStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        this.Store = store;
    }

    /// <summary>The underlying store. Use it for anything the typed sets do not surface.</summary>
    public IDocumentStore Store { get; }

    /// <summary>
    /// The DI scope this context flows into write-time interceptors (<see cref="DocumentWriteContext.Services"/>).
    /// Set by the scoped registration to the request/resolving scope, so an <see cref="IScopedDocumentInterceptor"/>
    /// resolves the caller's own scoped instances rather than a fresh child scope. Null when the context was
    /// constructed outside DI (e.g. a hand-rolled <c>new AppDb(store)</c>) — writes then fall back per the store.
    /// </summary>
    internal IServiceProvider? Scope { get; private set; }

    /// <summary>Flows <paramref name="serviceProvider"/> (the resolving DI scope) into this context's writes.
    /// Called once by the scoped registration right after construction.</summary>
    internal void AttachScope(IServiceProvider serviceProvider) => this.Scope = serviceProvider;

    /// <summary>
    /// Creates a <see cref="UnitOfWork"/> for grouping writes into one transaction — the explicit-transaction
    /// escape hatch, since <see cref="DocumentSet{T}"/> writes are immediate.
    /// </summary>
    public UnitOfWork CreateUnitOfWork() => this.Store.CreateUnitOfWork();

    /// <summary>
    /// Creates a typed set bound to this context's store. The generator calls this for each <c>[Document]</c>
    /// type; <paramref name="typeInfo"/> is normally <c>null</c> (the store resolves AOT-safe metadata from
    /// its configured resolver — see the generated <c>ConfigureModel</c>), and may be supplied for an
    /// explicit, pre-resolved <see cref="JsonTypeInfo{T}"/>.
    /// </summary>
    protected DocumentSet<T> Set<T>(JsonTypeInfo<T>? typeInfo = null) where T : class
        => new(this.Store, typeInfo, this);
}
