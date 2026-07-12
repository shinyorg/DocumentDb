using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// A strongly-typed facade over <see cref="IDocumentStore"/> for a single document type, exposed as a
/// property on a <see cref="DocumentContext"/>. Every member forwards to the store with this set's
/// <see cref="JsonTypeInfo{T}"/> pre-applied, so callers never re-type <c>&lt;T&gt;</c> or pass a
/// <c>JsonTypeInfo</c>. Queries return the store's <see cref="IDocumentQuery{T}"/> unchanged, so the full
/// query surface (<c>OrderBy</c>, <c>Select</c>, <c>Paginate</c>, aggregates, spatial/vector/full-text
/// terminators, <c>NotifyOnChange</c>, …) is available for free.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public sealed class DocumentSet<T> where T : class
{
    readonly IDocumentStore store;
    readonly JsonTypeInfo<T>? typeInfo;
    readonly DocumentContext? context;

    internal DocumentSet(IDocumentStore store, JsonTypeInfo<T>? typeInfo, DocumentContext? context = null)
    {
        this.store = store;
        this.typeInfo = typeInfo;
        this.context = context;
    }

    // Flows the owning context's DI scope for the duration of a write, so a scoped interceptor resolves the
    // caller's own scoped services (ctx.Services) rather than a fresh child scope. Read lazily (not captured)
    // so it honors the scope attached after construction; null → the store's own fallback applies. Reads don't
    // run interceptors, so only writes are wrapped.
    IDisposable? EnterScope()
    {
        var scope = this.context?.Scope;
        return scope == null ? null : DocumentOperationScope.EnterServices(scope);
    }

    /// <summary>
    /// The <see cref="JsonTypeInfo{T}"/> this set threads into every call, or <c>null</c> when the store
    /// resolves metadata from its configured resolver/reflection fallback.
    /// </summary>
    public JsonTypeInfo<T>? TypeInfo => this.typeInfo;

    // ── queries ─────────────────────────────────────────────────────────────

    /// <summary>Starts a fluent query for this type.</summary>
    public IDocumentQuery<T> Query() => this.store.Query(this.typeInfo);

    /// <summary>Starts a query filtered by <paramref name="predicate"/>.</summary>
    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate)
        => this.store.Query(this.typeInfo).Where(predicate);

    /// <summary>Gets a document by id, or <c>null</c> if not found.</summary>
    public Task<T?> Get(object id, CancellationToken cancellationToken = default)
        => this.store.Get(id, this.typeInfo, cancellationToken);

    /// <summary>Materializes every document of this type.</summary>
    public Task<IReadOnlyList<T>> ToList(CancellationToken cancellationToken = default)
        => this.store.Query(this.typeInfo).ToList(cancellationToken);

    /// <summary>Counts every document of this type.</summary>
    public Task<int> Count(CancellationToken cancellationToken = default)
        => this.store.Count<T>(cancellationToken: cancellationToken);

    // ── immediate writes ────────────────────────────────────────────────────

    /// <summary>Inserts a new document.</summary>
    public async Task Insert(T document, CancellationToken cancellationToken = default)
    {
        using (this.EnterScope())
            await this.store.Insert(document, this.typeInfo, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Replaces an existing document entirely.</summary>
    public async Task Update(T document, CancellationToken cancellationToken = default)
    {
        using (this.EnterScope())
            await this.store.Update(document, this.typeInfo, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Updates an existing document — full replace (<paramref name="patch"/> false) or RFC 7396
    /// deep-merge (<paramref name="patch"/> true). Merge is relational-provider only.</summary>
    public async Task Update(T document, bool patch, CancellationToken cancellationToken = default)
    {
        using (this.EnterScope())
            await this.store.Update(document, patch, this.typeInfo, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Upserts a document (RFC 7396 JSON Merge Patch).</summary>
    public async Task Upsert(T patch, CancellationToken cancellationToken = default)
    {
        using (this.EnterScope())
            await this.store.Upsert(patch, this.typeInfo, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Upserts a document — merge on update (<paramref name="patchIfUpdate"/> true) or wholesale
    /// replace on update (<paramref name="patchIfUpdate"/> false). Replace-on-update is relational-provider only.</summary>
    public async Task Upsert(T patch, bool patchIfUpdate, CancellationToken cancellationToken = default)
    {
        using (this.EnterScope())
            await this.store.Upsert(patch, patchIfUpdate, this.typeInfo, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Removes a document by id. Returns <c>true</c> if one was deleted.</summary>
    public async Task<bool> Remove(object id, CancellationToken cancellationToken = default)
    {
        using (this.EnterScope())
            return await this.store.Remove<T>(id, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Removes every document of this type. Returns the number deleted.</summary>
    public async Task<int> Clear(CancellationToken cancellationToken = default)
    {
        using (this.EnterScope())
            return await this.store.Clear<T>(cancellationToken).ConfigureAwait(false);
    }

    // ── batch writes (atomic where the provider supports it) ─────────────────

    /// <summary>Inserts many documents in one transaction. Returns the number inserted.</summary>
    public async Task<int> BatchInsert(IEnumerable<T> documents, CancellationToken cancellationToken = default)
    {
        using (this.EnterScope())
            return await this.store.BatchInsert(documents, this.typeInfo, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Upserts many documents as one batch. Returns the number upserted.</summary>
    public async Task<int> BatchUpsert(IEnumerable<T> patches, CancellationToken cancellationToken = default)
    {
        using (this.EnterScope())
            return await this.store.BatchUpsert(patches, this.typeInfo, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Updates many existing documents as one batch. Returns the number updated.</summary>
    public async Task<int> BatchUpdate(IEnumerable<T> documents, CancellationToken cancellationToken = default)
    {
        using (this.EnterScope())
            return await this.store.BatchUpdate(documents, this.typeInfo, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Removes many documents by id as one batch. Returns the number deleted.</summary>
    public async Task<int> BatchRemove(IEnumerable<object> ids, CancellationToken cancellationToken = default)
    {
        using (this.EnterScope())
            return await this.store.BatchRemove<T>(ids, cancellationToken).ConfigureAwait(false);
    }
}
