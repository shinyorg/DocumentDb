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

    internal DocumentSet(IDocumentStore store, JsonTypeInfo<T>? typeInfo)
    {
        this.store = store;
        this.typeInfo = typeInfo;
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
    public Task Insert(T document, CancellationToken cancellationToken = default)
        => this.store.Insert(document, this.typeInfo, cancellationToken);

    /// <summary>Replaces an existing document entirely.</summary>
    public Task Update(T document, CancellationToken cancellationToken = default)
        => this.store.Update(document, this.typeInfo, cancellationToken);

    /// <summary>Updates an existing document — full replace (<paramref name="patch"/> false) or RFC 7396
    /// deep-merge (<paramref name="patch"/> true). Merge is relational-provider only.</summary>
    public Task Update(T document, bool patch, CancellationToken cancellationToken = default)
        => this.store.Update(document, patch, this.typeInfo, cancellationToken);

    /// <summary>Upserts a document (RFC 7396 JSON Merge Patch).</summary>
    public Task Upsert(T patch, CancellationToken cancellationToken = default)
        => this.store.Upsert(patch, this.typeInfo, cancellationToken);

    /// <summary>Upserts a document — merge on update (<paramref name="patchIfUpdate"/> true) or wholesale
    /// replace on update (<paramref name="patchIfUpdate"/> false). Replace-on-update is relational-provider only.</summary>
    public Task Upsert(T patch, bool patchIfUpdate, CancellationToken cancellationToken = default)
        => this.store.Upsert(patch, patchIfUpdate, this.typeInfo, cancellationToken);

    /// <summary>Removes a document by id. Returns <c>true</c> if one was deleted.</summary>
    public Task<bool> Remove(object id, CancellationToken cancellationToken = default)
        => this.store.Remove<T>(id, cancellationToken);

    /// <summary>Removes every document of this type. Returns the number deleted.</summary>
    public Task<int> Clear(CancellationToken cancellationToken = default)
        => this.store.Clear<T>(cancellationToken);

    // ── batch writes (atomic where the provider supports it) ─────────────────

    /// <summary>Inserts many documents in one transaction. Returns the number inserted.</summary>
    public Task<int> BatchInsert(IEnumerable<T> documents, CancellationToken cancellationToken = default)
        => this.store.BatchInsert(documents, this.typeInfo, cancellationToken);

    /// <summary>Upserts many documents as one batch. Returns the number upserted.</summary>
    public Task<int> BatchUpsert(IEnumerable<T> patches, CancellationToken cancellationToken = default)
        => this.store.BatchUpsert(patches, this.typeInfo, cancellationToken);

    /// <summary>Updates many existing documents as one batch. Returns the number updated.</summary>
    public Task<int> BatchUpdate(IEnumerable<T> documents, CancellationToken cancellationToken = default)
        => this.store.BatchUpdate(documents, this.typeInfo, cancellationToken);

    /// <summary>Removes many documents by id as one batch. Returns the number deleted.</summary>
    public Task<int> BatchRemove(IEnumerable<object> ids, CancellationToken cancellationToken = default)
        => this.store.BatchRemove<T>(ids, cancellationToken);
}
