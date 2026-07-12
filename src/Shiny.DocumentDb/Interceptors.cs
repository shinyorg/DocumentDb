using System.Text.Json;

namespace Shiny.DocumentDb;

/// <summary>The kind of write operation an interceptor is observing.</summary>
public enum DocumentOperation
{
    Insert,
    Update,
    Upsert,
    Delete,
    Clear
}

/// <summary>Where a write originated — a direct API call, or an internal temporal-driven write (e.g. Restore).</summary>
public enum DocumentOperationSource
{
    Direct,
    Temporal
}

/// <summary>
/// Intercepts single-document writes (<c>Insert</c>, <c>BatchInsert</c> per item, <c>Update</c>, <c>Upsert</c>,
/// <c>Remove</c>). The after-hook runs inside the same transaction as the write, after it succeeds and before
/// commit, so it sees the generated id/version and can perform transactional side effects (e.g. an outbox).
/// Per-document interceptors do NOT fire for set-based operations — see <see cref="IDocumentBulkInterceptor"/>.
/// </summary>
public interface IDocumentInterceptor
{
    /// <summary>
    /// Fires before the document is serialized and written. Mutations to <see cref="DocumentWriteContext.Document"/>
    /// are persisted. Throw to abort the write (and roll back the surrounding unit).
    /// </summary>
    Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct);

    /// <summary>Fires after the write succeeds, inside the transaction, with id/version populated.</summary>
    Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct);

    /// <summary>
    /// Relative execution order. Lower runs first. Interceptors with equal <see cref="Order"/> keep their
    /// registration order (options-registered before DI-registered). Defaults to <c>0</c>.
    /// </summary>
    int Order => 0;
}

/// <summary>
/// Opt-in marker for an <see cref="IDocumentInterceptor"/> that needs the caller's DI scope
/// (<see cref="DocumentWriteContext.Services"/>) during a write. Only interceptors implementing this marker
/// trigger the pipeline's scope machinery — plain interceptors keep the allocation-free hot path. When no
/// ambient scope is flowing (e.g. a raw singleton store, an Orleans grain write, a background worker), the
/// pipeline opens one fresh child scope per write (spanning <c>BeforeWrite</c>→write→<c>AfterWrite</c>) from the
/// store's <see cref="System.IServiceProvider"/>, and disposes it after.
/// </summary>
public interface IScopedDocumentInterceptor : IDocumentInterceptor
{
}

/// <summary>Context passed to an <see cref="IDocumentInterceptor"/>.</summary>
public sealed class DocumentWriteContext
{
    public DocumentOperation Operation { get; init; }
    public DocumentOperationSource Source { get; init; }
    public required Type DocumentType { get; init; }
    public required string TypeName { get; init; }

    /// <summary>
    /// The DI scope for this write, when one is available: the caller's ambient scope (flowed through a scoped
    /// <see cref="DocumentContext"/>), or a fresh child scope the pipeline opened for an
    /// <see cref="IScopedDocumentInterceptor"/> when no ambient scope exists. Null when neither applies —
    /// resolve scoped services here, never in the interceptor's constructor (interceptors are singletons).
    /// </summary>
    public IServiceProvider? Services { get; internal set; }

    /// <summary>
    /// The originating, operation-scoped store. Inside a unit of work (and for a single write that has per-document
    /// interceptors registered — which runs as an implicit one-operation unit) this is the transaction-bound store,
    /// so reads and side-effect writes made through it in <see cref="IDocumentInterceptor.BeforeWrite"/> /
    /// <see cref="IDocumentInterceptor.AfterWrite"/> run on the same connection and transaction as the triggering
    /// write (an <c>AfterWrite</c> outbox insert commits atomically with it and sees this unit's uncommitted rows).
    /// Never null. Valid only within the hook — do not capture it past <c>AfterWrite</c>. Wrap re-entrant
    /// side-effect writes in <see cref="IDocumentStore.SuppressInterceptors"/> so they don't recurse.
    /// </summary>
    public IDocumentStore Store { get; internal set; } = null!;

    /// <summary>The document id. May be a default/unassigned value in <see cref="IDocumentInterceptor.BeforeWrite"/> for auto-generated ids; populated by <see cref="IDocumentInterceptor.AfterWrite"/>.</summary>
    public object? Id { get; internal set; }

    object? document;
    string? cachedJson;

    /// <summary>The document. Mutable in <see cref="IDocumentInterceptor.BeforeWrite"/>. Null for delete-by-id.
    /// Assigning a new value invalidates any JSON cached by <see cref="GetJson"/>.</summary>
    public object? Document
    {
        get => this.document;
        set
        {
            this.document = value;
            this.cachedJson = null;
        }
    }

    /// <summary>Serialize delegate the store supplies when it builds the context (it holds the
    /// <c>JsonTypeInfo</c>/options; the pipeline does not). Null for deletes / when no document is present.</summary>
    internal Func<object, string>? JsonFactory { get; set; }

    /// <summary>Pre-materialized JSON for the late-bound JSON write lane, where there is no CLR
    /// <see cref="Document"/> to serialize. When set, <see cref="GetJson"/> returns it directly.</summary>
    internal string? RawJson { get; set; }

    /// <summary>
    /// The exact JSON that will be persisted for <see cref="Document"/>, serialized with the store's
    /// configured options/typeInfo. Computed on first access and cached; the cache is invalidated if an
    /// earlier interceptor replaces <see cref="Document"/>. Returns null for deletes (no document).
    /// </summary>
    public string? GetJson()
    {
        if (this.RawJson != null)
            return this.RawJson;
        if (this.document == null || this.JsonFactory == null)
            return null;
        return this.cachedJson ??= this.JsonFactory(this.document);
    }

    /// <summary>Parsed form of <see cref="GetJson"/>. Caller owns the returned document — dispose it (<c>using</c>).</summary>
    public JsonDocument? GetJsonDocument()
    {
        var json = this.GetJson();
        return json == null ? null : JsonDocument.Parse(json);
    }

    /// <summary>The optimistic-concurrency version, when the type maps one.</summary>
    public int? Version { get; internal set; }

    // After-write only:
    public bool Succeeded { get; internal set; }
    public Exception? Error { get; internal set; }
}

/// <summary>
/// Intercepts set-based writes (<c>ExecuteUpdate</c>, <c>ExecuteDelete</c>, <c>Clear</c>) that never materialize the
/// affected documents. Fires once per call with the translated predicate and affected count.
/// </summary>
public interface IDocumentBulkInterceptor
{
    /// <summary>Fires once before the set-based write. Throw to abort.</summary>
    Task BeforeBulkWrite(DocumentBulkContext ctx, CancellationToken ct);

    /// <summary>Fires once after the set-based write, with <see cref="DocumentBulkContext.AffectedCount"/> populated.</summary>
    Task AfterBulkWrite(DocumentBulkContext ctx, CancellationToken ct);

    /// <summary>
    /// Relative execution order. Lower runs first. Interceptors with equal <see cref="Order"/> keep their
    /// registration order (options-registered before DI-registered). Defaults to <c>0</c>.
    /// </summary>
    int Order => 0;
}

/// <summary>Context passed to an <see cref="IDocumentBulkInterceptor"/>.</summary>
public sealed class DocumentBulkContext
{
    public DocumentOperation Operation { get; init; }
    public DocumentOperationSource Source { get; init; }
    public required Type DocumentType { get; init; }
    public required string TypeName { get; init; }

    /// <summary>The DI scope for this set-based write, when available (see <see cref="DocumentWriteContext.Services"/>).</summary>
    public IServiceProvider? Services { get; internal set; }

    /// <summary>The originating, operation-scoped store (see <see cref="DocumentWriteContext.Store"/>). Never null.</summary>
    public IDocumentStore Store { get; internal set; } = null!;

    /// <summary>The translated WHERE clause (including injected query filters); null for Clear-all.</summary>
    public string? WhereClause { get; init; }

    /// <summary>For <c>ExecuteUpdate</c>: the property/value assignment.</summary>
    public (string Property, object? Value)? Assignment { get; init; }

    /// <summary>Number of documents affected — populated in <see cref="IDocumentBulkInterceptor.AfterBulkWrite"/>.</summary>
    public int AffectedCount { get; internal set; }
}

/// <summary>
/// Shared interceptor storage, registration, and execution. The core and each document-provider options
/// class holds one of these, so the public registration API (`AddInterceptor` / `OnBeforeWrite&lt;T&gt;` …)
/// AND the per-write execution helpers are identical everywhere without duplication. Returns null contexts
/// (no allocation) when nothing is registered, keeping the no-interceptor hot path free.
/// </summary>
sealed class InterceptorPipeline
{
    readonly List<IDocumentInterceptor> perDoc = new();
    readonly List<IDocumentBulkInterceptor> bulk = new();

    // DI-resolved interceptors — populated once by AttachServiceProvider. These run AFTER the
    // options-registered ones above (before Order sorting), keeping a single deterministic execution order.
    IReadOnlyList<IDocumentInterceptor>? diPerDoc;
    IReadOnlyList<IDocumentBulkInterceptor>? diBulk;
    bool attached;

    // Merged (options + DI) lists, stable-sorted by Order. Built lazily on first execution and invalidated
    // by any registration / attach. This is the list the execution helpers iterate.
    IReadOnlyList<IDocumentInterceptor>? effectivePerDoc;
    IReadOnlyList<IDocumentBulkInterceptor>? effectiveBulk;
    bool? needsScope;

    bool HasAnyPerDoc => this.perDoc.Count > 0 || this.diPerDoc is { Count: > 0 };
    bool HasAnyBulk => this.bulk.Count > 0 || this.diBulk is { Count: > 0 };

    void InvalidateEffective()
    {
        this.effectivePerDoc = null;
        this.effectiveBulk = null;
        this.needsScope = null;
    }

    // Stable sort by Order: LINQ OrderBy is stable, so options-before-DI order is preserved within a tie.
    IReadOnlyList<IDocumentInterceptor> EffectivePerDoc()
    {
        if (this.effectivePerDoc != null)
            return this.effectivePerDoc;
        var merged = new List<IDocumentInterceptor>(this.perDoc);
        if (this.diPerDoc != null)
            merged.AddRange(this.diPerDoc);
        merged = merged.OrderBy(i => i.Order).ToList();
        return this.effectivePerDoc = merged;
    }

    IReadOnlyList<IDocumentBulkInterceptor> EffectiveBulk()
    {
        if (this.effectiveBulk != null)
            return this.effectiveBulk;
        var merged = new List<IDocumentBulkInterceptor>(this.bulk);
        if (this.diBulk != null)
            merged.AddRange(this.diBulk);
        merged = merged.OrderBy(i => i.Order).ToList();
        return this.effectiveBulk = merged;
    }

    /// <summary>True when any registered per-document interceptor implements <see cref="IScopedDocumentInterceptor"/>
    /// — i.e. the pipeline must resolve a DI scope for writes. Precomputed once; the scope-free hot path is
    /// preserved byte-for-byte when this is false.</summary>
    public bool NeedsScope
    {
        get
        {
            if (this.needsScope is bool b)
                return b;
            var need = false;
            var list = this.EffectivePerDoc();
            for (var i = 0; i < list.Count && !need; i++)
                need = list[i] is IScopedDocumentInterceptor;
            this.needsScope = need;
            return need;
        }
    }

    // ── Registration ────────────────────────────────────────────────────
    public void Add(IDocumentInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);
        this.perDoc.Add(interceptor);
        this.InvalidateEffective();
    }

    public void AddBulk(IDocumentBulkInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);
        this.bulk.Add(interceptor);
        this.InvalidateEffective();
    }

    /// <summary>
    /// Resolves <c>IEnumerable&lt;IDocumentInterceptor&gt;</c> / <c>IEnumerable&lt;IDocumentBulkInterceptor&gt;</c>
    /// from the container so DI-registered interceptors run alongside the options-registered ones. Idempotent —
    /// the first call wins. DI-resolved interceptors execute AFTER options-registered ones.
    /// </summary>
    public void AttachServiceProvider(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (this.attached)
            return;
        this.attached = true;

        if (services.GetService(typeof(IEnumerable<IDocumentInterceptor>)) is IEnumerable<IDocumentInterceptor> perDocFromDi)
        {
            var list = perDocFromDi as IReadOnlyList<IDocumentInterceptor> ?? perDocFromDi.ToList();
            if (list.Count > 0)
                this.diPerDoc = list;
        }
        if (services.GetService(typeof(IEnumerable<IDocumentBulkInterceptor>)) is IEnumerable<IDocumentBulkInterceptor> bulkFromDi)
        {
            var list = bulkFromDi as IReadOnlyList<IDocumentBulkInterceptor> ?? bulkFromDi.ToList();
            if (list.Count > 0)
                this.diBulk = list;
        }
        this.InvalidateEffective();
    }

    public void AddBefore<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        this.perDoc.Add(new LambdaInterceptor(typeof(T), handler, null));
        this.InvalidateEffective();
    }

    public void AddAfter<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        this.perDoc.Add(new LambdaInterceptor(typeof(T), null, handler));
        this.InvalidateEffective();
    }

    // ── Per-document execution ──────────────────────────────────────────
    public bool HasPerDoc => this.HasAnyPerDoc;

    public DocumentWriteContext? NewWrite<T>(DocumentOperation op, string typeName, object? id, T? document, IDocumentStore store, IServiceProvider? services, Func<object, string>? jsonFactory = null) where T : class
        => (!this.HasAnyPerDoc || DocumentOperationScope.Suppressed)
            ? null
            : new DocumentWriteContext { Operation = op, Source = DocumentOperationScope.Current, DocumentType = typeof(T), TypeName = typeName, Id = id, Document = document, JsonFactory = jsonFactory, Store = store, Services = services };

    public async Task BeforeWrite(DocumentWriteContext? ctx, CancellationToken ct)
    {
        if (ctx == null) return;
        var list = this.EffectivePerDoc();
        for (var i = 0; i < list.Count; i++)
            await list[i].BeforeWrite(ctx, ct).ConfigureAwait(false);
    }

    public async Task AfterWrite(DocumentWriteContext? ctx, object? id, int? version, CancellationToken ct)
    {
        if (ctx == null) return;
        ctx.Id = id;
        ctx.Version = version;
        ctx.Succeeded = true;
        var list = this.EffectivePerDoc();
        for (var i = 0; i < list.Count; i++)
            await list[i].AfterWrite(ctx, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs per-document <c>BeforeWrite</c> over a batch before serialization, applying any document
    /// replacements back into <paramref name="documents"/>. Returns the contexts (index-aligned) to pass
    /// to <see cref="AfterWrite"/> after each row is written, or null when no interceptors are registered.
    /// </summary>
    public async Task<DocumentWriteContext[]?> BeforeWriteBatch<T>(IList<T> documents, string typeName, IDocumentStore store, IServiceProvider? services, CancellationToken ct, Func<object, string>? jsonFactory = null) where T : class
    {
        if (!this.HasAnyPerDoc || DocumentOperationScope.Suppressed) return null;
        var ctxs = new DocumentWriteContext[documents.Count];
        for (var i = 0; i < documents.Count; i++)
        {
            var ctx = this.NewWrite(DocumentOperation.Insert, typeName, null, documents[i], store, services, jsonFactory)!;
            await this.BeforeWrite(ctx, ct).ConfigureAwait(false);
            if (ctx.Document is T replaced)
                documents[i] = replaced;
            ctxs[i] = ctx;
        }
        return ctxs;
    }

    // ── Bulk (set-based) execution ──────────────────────────────────────
    public DocumentBulkContext? NewBulk<T>(DocumentOperation op, string typeName, string? whereClause = null, (string Property, object? Value)? assignment = null) where T : class
        => (!this.HasAnyBulk || DocumentOperationScope.Suppressed)
            ? null
            : new DocumentBulkContext { Operation = op, Source = DocumentOperationScope.Current, DocumentType = typeof(T), TypeName = typeName, WhereClause = whereClause, Assignment = assignment };

    public async Task BeforeBulk(DocumentBulkContext? ctx, CancellationToken ct)
    {
        if (ctx == null) return;
        var list = this.EffectiveBulk();
        for (var i = 0; i < list.Count; i++)
            await list[i].BeforeBulkWrite(ctx, ct).ConfigureAwait(false);
    }

    public async Task AfterBulk(DocumentBulkContext? ctx, int affected, CancellationToken ct)
    {
        if (ctx == null) return;
        ctx.AffectedCount = affected;
        var list = this.EffectiveBulk();
        for (var i = 0; i < list.Count; i++)
            await list[i].AfterBulkWrite(ctx, ct).ConfigureAwait(false);
    }
}

/// <summary>Adapts <c>OnBeforeWrite&lt;T&gt;</c>/<c>OnAfterWrite&lt;T&gt;</c> lambdas to <see cref="IDocumentInterceptor"/>, filtered by type.</summary>
sealed class LambdaInterceptor : IDocumentInterceptor
{
    readonly Type documentType;
    readonly Func<DocumentWriteContext, CancellationToken, Task>? before;
    readonly Func<DocumentWriteContext, CancellationToken, Task>? after;

    public LambdaInterceptor(Type documentType, Func<DocumentWriteContext, CancellationToken, Task>? before, Func<DocumentWriteContext, CancellationToken, Task>? after)
    {
        this.documentType = documentType;
        this.before = before;
        this.after = after;
    }

    public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        => this.before != null && ctx.DocumentType == this.documentType ? this.before(ctx, ct) : Task.CompletedTask;

    public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
        => this.after != null && ctx.DocumentType == this.documentType ? this.after(ctx, ct) : Task.CompletedTask;
}

/// <summary>
/// Ambient flag marking the current write's <see cref="DocumentOperationSource"/>. Temporal-driven writes
/// (e.g. Restore) push <see cref="DocumentOperationSource.Temporal"/> around their inner Insert/Update so
/// interceptors can tell them apart from direct calls. Defaults to <see cref="DocumentOperationSource.Direct"/>.
/// </summary>
static class DocumentOperationScope
{
    static readonly AsyncLocal<DocumentOperationSource> current = new();
    static readonly AsyncLocal<bool> suppressed = new();
    static readonly AsyncLocal<IServiceProvider?> services = new();

    public static DocumentOperationSource Current => current.Value;

    /// <summary>The ambient DI scope flowing through the current async operation, set by a scoped
    /// <see cref="DocumentContext"/> (or a pipeline fallback child scope). Null when no scope is flowing.</summary>
    public static IServiceProvider? CurrentServices => services.Value;

    /// <summary>Flows <paramref name="serviceProvider"/> as the ambient DI scope for the duration of the
    /// returned scope, restoring the previous value on dispose. Interceptors read it via
    /// <see cref="DocumentWriteContext.Services"/>.</summary>
    public static IDisposable EnterServices(IServiceProvider serviceProvider)
    {
        var previous = services.Value;
        services.Value = serviceProvider;
        return new PopServices(previous);
    }

    /// <summary>True while a <see cref="SuppressInterceptors"/> scope is active on this async flow. When set,
    /// the pipeline builds no contexts and runs no interceptors for the duration of the scope.</summary>
    public static bool Suppressed => suppressed.Value;

    public static IDisposable Push(DocumentOperationSource source)
    {
        var previous = current.Value;
        current.Value = source;
        return new Pop(previous);
    }

    /// <summary>Suppresses every interceptor (per-document and bulk) for the duration of the returned scope.
    /// Bounded by the caller's async flow — see <see cref="UnitOfWork.SaveChanges(bool, CancellationToken)"/>.</summary>
    public static IDisposable SuppressInterceptors()
    {
        var previous = suppressed.Value;
        suppressed.Value = true;
        return new PopSuppress(previous);
    }

    sealed class Pop : IDisposable
    {
        readonly DocumentOperationSource previous;
        bool done;
        public Pop(DocumentOperationSource previous) => this.previous = previous;
        public void Dispose()
        {
            if (this.done) return;
            this.done = true;
            current.Value = this.previous;
        }
    }

    sealed class PopSuppress : IDisposable
    {
        readonly bool previous;
        bool done;
        public PopSuppress(bool previous) => this.previous = previous;
        public void Dispose()
        {
            if (this.done) return;
            this.done = true;
            suppressed.Value = this.previous;
        }
    }

    sealed class PopServices : IDisposable
    {
        readonly IServiceProvider? previous;
        bool done;
        public PopServices(IServiceProvider? previous) => this.previous = previous;
        public void Dispose()
        {
            if (this.done) return;
            this.done = true;
            services.Value = this.previous;
        }
    }
}
