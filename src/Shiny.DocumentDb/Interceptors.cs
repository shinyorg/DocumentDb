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
}

/// <summary>Context passed to an <see cref="IDocumentInterceptor"/>.</summary>
public sealed class DocumentWriteContext
{
    public DocumentOperation Operation { get; init; }
    public DocumentOperationSource Source { get; init; }
    public required Type DocumentType { get; init; }
    public required string TypeName { get; init; }

    /// <summary>The document id. May be a default/unassigned value in <see cref="IDocumentInterceptor.BeforeWrite"/> for auto-generated ids; populated by <see cref="IDocumentInterceptor.AfterWrite"/>.</summary>
    public object? Id { get; internal set; }

    /// <summary>The document. Mutable in <see cref="IDocumentInterceptor.BeforeWrite"/>. Null for delete-by-id.</summary>
    public object? Document { get; set; }

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
}

/// <summary>Context passed to an <see cref="IDocumentBulkInterceptor"/>.</summary>
public sealed class DocumentBulkContext
{
    public DocumentOperation Operation { get; init; }
    public DocumentOperationSource Source { get; init; }
    public required Type DocumentType { get; init; }
    public required string TypeName { get; init; }

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
    // options-registered ones above, keeping a single deterministic execution order everywhere.
    IReadOnlyList<IDocumentInterceptor>? diPerDoc;
    IReadOnlyList<IDocumentBulkInterceptor>? diBulk;
    bool attached;

    bool HasAnyPerDoc => this.perDoc.Count > 0 || this.diPerDoc is { Count: > 0 };
    bool HasAnyBulk => this.bulk.Count > 0 || this.diBulk is { Count: > 0 };

    // ── Registration ────────────────────────────────────────────────────
    public void Add(IDocumentInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);
        this.perDoc.Add(interceptor);
    }

    public void AddBulk(IDocumentBulkInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);
        this.bulk.Add(interceptor);
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
    }

    public void AddBefore<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        this.perDoc.Add(new LambdaInterceptor(typeof(T), handler, null));
    }

    public void AddAfter<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        this.perDoc.Add(new LambdaInterceptor(typeof(T), null, handler));
    }

    // ── Per-document execution ──────────────────────────────────────────
    public bool HasPerDoc => this.HasAnyPerDoc;

    public DocumentWriteContext? NewWrite<T>(DocumentOperation op, string typeName, object? id, T? document) where T : class
        => !this.HasAnyPerDoc
            ? null
            : new DocumentWriteContext { Operation = op, Source = DocumentOperationScope.Current, DocumentType = typeof(T), TypeName = typeName, Id = id, Document = document };

    public async Task BeforeWrite(DocumentWriteContext? ctx, CancellationToken ct)
    {
        if (ctx == null) return;
        for (var i = 0; i < this.perDoc.Count; i++)
            await this.perDoc[i].BeforeWrite(ctx, ct).ConfigureAwait(false);
        if (this.diPerDoc != null)
            for (var i = 0; i < this.diPerDoc.Count; i++)
                await this.diPerDoc[i].BeforeWrite(ctx, ct).ConfigureAwait(false);
    }

    public async Task AfterWrite(DocumentWriteContext? ctx, object? id, int? version, CancellationToken ct)
    {
        if (ctx == null) return;
        ctx.Id = id;
        ctx.Version = version;
        ctx.Succeeded = true;
        for (var i = 0; i < this.perDoc.Count; i++)
            await this.perDoc[i].AfterWrite(ctx, ct).ConfigureAwait(false);
        if (this.diPerDoc != null)
            for (var i = 0; i < this.diPerDoc.Count; i++)
                await this.diPerDoc[i].AfterWrite(ctx, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs per-document <c>BeforeWrite</c> over a batch before serialization, applying any document
    /// replacements back into <paramref name="documents"/>. Returns the contexts (index-aligned) to pass
    /// to <see cref="AfterWrite"/> after each row is written, or null when no interceptors are registered.
    /// </summary>
    public async Task<DocumentWriteContext[]?> BeforeWriteBatch<T>(IList<T> documents, string typeName, CancellationToken ct) where T : class
    {
        if (!this.HasAnyPerDoc) return null;
        var ctxs = new DocumentWriteContext[documents.Count];
        for (var i = 0; i < documents.Count; i++)
        {
            var ctx = this.NewWrite(DocumentOperation.Insert, typeName, null, documents[i])!;
            await this.BeforeWrite(ctx, ct).ConfigureAwait(false);
            if (ctx.Document is T replaced)
                documents[i] = replaced;
            ctxs[i] = ctx;
        }
        return ctxs;
    }

    // ── Bulk (set-based) execution ──────────────────────────────────────
    public DocumentBulkContext? NewBulk<T>(DocumentOperation op, string typeName, string? whereClause = null, (string Property, object? Value)? assignment = null) where T : class
        => !this.HasAnyBulk
            ? null
            : new DocumentBulkContext { Operation = op, Source = DocumentOperationScope.Current, DocumentType = typeof(T), TypeName = typeName, WhereClause = whereClause, Assignment = assignment };

    public async Task BeforeBulk(DocumentBulkContext? ctx, CancellationToken ct)
    {
        if (ctx == null) return;
        for (var i = 0; i < this.bulk.Count; i++)
            await this.bulk[i].BeforeBulkWrite(ctx, ct).ConfigureAwait(false);
        if (this.diBulk != null)
            for (var i = 0; i < this.diBulk.Count; i++)
                await this.diBulk[i].BeforeBulkWrite(ctx, ct).ConfigureAwait(false);
    }

    public async Task AfterBulk(DocumentBulkContext? ctx, int affected, CancellationToken ct)
    {
        if (ctx == null) return;
        ctx.AffectedCount = affected;
        for (var i = 0; i < this.bulk.Count; i++)
            await this.bulk[i].AfterBulkWrite(ctx, ct).ConfigureAwait(false);
        if (this.diBulk != null)
            for (var i = 0; i < this.diBulk.Count; i++)
                await this.diBulk[i].AfterBulkWrite(ctx, ct).ConfigureAwait(false);
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

    public static DocumentOperationSource Current => current.Value;

    public static IDisposable Push(DocumentOperationSource source)
    {
        var previous = current.Value;
        current.Value = source;
        return new Pop(previous);
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
}
