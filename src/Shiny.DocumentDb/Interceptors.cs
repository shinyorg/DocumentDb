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
/// Shared interceptor storage + registration logic. The core and each document-provider options class
/// holds one of these so the public registration API (`AddInterceptor` / `OnBeforeWrite&lt;T&gt;` …) is
/// identical everywhere without duplicating the lambda-adapter wiring.
/// </summary>
sealed class InterceptorRegistry
{
    public readonly List<IDocumentInterceptor> Interceptors = new();
    public readonly List<IDocumentBulkInterceptor> BulkInterceptors = new();

    public void Add(IDocumentInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);
        this.Interceptors.Add(interceptor);
    }

    public void AddBulk(IDocumentBulkInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);
        this.BulkInterceptors.Add(interceptor);
    }

    public void AddBefore<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        this.Interceptors.Add(new LambdaInterceptor(typeof(T), handler, null));
    }

    public void AddAfter<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        this.Interceptors.Add(new LambdaInterceptor(typeof(T), null, handler));
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

/// <summary>Runs the registered interceptor pipelines. Order = registration order. Throwing aborts.</summary>
static class InterceptorRunner
{
    public static async Task BeforeWriteAsync(IReadOnlyList<IDocumentInterceptor> interceptors, DocumentWriteContext ctx, CancellationToken ct)
    {
        for (var i = 0; i < interceptors.Count; i++)
            await interceptors[i].BeforeWrite(ctx, ct).ConfigureAwait(false);
    }

    public static async Task AfterWriteAsync(IReadOnlyList<IDocumentInterceptor> interceptors, DocumentWriteContext ctx, CancellationToken ct)
    {
        ctx.Succeeded = true;
        for (var i = 0; i < interceptors.Count; i++)
            await interceptors[i].AfterWrite(ctx, ct).ConfigureAwait(false);
    }

    public static async Task BeforeBulkAsync(IReadOnlyList<IDocumentBulkInterceptor> interceptors, DocumentBulkContext ctx, CancellationToken ct)
    {
        for (var i = 0; i < interceptors.Count; i++)
            await interceptors[i].BeforeBulkWrite(ctx, ct).ConfigureAwait(false);
    }

    public static async Task AfterBulkAsync(IReadOnlyList<IDocumentBulkInterceptor> interceptors, DocumentBulkContext ctx, CancellationToken ct)
    {
        for (var i = 0; i < interceptors.Count; i++)
            await interceptors[i].AfterBulkWrite(ctx, ct).ConfigureAwait(false);
    }
}
