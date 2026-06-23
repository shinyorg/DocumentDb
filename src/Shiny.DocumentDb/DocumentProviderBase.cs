namespace Shiny.DocumentDb;

/// <summary>
/// Shared base for the non-relational document providers (MongoDB, Cosmos DB, LiteDB, IndexedDB).
/// It centralizes the write-interceptor orchestration so each provider's write methods call the same
/// helpers instead of duplicating the pipeline plumbing. Storage and concurrency remain provider-specific
/// (each backend's transaction model and ETag/version mechanics differ too much to share safely), so this
/// base deliberately does not abstract persistence — only the cross-cutting interceptor flow.
/// </summary>
public abstract class DocumentProviderBase
{
    /// <summary>The interceptor pipeline for this provider — typically <c>this.options.Interceptors</c>.</summary>
    internal abstract InterceptorPipeline Interceptors { get; }

    // ── Per-document ────────────────────────────────────────────────────
    /// <summary>True when at least one per-document interceptor is registered.</summary>
    protected bool HasPerDocInterceptors => this.Interceptors.HasPerDoc;

    /// <summary>Builds a write context (null when no per-doc interceptors are registered, or under a
    /// suppression scope). Pass <paramref name="jsonFactory"/> (e.g. <c>doc =&gt; Serialize((T)doc, typeInfo,
    /// jsonOptions)</c>) so interceptors can read <c>ctx.GetJson()</c>.</summary>
    protected DocumentWriteContext? NewWriteContext<T>(DocumentOperation op, string typeName, object? id, T? document, Func<object, string>? jsonFactory = null) where T : class
        => this.Interceptors.NewWrite(op, typeName, id, document, jsonFactory);

    protected Task RunBeforeWriteAsync(DocumentWriteContext? ctx, CancellationToken ct)
        => this.Interceptors.BeforeWrite(ctx, ct);

    protected Task RunAfterWriteAsync(DocumentWriteContext? ctx, object? id, int? version, CancellationToken ct)
        => this.Interceptors.AfterWrite(ctx, id, version, ct);

    /// <summary>Runs per-doc BeforeWrite across a batch (mutating <paramref name="documents"/>); returns index-aligned contexts or null.</summary>
    protected Task<DocumentWriteContext[]?> RunBeforeWriteBatchAsync<T>(IList<T> documents, string typeName, CancellationToken ct, Func<object, string>? jsonFactory = null) where T : class
        => this.Interceptors.BeforeWriteBatch(documents, typeName, ct, jsonFactory);

    // ── Bulk (set-based) ────────────────────────────────────────────────
    protected DocumentBulkContext? NewBulkContext<T>(DocumentOperation op, string typeName, string? whereClause = null, (string Property, object? Value)? assignment = null) where T : class
        => this.Interceptors.NewBulk<T>(op, typeName, whereClause, assignment);

    protected Task RunBeforeBulkAsync(DocumentBulkContext? ctx, CancellationToken ct)
        => this.Interceptors.BeforeBulk(ctx, ct);

    protected Task RunAfterBulkAsync(DocumentBulkContext? ctx, int affected, CancellationToken ct)
        => this.Interceptors.AfterBulk(ctx, affected, ct);
}
