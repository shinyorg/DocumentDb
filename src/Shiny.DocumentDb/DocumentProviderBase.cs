using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Shiny.DocumentDb;

/// <summary>
/// Shared logging wiring used by the core store and every provider: resolves the <c>Shiny.DocumentDb</c>
/// <see cref="ILogger"/> from the container and fans a SQL/diagnostic string out to both the raw
/// <c>options.Logging</c> callback and that logger (Debug, structured <c>{Sql}</c>). Keeps SQL logging
/// consistent — same category and level — across all providers.
/// </summary>
static class DocumentStoreLogging
{
    public const string Category = "Shiny.DocumentDb";

    public static ILogger? CreateLogger(IServiceProvider services)
        => (services.GetService(typeof(ILoggerFactory)) as ILoggerFactory)?.CreateLogger(Category);

    public static Action<string>? Compose(Action<string>? callback, ILogger? logger)
    {
        if (logger == null)
            return callback;
        return message =>
        {
            callback?.Invoke(message);
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("DocumentDb SQL: {Sql}", message);
        };
    }
}

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

    // Concrete OpenSession so `concreteStore.OpenSession()` resolves without an interface cast (default interface
    // members aren't visible on the concrete type). Every derived store implements IDocumentStore, so the cast
    // always succeeds. Explicit transactions are unsupported on these providers (§4f) — BeginTransaction throws.
    /// <summary>Opens a scope-less <see cref="IDocumentSession"/> unit of work over this store.</summary>
    public IDocumentSession OpenSession() => new DocumentSession((IDocumentStore)this, null, null);
    /// <summary>Opens a session bound to a caller-supplied DI scope.</summary>
    public IDocumentSession OpenSession(IServiceProvider scope) => new DocumentSession((IDocumentStore)this, scope, null);

    // Embedded OpenTelemetry: always present, zero-cost when unobserved. db.system.name is the backend derived
    // from the concrete store type (MongoDbDocumentStore → "mongodb"). Providers wrap their public operations
    // with this.Tracker.Track(op, typeof(T).Name, …).
    Diagnostics.OperationTracker? tracker;
    internal Diagnostics.OperationTracker Tracker => this.tracker ??= new(this.InstrumentationSystem, null);

    /// <summary>db.system.name for this provider. Defaults to the concrete type name minus a "DocumentStore"
    /// suffix, lower-cased (MongoDbDocumentStore → "mongodb"). Override if the backend name differs.</summary>
    internal virtual string InstrumentationSystem
    {
        get
        {
            var name = this.GetType().Name;
            if (name.EndsWith("DocumentStore", StringComparison.Ordinal))
                name = name[..^"DocumentStore".Length];
            return name.ToLowerInvariant();
        }
    }

    /// <summary>
    /// Wires DI-registered interceptors (so <c>IEnumerable&lt;IDocumentInterceptor&gt;</c> from the container
    /// run alongside options-registered ones) and captures the <see cref="IServiceScopeFactory"/> for fallback
    /// child scopes. Called by the provider's DI registration; a container-free <c>new XDocumentStore(options)</c>
    /// simply never calls it. This is what lifts DI interceptors to the non-relational providers — previously they
    /// had no <see cref="IServiceProvider"/> entry point, so container-registered interceptors silently never fired.
    /// </summary>
    internal void AttachServiceProvider(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        this.Interceptors.AttachServiceProvider(services);
        this.ScopeFactory = services.GetService(typeof(IServiceScopeFactory)) as IServiceScopeFactory;
        this.Logger = DocumentStoreLogging.CreateLogger(services);
    }

    /// <summary>Fallback child-scope factory captured from the container; null on the container-free path.</summary>
    internal IServiceScopeFactory? ScopeFactory { get; private set; }

    /// <summary>The <c>Shiny.DocumentDb</c> logger resolved from the container; null on the container-free path.
    /// A provider composes its <c>logging</c> callback with this via <see cref="DocumentStoreLogging.Compose"/>.</summary>
    internal ILogger? Logger { get; private set; }

    // ── Per-document ────────────────────────────────────────────────────
    /// <summary>True when at least one per-document interceptor is registered.</summary>
    protected bool HasPerDocInterceptors => this.Interceptors.HasPerDoc;

    /// <summary>Builds a write context (null when no per-doc interceptors are registered, or under a
    /// suppression scope). Pass <paramref name="jsonFactory"/> (e.g. <c>doc =&gt; Serialize((T)doc, typeInfo,
    /// jsonOptions)</c>) so interceptors can read <c>ctx.GetJson()</c>.</summary>
    protected DocumentWriteContext? NewWriteContext<T>(DocumentOperation op, string typeName, object? id, T? document, Func<object, string>? jsonFactory = null) where T : class
        => this.Interceptors.NewWrite(op, typeName, id, document, (IDocumentStore)this, DocumentOperationScope.CurrentServices, jsonFactory);

    protected Task RunBeforeWriteAsync(DocumentWriteContext? ctx, CancellationToken ct)
        => this.Interceptors.BeforeWrite(ctx, ct);

    protected Task RunAfterWriteAsync(DocumentWriteContext? ctx, object? id, int? version, CancellationToken ct)
        => this.Interceptors.AfterWrite(ctx, id, version, ct);

    /// <summary>Runs per-doc BeforeWrite across a batch (mutating <paramref name="documents"/>); returns index-aligned contexts or null.</summary>
    protected Task<DocumentWriteContext[]?> RunBeforeWriteBatchAsync<T>(IList<T> documents, string typeName, CancellationToken ct, Func<object, string>? jsonFactory = null) where T : class
        => this.Interceptors.BeforeWriteBatch(documents, typeName, (IDocumentStore)this, DocumentOperationScope.CurrentServices, ct, jsonFactory);

    // ── Bulk (set-based) ────────────────────────────────────────────────
    protected DocumentBulkContext? NewBulkContext<T>(DocumentOperation op, string typeName, string? whereClause = null, (string Property, object? Value)? assignment = null) where T : class
        => this.Interceptors.NewBulk<T>(op, typeName, whereClause, assignment);

    protected Task RunBeforeBulkAsync(DocumentBulkContext? ctx, CancellationToken ct)
        => this.Interceptors.BeforeBulk(ctx, ct);

    protected Task RunAfterBulkAsync(DocumentBulkContext? ctx, int affected, CancellationToken ct)
        => this.Interceptors.AfterBulk(ctx, affected, ct);
}
