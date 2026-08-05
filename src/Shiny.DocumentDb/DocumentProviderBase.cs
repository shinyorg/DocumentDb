using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Internal;
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
public abstract class DocumentProviderBase : Diagnostics.IUnitScopeSource
{
    /// <summary>The interceptor pipeline for this provider — typically <c>this.options.Interceptors</c>.</summary>
    /// <remarks>
    /// Protected, not internal: an internal abstract member on a public base class makes
    /// <see cref="DocumentProviderBase"/> unimplementable outside this assembly, which forced every provider
    /// onto <c>InternalsVisibleTo</c>. Deliberately not <c>protected internal</c> either — under IVT the
    /// compiler would require overrides to match as <c>protected internal override</c>, so the modifier a
    /// provider must write would depend on whether it holds an IVT grant. Plain <c>protected override</c>
    /// works the same everywhere.
    /// </remarks>
    protected abstract InterceptorPipeline Interceptors { get; }

    System.Diagnostics.Activity? Diagnostics.IUnitScopeSource.StartUnitActivity(string operation)
        => Diagnostics.DocumentStoreMetrics.StartActivity(this.InstrumentationSystem, operation, "(unit)");

    void Diagnostics.IUnitScopeSource.RecordUnitSize(long operations)
        => Diagnostics.DocumentStoreMetrics.RecordUnitSize(this.InstrumentationSystem, operations);

    // Concrete OpenSession so `concreteStore.OpenSession()` resolves without an interface cast (default interface
    // members aren't visible on the concrete type). Every derived store implements IDocumentStore, so the cast
    // always succeeds. Explicit transactions are unsupported on these providers (§4f) — BeginTransaction throws.
    /// <summary>Opens a scope-less <see cref="IDocumentSession"/> unit of work over this store.</summary>
    public IDocumentSession OpenSession() => new DocumentSession((IDocumentStore)this, null, null);
    /// <summary>Opens a session bound to a caller-supplied DI scope.</summary>
    public IDocumentSession OpenSession(IServiceProvider scope) => new DocumentSession((IDocumentStore)this, scope, null);

    /// <summary>
    /// True when something other than the resolving DI scope owns this store's lifetime — set by the
    /// tenant-per-database store cache, which shares one store across every request for that tenant and closes
    /// it itself. A provider's <c>Dispose</c>/<c>DisposeAsync</c> <b>must</b> return early while this is set,
    /// otherwise the first request to end closes the store the others are still using.
    /// </summary>
    protected internal bool DisposeSuppressed { get; internal set; }

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

    // ── Single-document write pipeline ──────────────────────────────────
    // Every provider's Insert/Update/Upsert/Remove opens with the same preamble (resolve the type info, id
    // accessor, type name and version mapping; build the write context; run BeforeWrite; honor a cancel; take
    // any replacement document) and closes with the same tail (AfterWrite, then publish the change). Those live
    // here so a change to the write pipeline — the next ctx.Cancel() — lands once instead of nine times.

    /// <summary>The provider's mapping state. Typically <c>this.options.Mappings</c>.</summary>
    protected abstract DocumentMappingRegistry Mappings { get; }

    /// <summary>The provider's id-accessor cache. Typically <c>this.idCache</c>.</summary>
    protected abstract IdAccessorCache IdCache { get; }

    /// <summary>Resolves the effective <see cref="JsonTypeInfo{T}"/> — the caller's, the store's, or null.</summary>
    protected abstract JsonTypeInfo<T>? ResolveTypeInfo<T>(JsonTypeInfo<T>? provided) where T : class;

    /// <summary>The stored type discriminator for <typeparamref name="T"/>.</summary>
    protected abstract string ResolveDocumentTypeName<T>() where T : class;

    /// <summary>
    /// In-process change notifications for this store. Providers publish through <see cref="PublishChange"/>;
    /// the query layer subscribes through it for per-query <c>NotifyOnChange</c>.
    /// </summary>
    public ChangeBroadcaster Broadcaster { get; } = new();

    /// <summary>
    /// Change notifications buffered for the current unit of work. AsyncLocal (not a plain field) so concurrent
    /// units — and direct writes racing an open unit — each have their own buffer.
    /// </summary>
    protected AsyncLocal<List<Action>?> PendingChanges { get; } = new();

    /// <summary>
    /// Publishes an in-process change notification for observers of <typeparamref name="T"/> — buffered until
    /// commit when a unit of work is open, so a rolled-back unit emits nothing.
    /// </summary>
    protected void PublishChange<T>(DocumentChangeType changeType, string id, T? document) where T : class
    {
        var change = new DocumentChange<T> { ChangeType = changeType, Id = id, Document = document };
        var buffer = this.PendingChanges.Value;
        if (buffer != null)
            buffer.Add(() => this.Broadcaster.Publish(change));
        else if (this.Broadcaster.HasSubscribers<T>())
            this.Broadcaster.Publish(change);
    }

    /// <summary>One document write, after the before-hooks have run.</summary>
    protected sealed class DocumentWrite<T> where T : class
    {
        internal DocumentWrite() { }

        /// <summary>The write context, or null when no interceptor is registered.</summary>
        public DocumentWriteContext? Context { get; internal set; }

        /// <summary>The document to persist — already replaced if an interceptor swapped it.</summary>
        public T? Document { get; internal set; }

        /// <summary>The document to persist, for the operations that always have one.</summary>
        public T Doc => this.Document!;

        public required string TypeName { get; init; }
        public required JsonTypeInfo<T>? TypeInfo { get; init; }
        public required IdAccessor<T> Accessor { get; init; }
        public required VersionMapping? VersionMapping { get; init; }

        /// <summary>False when an interceptor cancelled: perform no write and report <see cref="CancelResult"/>.</summary>
        public bool Proceed { get; internal set; }

        /// <summary>What a cancelled write reports to its caller (<c>Remove</c> returns this).</summary>
        public bool CancelResult => this.Context?.CancelResult ?? true;
    }

    /// <summary>
    /// Opens a single-document write: resolves the type info / id accessor / type name / version mapping,
    /// builds the write context, runs the <c>BeforeWrite</c> chain, and applies a replacement document.
    /// Check <see cref="DocumentWrite{T}.Proceed"/> before persisting anything.
    /// </summary>
    protected async Task<DocumentWrite<T>> BeginWriteAsync<T>(
        DocumentOperation operation,
        T? document,
        object? id,
        JsonTypeInfo<T>? jsonTypeInfo,
        CancellationToken ct,
        Func<object, string>? jsonFactory = null) where T : class
    {
        var typeInfo = this.ResolveTypeInfo(jsonTypeInfo);
        var typeName = this.ResolveDocumentTypeName<T>();
        var write = new DocumentWrite<T>
        {
            TypeName = typeName,
            TypeInfo = typeInfo,
            Accessor = this.IdCache.GetOrCreate(typeInfo),
            VersionMapping = this.Mappings.ResolveVersionMapping(typeof(T)),
            Document = document
        };

        write.Context = this.NewWriteContext(operation, typeName, id, document, jsonFactory);
        write.Proceed = await this.Interceptors.BeforeWrite(write.Context, ct).ConfigureAwait(false);
        if (write.Context?.Document is T replaced)
            write.Document = replaced;
        return write;
    }

    /// <summary>
    /// The id an insert should use: the document's own when set, otherwise a generated one written back onto
    /// the document. String ids are never generated — an unset one is a caller error.
    /// </summary>
    protected string ResolveInsertId<T>(DocumentWrite<T> write, Func<IdAccessor<T>, string> generate) where T : class
    {
        if (!write.Accessor.IsDefaultId(write.Doc))
            return write.Accessor.GetIdAsString(write.Doc);

        RequireGeneratableId<T>(write.Accessor.Kind);
        var id = generate(write.Accessor);
        write.Accessor.SetId(write.Doc, id);
        return id;
    }

    /// <inheritdoc cref="ResolveInsertId{T}(DocumentWrite{T}, Func{IdAccessor{T}, string})"/>
    protected async Task<string> ResolveInsertIdAsync<T>(DocumentWrite<T> write, Func<IdAccessor<T>, Task<string>> generate) where T : class
    {
        if (!write.Accessor.IsDefaultId(write.Doc))
            return write.Accessor.GetIdAsString(write.Doc);

        RequireGeneratableId<T>(write.Accessor.Kind);
        var id = await generate(write.Accessor).ConfigureAwait(false);
        write.Accessor.SetId(write.Doc, id);
        return id;
    }

    static void RequireGeneratableId<T>(IdKind kind)
    {
        if (kind == IdKind.String)
            throw new InvalidOperationException(
                $"Insert requires a non-empty string Id on '{typeof(T).Name}'. " +
                "String Id properties are not auto-generated during Insert.");
    }

    /// <summary>The id an update must have — an unset id is a caller error, not a "not found".</summary>
    protected string RequireDocumentId<T>(DocumentWrite<T> write, string operation = "Update") where T : class
    {
        if (write.Accessor.IsDefaultId(write.Doc))
            throw new InvalidOperationException(
                $"{operation} requires a non-default Id on the document. " +
                $"Set the Id property on '{typeof(T).Name}' before calling {operation}.");
        return write.Accessor.GetIdAsString(write.Doc);
    }

    /// <summary>
    /// Closes a single-document write: runs the <c>AfterWrite</c> chain inside the write, then publishes the
    /// in-process change notification.
    /// </summary>
    protected async Task CompleteWriteAsync<T>(
        DocumentWrite<T> write,
        string id,
        int? version,
        DocumentChangeType changeType,
        T? document,
        CancellationToken ct,
        object? contextId = null) where T : class
    {
        await this.Interceptors.AfterWrite(write.Context, contextId ?? id, version, ct).ConfigureAwait(false);
        this.PublishChange(changeType, id, document);
    }

    // ── Per-document ────────────────────────────────────────────────────
    /// <summary>True when at least one per-document interceptor is registered.</summary>
    protected bool HasPerDocInterceptors => this.Interceptors.HasPerDoc;

    /// <summary>Builds a write context (null when no per-doc interceptors are registered, or under a
    /// suppression scope). Pass <paramref name="jsonFactory"/> (e.g. <c>doc =&gt; Serialize((T)doc, typeInfo,
    /// jsonOptions)</c>) so interceptors can read <c>ctx.GetJson()</c>.</summary>
    protected DocumentWriteContext? NewWriteContext<T>(DocumentOperation op, string typeName, object? id, T? document, Func<object, string>? jsonFactory = null) where T : class
        => this.Interceptors.NewWrite(op, typeName, id, document, (IDocumentStore)this, DocumentOperationScope.CurrentServices, jsonFactory);

    /// <summary>Runs the before-write chain. Returns false when an interceptor cancelled the write — the caller
    /// must skip its write and report <see cref="DocumentWriteContext.CancelResult"/>.</summary>
    protected Task<bool> RunBeforeWriteAsync(DocumentWriteContext? ctx, CancellationToken ct)
        => this.Interceptors.BeforeWrite(ctx, ct);

    protected Task RunAfterWriteAsync(DocumentWriteContext? ctx, object? id, int? version, CancellationToken ct)
        => this.Interceptors.AfterWrite(ctx, id, version, ct);

    /// <summary>Runs per-doc BeforeWrite across a batch (mutating <paramref name="documents"/>); returns index-aligned contexts or null.</summary>
    protected Task<DocumentWriteContext[]?> RunBeforeWriteBatchAsync<T>(IList<T> documents, string typeName, CancellationToken ct, Func<object, string>? jsonFactory = null) where T : class
        => this.Interceptors.BeforeWriteBatch(documents, typeName, (IDocumentStore)this, DocumentOperationScope.CurrentServices, ct, jsonFactory);

    // ── Bulk (set-based) ────────────────────────────────────────────────
    protected DocumentBulkContext? NewBulkContext<T>(DocumentOperation op, string typeName, string? whereClause = null, IReadOnlyList<(string Property, object? Value)>? assignments = null, IDocumentQuery<T>? sourceQuery = null) where T : class
    {
        var ctx = this.Interceptors.NewBulk(op, typeName, whereClause, assignments, sourceQuery);
        // Store lets a cancelling interceptor re-issue the write itself — the only handle it has for a Clear,
        // which has no source query.
        if (ctx != null)
            ctx.Store = (IDocumentStore)this;
        return ctx;
    }

    /// <summary>Runs the before-bulk chain. Returns false when an interceptor cancelled the write — the caller
    /// must skip its write and report <see cref="DocumentBulkContext.CancelAffected"/>.</summary>
    protected Task<bool> RunBeforeBulkAsync(DocumentBulkContext? ctx, CancellationToken ct)
        => this.Interceptors.BeforeBulk(ctx, ct);

    protected Task RunAfterBulkAsync(DocumentBulkContext? ctx, int affected, CancellationToken ct)
        => this.Interceptors.AfterBulk(ctx, affected, ct);
}
