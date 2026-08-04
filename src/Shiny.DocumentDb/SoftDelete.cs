using System.Collections.Concurrent;
using System.Linq.Expressions;

// Inside SoftDeleteExtensions the simple name 'SoftDelete' binds to the extension method, so the type needs
// an alias to stay reachable.
using SoftDeleteMap = Shiny.DocumentDb.SoftDelete;

namespace Shiny.DocumentDb;

/// <summary>
/// Soft delete, built entirely on the public interceptor + query-filter surface — nothing about it is wired into
/// the stores. <c>AddSoftDelete&lt;T&gt;</c> registers two things against a store's options:
/// <list type="bullet">
/// <item>a named query filter (<see cref="FilterName"/>) that hides flagged documents from every read; and</item>
/// <item>an interceptor that cancels each <c>Remove</c> / <c>ExecuteDelete</c> / <c>Clear</c> of
/// <typeparamref name="T"/> and sets the flag instead.</item>
/// </list>
/// The flag property is either a <c>bool</c> (set to <c>true</c> on delete) or a nullable
/// <c>DateTime</c>/<c>DateTimeOffset</c> (set to now, from the ambient <see cref="TimeProvider"/> when one is
/// registered). Use <see cref="SoftDeleteExtensions.IncludeDeleted{T}"/> to read past the filter.
/// </summary>
public static class SoftDelete
{
    /// <summary>The name of the query filter <c>AddSoftDelete</c> registers, so it can be lifted per query with
    /// <c>IgnoreQueryFilters("soft-delete")</c> — which is what <see cref="SoftDeleteExtensions.IncludeDeleted{T}"/>
    /// does.</summary>
    public const string FilterName = "soft-delete";

    // Mapping per document type. Process-wide because the mapping is a property of the document type itself, so
    // the convenience extensions (Restore/PurgeDeleted/OnlyDeleted) can find it from a bare IDocumentStore.
    // Re-registering the same type with the same property is a no-op; with a different property it throws.
    static readonly ConcurrentDictionary<Type, object> mappings = new();

    /// <summary>
    /// Validates <paramref name="flagProperty"/>, registers the soft-delete query filter and interceptors on
    /// <paramref name="options"/>, and records the mapping for the <see cref="SoftDeleteExtensions"/> helpers.
    /// Call <c>AddSoftDelete</c> on your store's options instead.
    /// </summary>
    public static void Configure<T>(IDocumentStoreOptions options, Expression<Func<T, object>> flagProperty) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(flagProperty);

        var mapping = Register(flagProperty);
        var interceptor = new SoftDeleteInterceptor<T>(mapping);
        options.AddInterceptor(interceptor);
        options.AddBulkInterceptor(interceptor);
        options.Mappings.AddQueryFilter(FilterName, mapping.NotDeleted);
    }

    static SoftDeleteMapping<T> Register<T>(Expression<Func<T, object>> flagProperty) where T : class
    {
        var mapping = SoftDeleteMapping<T>.Build(flagProperty);
        var existing = (SoftDeleteMapping<T>)mappings.GetOrAdd(typeof(T), mapping);
        if (!ReferenceEquals(existing, mapping) && existing.PropertyName != mapping.PropertyName)
            throw new InvalidOperationException(
                $"'{typeof(T).Name}' is already mapped for soft delete on '{existing.PropertyName}'; it cannot also be mapped on '{mapping.PropertyName}'. A document type has one soft-delete flag.");
        return existing;
    }

    internal static SoftDeleteMapping<T> Require<T>() where T : class
        => mappings.TryGetValue(typeof(T), out var mapping)
            ? (SoftDeleteMapping<T>)mapping
            : throw new InvalidOperationException(
                $"'{typeof(T).Name}' is not mapped for soft delete. Call options.ConfigureDocument<{typeof(T).Name}>(cfg => cfg.AddSoftDelete(x => x.IsDeleted)) when configuring the store.");

    // A timestamp flag stamps the ambient TimeProvider when the container has one, so tests (and anything else
    // on a controlled clock) get the clock they registered; the system clock otherwise.
    internal static TimeProvider Clock(IServiceProvider services)
        => services.GetService(typeof(TimeProvider)) as TimeProvider ?? TimeProvider.System;
}

/// <summary>Enables soft delete on a document type — one extension for every provider.</summary>
public static class SoftDeleteOptionsExtensions
{
    /// <summary>
    /// Maps a soft-delete flag on <typeparamref name="T"/>: every <c>Remove</c> / <c>ExecuteDelete</c> /
    /// <c>Clear</c> of the type sets <paramref name="flagProperty"/> instead of deleting the document, and a named
    /// query filter (<see cref="SoftDelete.FilterName"/>) hides flagged documents from every read. Pass a
    /// <c>bool</c> property (set to <c>true</c>) or a nullable <c>DateTime</c>/<c>DateTimeOffset</c> (stamped with
    /// now). Read past it with <c>Query&lt;T&gt;().IncludeDeleted()</c>; see <see cref="SoftDeleteExtensions"/>
    /// for <c>Restore</c> / <c>PurgeDeleted</c> / <c>HardDelete</c>.
    /// </summary>
    /// <example>
    /// <code>
    /// options.ConfigureDocument&lt;Customer&gt;(cfg => cfg.AddSoftDelete(x => x.IsDeleted));
    /// </code>
    /// </example>
    public static DocumentTypeBuilder<T> AddSoftDelete<T>(this DocumentTypeBuilder<T> cfg, Expression<Func<T, object>> flagProperty) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        SoftDelete.Configure(cfg.Options, flagProperty);
        return cfg;
    }
}

/// <summary>Read and write helpers for soft-deleted documents.</summary>
public static class SoftDeleteExtensions
{
    /// <summary>Lifts the soft-delete filter for this query so flagged documents come back too. Other query
    /// filters stay in force.</summary>
    public static IDocumentQuery<T> IncludeDeleted<T>(this IDocumentQuery<T> query) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.IgnoreQueryFilters(SoftDeleteMap.FilterName);
    }

    /// <summary>Returns only the soft-deleted documents — the flagged rows the soft-delete filter normally hides.</summary>
    public static IDocumentQuery<T> OnlyDeleted<T>(this IDocumentQuery<T> query) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.IncludeDeleted().Where(SoftDeleteMap.Require<T>().Deleted);
    }

    /// <summary>
    /// Soft-deletes one document by id — sets the mapped flag. This is <c>Remove&lt;T&gt;(id)</c> (the interceptor
    /// converts that too), spelled so the intent is explicit at the call site, and it throws instead of hard
    /// deleting if <typeparamref name="T"/> is not mapped for soft delete. False when no live document matched.
    /// </summary>
    public static Task<bool> SoftDelete<T>(this IDocumentStore store, object id, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        SoftDeleteMap.Require<T>();
        return store.Remove<T>(id, cancellationToken);
    }

    /// <summary>
    /// Clears the soft-delete flag on every document matching <paramref name="predicate"/>, and returns how many
    /// were restored. Takes a predicate rather than an id because a flagged document is invisible to a by-id
    /// write — the query filter hides it — so the restore has to run through
    /// <see cref="IncludeDeleted{T}"/>: <c>store.Restore&lt;Customer&gt;(x =&gt; x.Id == id)</c>.
    /// </summary>
    public static Task<int> Restore<T>(this IDocumentStore store, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(predicate);
        var mapping = SoftDeleteMap.Require<T>();
        return store.Query<T>().IncludeDeleted().Where(predicate).ExecuteUpdate(mapping.Property, mapping.RestoreValue, cancellationToken);
    }

    /// <summary>
    /// Permanently deletes soft-deleted documents — the real <c>DELETE</c> the interceptor normally replaces.
    /// Pass <paramref name="predicate"/> to purge a subset (e.g. <c>x =&gt; x.Id == id</c>, or everything flagged
    /// before a cutoff); omit it to purge every flagged document of the type. Live documents are never touched.
    /// </summary>
    public static async Task<int> PurgeDeleted<T>(this IDocumentStore store, Expression<Func<T, bool>>? predicate = null, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        var mapping = SoftDeleteMap.Require<T>();
        var query = store.Query<T>().IncludeDeleted().Where(mapping.Deleted);
        if (predicate != null)
            query = query.Where(predicate);
        // Suppressed so the soft-delete interceptor does not convert this delete back into a flag update. Awaited
        // inside the scope — returning the task would pop the suppression before the delete runs.
        using var suppression = store.SuppressInterceptors();
        return await query.ExecuteDelete(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Permanently deletes one live document by id, bypassing soft delete. An already-flagged document is hidden
    /// by the query filter and cannot be reached by id — purge it with <see cref="PurgeDeleted{T}"/> instead.
    /// </summary>
    public static async Task<bool> HardDelete<T>(this IDocumentStore store, object id, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        using var suppression = store.SuppressInterceptors();
        return await store.Remove<T>(id, cancellationToken).ConfigureAwait(false);
    }
}

// The bool / nullable-timestamp flavors of a soft-delete flag, plus the predicates the query filter and the
// Restore/Purge helpers need. Built once per document type at registration.
sealed class SoftDeleteMapping<T> where T : class
{
    SoftDeleteMapping(
        string propertyName,
        Expression<Func<T, object>> property,
        Expression<Func<T, bool>> notDeleted,
        Expression<Func<T, bool>> deleted,
        bool isTimestamp,
        bool timestampIsOffset,
        object? restoreValue)
    {
        this.PropertyName = propertyName;
        this.Property = property;
        this.NotDeleted = notDeleted;
        this.Deleted = deleted;
        this.isTimestamp = isTimestamp;
        this.timestampIsOffset = timestampIsOffset;
        this.RestoreValue = restoreValue;
    }

    readonly bool isTimestamp;
    readonly bool timestampIsOffset;

    public string PropertyName { get; }
    public Expression<Func<T, object>> Property { get; }
    public Expression<Func<T, bool>> NotDeleted { get; }
    public Expression<Func<T, bool>> Deleted { get; }
    public object? RestoreValue { get; }

    /// <summary>The value written to the flag on delete — <c>true</c>, or "now" for a timestamp flag.</summary>
    public object DeletedValue(TimeProvider timeProvider)
    {
        if (!this.isTimestamp)
            return true;
        var now = timeProvider.GetUtcNow();
        return this.timestampIsOffset ? now : now.UtcDateTime;
    }

    public static SoftDeleteMapping<T> Build(Expression<Func<T, object>> flagProperty)
    {
        // x => x.IsDeleted arrives as a Convert(member, object) because of the object-typed lambda.
        var body = flagProperty.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            body = unary.Operand;
        if (body is not MemberExpression member)
            throw new ArgumentException(
                $"The soft-delete flag must be a simple property access on '{typeof(T).Name}' (e.g. x => x.IsDeleted).",
                nameof(flagProperty));

        var parameter = flagProperty.Parameters[0];
        var type = member.Type;

        if (type == typeof(bool))
            return new SoftDeleteMapping<T>(
                member.Member.Name,
                flagProperty,
                Expression.Lambda<Func<T, bool>>(Expression.Not(member), parameter),
                Expression.Lambda<Func<T, bool>>(member, parameter),
                isTimestamp: false,
                timestampIsOffset: false,
                restoreValue: false);

        if (type == typeof(DateTime?) || type == typeof(DateTimeOffset?))
        {
            var nullConstant = Expression.Constant(null, type);
            return new SoftDeleteMapping<T>(
                member.Member.Name,
                flagProperty,
                Expression.Lambda<Func<T, bool>>(Expression.Equal(member, nullConstant), parameter),
                Expression.Lambda<Func<T, bool>>(Expression.NotEqual(member, nullConstant), parameter),
                isTimestamp: true,
                timestampIsOffset: type == typeof(DateTimeOffset?),
                restoreValue: null);
        }

        throw new ArgumentException(
            $"Soft delete supports a bool flag or a nullable DateTime/DateTimeOffset timestamp; '{typeof(T).Name}.{member.Member.Name}' is a {type.Name}.",
            nameof(flagProperty));
    }
}

/// <summary>
/// Converts deletes of <typeparamref name="T"/> into flag updates. Registered as both a per-document and a
/// set-based interceptor: <c>Remove</c> becomes a <c>SetProperty</c>, and <c>ExecuteDelete</c> / <c>Clear</c>
/// become an <c>ExecuteUpdate</c> over the same predicate. Runs last so other interceptors still observe the
/// delete before it is replaced.
/// </summary>
sealed class SoftDeleteInterceptor<T> : IDocumentInterceptor, IDocumentBulkInterceptor where T : class
{
    readonly SoftDeleteMapping<T> mapping;

    public SoftDeleteInterceptor(SoftDeleteMapping<T> mapping) => this.mapping = mapping;

    public int Order => int.MaxValue;

    public async Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
    {
        if (ctx.Operation != DocumentOperation.Delete || ctx.DocumentType != typeof(T))
            return;

        // ctx.Store is the operation-scoped (transaction-bound) store, so the flag update commits with whatever
        // unit the Remove was part of. An already-flagged document is hidden by the soft-delete query filter, so
        // SetProperty matches nothing and the Remove correctly reports false.
        var value = this.mapping.DeletedValue(SoftDeleteMap.Clock(ctx.Services));
        var updated = await ctx.Store
            .SetProperty(ctx.Id!, this.mapping.Property, value, null, ct)
            .ConfigureAwait(false);
        ctx.Cancel(updated);
    }

    public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;

    public async Task BeforeBulkWrite(DocumentBulkContext ctx, CancellationToken ct)
    {
        if (ctx.DocumentType != typeof(T))
            return;
        if (ctx.Operation is not (DocumentOperation.Delete or DocumentOperation.Clear))
            return;

        // A query-driven ExecuteDelete re-runs as an ExecuteUpdate over the very same query (predicate and
        // filters intact); a Clear has no source query, so it updates every live document of the type.
        var query = ctx.QueryAs<T>() ?? ctx.Store?.Query<T>()
            ?? throw new NotSupportedException(
                $"Soft delete could not replace a set-based {ctx.Operation} of '{ctx.TypeName}': this store exposes neither the source query nor a store on the interceptor context.");

        var value = this.mapping.DeletedValue(SoftDeleteMap.Clock(ctx.Services));
        var affected = await query.ExecuteUpdate(this.mapping.Property, value, ct).ConfigureAwait(false);
        ctx.Cancel(affected);
    }

    public Task AfterBulkWrite(DocumentBulkContext ctx, CancellationToken ct) => Task.CompletedTask;
}
