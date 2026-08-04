using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Diagnostics;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb;

/// <summary>
/// Everything the shared query machinery needs from a provider, handed over once when the query is created.
/// </summary>
public sealed class DocumentQueryContext<T> where T : class
{
    /// <summary>The stored type discriminator for <typeparamref name="T"/>.</summary>
    public required string TypeName { get; init; }

    /// <summary>Telemetry for the query terminals — one per store, so span/metric names stay stable.</summary>
    public required OperationTracker Tracker { get; init; }

    /// <summary>The store's interceptor pipeline; the base runs the bulk hooks for <c>ExecuteDelete</c>/<c>ExecuteUpdate</c>.</summary>
    public required InterceptorPipeline Interceptors { get; init; }

    /// <summary>The store's serializer options — used to resolve a property's JSON path.</summary>
    public required JsonSerializerOptions JsonOptions { get; init; }

    /// <summary>The type info for <typeparamref name="T"/>, when the caller or the store supplied one.</summary>
    public JsonTypeInfo<T>? TypeInfo { get; init; }

    /// <summary>Registered global query filters, as (name, predicate) — the name is what
    /// <c>IgnoreQueryFilters(names)</c> matches on.</summary>
    public IReadOnlyList<(string? Name, Expression<Func<T, bool>> Predicate)> Filters { get; init; } = [];

    /// <summary>Populates a materialized document's computed properties, when the type maps any.</summary>
    public Action<T>? ApplyComputed { get; init; }

    /// <summary>Reads a document's id as a string — required for cursor pagination.</summary>
    public Func<T, string>? GetId { get; init; }

    // Core-only: the alias → mapping lookup the string projection needs. Providers inside this repo set it;
    // an external provider leaves it null and gets projection over the plain JSON properties.
    internal IReadOnlyDictionary<string, ComputedMapping>? ComputedLookup { get; init; }
}

/// <summary>
/// The provider-agnostic half of <see cref="IDocumentQuery{T}"/>: builder state and immutability, query-filter
/// resolution, every client-side terminal, and the interceptor plumbing for set-based writes. A provider
/// supplies a <see cref="Clone"/>, an <see cref="ExecuteAsync"/> that turns a <see cref="QueryPlan{T}"/> into
/// rows, and the two set-based write primitives; everything else is inherited.
/// <para>
/// Push-down is preserved: a provider that filters, orders, and pages server-side returns
/// <see cref="QueryExecution{T}.Complete"/> and nothing is redone client-side. A provider that can only narrow
/// candidates returns <see cref="QueryExecution{T}.Candidates"/> and the base finishes the job.
/// </para>
/// </summary>
public abstract class DocumentQueryBase<T> : IDocumentQuery<T> where T : class
{
    readonly List<Expression<Func<T, bool>>> predicates = new();
    readonly List<(LambdaExpression Selector, bool Descending)> orderBys = new();
    int? skipCount;
    int? takeCount;
    bool ignoreAllFilters;
    HashSet<string>? ignoredFilterNames;

    protected DocumentQueryBase(DocumentQueryContext<T> context)
        => this.Context = context ?? throw new ArgumentNullException(nameof(context));

    /// <summary>Copy constructor — a provider's <see cref="Clone"/> must chain to this so builder state carries over.</summary>
    protected DocumentQueryBase(DocumentQueryBase<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        this.Context = source.Context;
        this.predicates.AddRange(source.predicates);
        this.orderBys.AddRange(source.orderBys);
        this.skipCount = source.skipCount;
        this.takeCount = source.takeCount;
        this.ignoreAllFilters = source.ignoreAllFilters;
        this.ignoredFilterNames = source.ignoredFilterNames is null
            ? null
            : new HashSet<string>(source.ignoredFilterNames, StringComparer.Ordinal);
    }

    protected DocumentQueryContext<T> Context { get; }

    protected JsonTypeInfo<T>? TypeInfo => this.Context.TypeInfo;

    /// <summary>The requested ordering — for a provider that pushes ordering (or cursor keys) down itself.</summary>
    protected IReadOnlyList<(LambdaExpression Selector, bool Descending)> Ordering => this.orderBys;

    /// <summary>The paging window, for a provider that renders it into its own query text.</summary>
    protected int? SkipCount => this.skipCount;

    /// <summary>The paging window, for a provider that renders it into its own query text.</summary>
    protected int? TakeCount => this.takeCount;

    public JsonTypeInfo<T>? QueryTypeInfo => this.Context.TypeInfo;

    // ── Provider hooks ──────────────────────────────────────────────────

    /// <summary>Returns a copy of this query, chaining to the copy constructor. Every builder call clones, so a
    /// query never mutates once handed out.</summary>
    protected abstract DocumentQueryBase<T> Clone();

    /// <summary>Runs the plan against the store and reports how much of it was satisfied server-side.</summary>
    protected abstract Task<QueryExecution<T>> ExecuteAsync(QueryPlan<T> plan, CancellationToken ct);

    /// <summary>Deletes the documents matching the plan's predicates; returns how many. Ordering and paging do
    /// not apply.</summary>
    protected abstract Task<int> DeleteMatchingAsync(QueryPlan<T> plan, CancellationToken ct);

    /// <summary>Sets one JSON property on every document matching the plan's predicates; returns how many.</summary>
    protected abstract Task<int> SetPropertyMatchingAsync(QueryPlan<T> plan, string jsonPath, object? value, CancellationToken ct);

    /// <summary>
    /// Sets several JSON properties on every document matching the plan's predicates; returns how many.
    /// The default applies them one at a time through <see cref="SetPropertyMatchingAsync"/> — correct, but one
    /// statement per assignment. A provider whose engine can set several paths at once overrides this so the
    /// whole update is a single statement.
    /// </summary>
    protected virtual async Task<int> SetPropertiesMatchingAsync(QueryPlan<T> plan, IReadOnlyList<(string JsonPath, object? Value)> assignments, CancellationToken ct)
    {
        var affected = 0;
        for (var i = 0; i < assignments.Count; i++)
        {
            var updated = await this.SetPropertyMatchingAsync(plan, assignments[i].JsonPath, assignments[i].Value, ct).ConfigureAwait(false);
            if (i == 0)
                affected = updated;   // same predicate every pass, so the first count is the row count
        }
        return affected;
    }

    /// <summary>Optional: the change stream this query filters. Providers without change monitoring leave it.</summary>
    protected virtual IAsyncEnumerable<DocumentChange<T>> ObserveChanges(CancellationToken ct)
        => throw new NotSupportedException($"Change monitoring is not supported by this store for '{this.Context.TypeName}'.");

    /// <summary>Optional: full-text search, pre-filtered by this query's predicates.</summary>
    protected virtual Task<IReadOnlyList<FullTextResult<T>>> FullTextSearchCore(string searchText, int maxResults, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => throw new NotSupportedException($"Full-text search is not supported by this store for '{this.Context.TypeName}'.");

    /// <summary>Optional: vector search, pre-filtered by this query's predicates.</summary>
    protected virtual Task<IReadOnlyList<VectorResult<T>>> NearestVectorsCore(ReadOnlyMemory<float> query, int k, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => throw new NotSupportedException($"Vector search is not supported by this store for '{this.Context.TypeName}'.");

    // Aggregate hooks — the defaults materialize and compute client-side. A provider whose engine can do the
    // aggregate server-side (Cosmos DB, MongoDB) overrides these; everyone else inherits.
    protected virtual async Task<long> CountCore(QueryPlan<T> plan, CancellationToken ct)
        => (await this.MaterializeAsync(plan, ct).ConfigureAwait(false)).LongCount();

    protected virtual async Task<bool> AnyCore(QueryPlan<T> plan, CancellationToken ct)
        => (await this.MaterializeAsync(plan, ct).ConfigureAwait(false)).Any();

    protected virtual async Task<TValue> MaxCore<TValue>(QueryPlan<T> plan, Expression<Func<T, TValue>> selector, CancellationToken ct)
        => (await this.MaterializeAsync(plan, ct).ConfigureAwait(false)).Max(ExpressionInterpreter.Interpret(selector))!;

    protected virtual async Task<TValue> MinCore<TValue>(QueryPlan<T> plan, Expression<Func<T, TValue>> selector, CancellationToken ct)
        => (await this.MaterializeAsync(plan, ct).ConfigureAwait(false)).Min(ExpressionInterpreter.Interpret(selector))!;

    protected virtual async Task<TValue> SumCore<TValue>(QueryPlan<T> plan, Expression<Func<T, TValue>> selector, CancellationToken ct)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var rows = await this.MaterializeAsync(plan, ct).ConfigureAwait(false);
        return rows.Select(compiled).Aggregate(default(TValue)!, DynamicAdd);
    }

    protected virtual async Task<double> AverageCore(QueryPlan<T> plan, Expression<Func<T, object>> selector, CancellationToken ct)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var rows = await this.MaterializeAsync(plan, ct).ConfigureAwait(false);
        return rows.Average(x => Convert.ToDouble(compiled(x)));
    }

    /// <summary>The engine-level query text, for providers that can render one.</summary>
    public virtual DocumentQueryString ToQueryString()
        => throw new NotSupportedException("ToQueryString is not supported by this provider.");

    // ── Plan construction ───────────────────────────────────────────────

    /// <summary>The effective predicates: registered filters that were not ignored, then the caller's <c>Where</c>s.</summary>
    protected IReadOnlyList<Expression<Func<T, bool>>> EffectivePredicates()
    {
        var effective = new List<Expression<Func<T, bool>>>(this.Context.Filters.Count + this.predicates.Count);
        if (!this.ignoreAllFilters)
        {
            foreach (var (name, predicate) in this.Context.Filters)
            {
                if (name != null && this.ignoredFilterNames?.Contains(name) == true)
                    continue;
                effective.Add(predicate);
            }
        }
        effective.AddRange(this.predicates);

        // Cross-cutting features that must transform the caller's expression (field-level encryption rewrites a
        // comparison constant into its ciphertext) get their pass here, so every provider inherits it.
        DocumentPredicateRewriters.ApplyAll(effective);
        return effective;
    }

    /// <summary>The full plan: predicates, ordering, and paging.</summary>
    protected QueryPlan<T> BuildPlan()
        => new(this.EffectivePredicates(), this.orderBys, this.skipCount, this.takeCount);

    /// <summary>
    /// The full plan with the row limit narrowed to <paramref name="takeOverride"/> — what the single-row
    /// terminals run, so a provider that pages server-side fetches one (or two) rows instead of the match set.
    /// An existing, smaller <c>Paginate</c> window wins; the skip is always preserved.
    /// </summary>
    protected QueryPlan<T> BuildPlan(int takeOverride)
        => new(
            this.EffectivePredicates(),
            this.orderBys,
            this.skipCount,
            this.takeCount is { } take && take < takeOverride ? take : takeOverride);

    /// <summary>The plan without ordering or paging — what an aggregate or a grouped query runs over.</summary>
    protected QueryPlan<T> BuildPredicatePlan()
        => new(this.EffectivePredicates(), [], null, null);

    /// <summary>Runs the plan and applies whatever the provider left to the client, in the order
    /// predicates → computed properties → ordering → paging.</summary>
    protected async Task<IEnumerable<T>> MaterializeAsync(QueryPlan<T> plan, CancellationToken ct)
    {
        var execution = await this.ExecuteAsync(plan, ct).ConfigureAwait(false);
        IEnumerable<T> rows = execution.Rows;

        var applyComputed = this.Context.ApplyComputed;
        if (applyComputed != null)
        {
            rows = rows.Select(d =>
            {
                applyComputed(d);
                return d;
            });
        }

        if (!execution.PredicatesApplied && plan.HasPredicates)
            rows = rows.Where(plan.CompilePredicate());

        if (!execution.OrderingApplied && plan.HasOrdering)
        {
            IOrderedEnumerable<T>? ordered = null;
            foreach (var (selector, descending) in plan.OrderBy)
            {
                var key = ExpressionInterpreter.Interpret<T, object>((Expression<Func<T, object>>)selector);
                ordered = ordered == null
                    ? (descending ? rows.OrderByDescending(key) : rows.OrderBy(key))
                    : (descending ? ordered.ThenByDescending(key) : ordered.ThenBy(key));
            }
            rows = ordered!;
        }

        if (!execution.PagingApplied)
        {
            if (plan.Skip is { } skip)
                rows = rows.Skip(skip);
            if (plan.Take is { } take)
                rows = rows.Take(take);
        }

        return rows;
    }

    // ── Builder (immutable — every call clones) ─────────────────────────

    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        var clone = this.Clone();
        clone.predicates.Add(predicate);
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters()
    {
        var clone = this.Clone();
        clone.ignoreAllFilters = true;
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames)
    {
        ArgumentNullException.ThrowIfNull(filterNames);
        var clone = this.Clone();
        clone.ignoredFilterNames ??= new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in filterNames)
            if (!String.IsNullOrWhiteSpace(name))
                clone.ignoredFilterNames.Add(name);
        return clone;
    }

    public IDocumentQuery<T> OrderBy(Expression<Func<T, object>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var clone = this.Clone();
        clone.orderBys.Add((selector, false));
        return clone;
    }

    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var clone = this.Clone();
        clone.orderBys.Add((selector, true));
        return clone;
    }

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = this.Clone();
        clone.skipCount = offset;
        clone.takeCount = take;
        return clone;
    }

    public virtual IGroupedDocumentQuery<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        return new InMemoryGroupedQuery<T, TKey>(
            () => this.MaterializeAsync(this.BuildPredicatePlan(), CancellationToken.None).GetAwaiter().GetResult(),
            ExpressionInterpreter.Interpret(keySelector));
    }

    public virtual IGroupedDocumentQuery<T, object> GroupBy(string keyField)
        => throw new NotSupportedException(
            "String GroupBy(\"field\") is not supported on this provider. Use the typed GroupBy(keySelector) form, " +
            "or a relational store for the string grammar.");

    public virtual IDocumentQuery<JsonObject> Project(string fields, JsonTypeInfo<T>? jsonTypeInfo = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fields);
        var info = jsonTypeInfo ?? this.Context.TypeInfo
            ?? throw new InvalidOperationException(
                $"No JsonTypeInfo<{typeof(T).Name}> could be resolved for the projection. Pass one explicitly or register a JsonSerializerContext.");
        return new StringProjectionQuery<T>(this, StringProjection.BuildGetters(fields, info, this.Context.ComputedLookup));
    }

    public virtual IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<T, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
    {
        ArgumentNullException.ThrowIfNull(selector);
        return new MaterializedProjectedQuery<T, TResult>(
            ct => this.MaterializeAsync(this.BuildPlan(), ct),
            ExpressionInterpreter.Interpret(selector),
            this.Context.Tracker);
    }

    // ── Terminals ───────────────────────────────────────────────────────

    public Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
        => this.Context.Tracker.Track("query.to_list", typeof(T).Name, async () =>
        {
            var rows = await this.MaterializeAsync(this.BuildPlan(), ct).ConfigureAwait(false);
            return (IReadOnlyList<T>)rows.ToList().AsReadOnly();
        }, r => r.Count);

    public async IAsyncEnumerable<T> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        var rows = await this.MaterializeAsync(this.BuildPlan(), ct).ConfigureAwait(false);
        foreach (var row in rows)
        {
            ct.ThrowIfCancellationRequested();
            yield return row;
        }
    }

    public Task<long> Count(CancellationToken ct = default)
        => this.Context.Tracker.Track("query.count", typeof(T).Name, () => this.CountCore(this.BuildPlan(), ct), r => r);

    public Task<bool> Any(CancellationToken ct = default)
        => this.Context.Tracker.Track("query.any", typeof(T).Name, () => this.AnyCore(this.BuildPlan(), ct), r => r ? 1 : 0);

    public virtual Task<T?> FirstOrDefault(CancellationToken ct = default)
        => this.Context.Tracker.Track("query.first", typeof(T).Name, async () =>
        {
            var rows = await this.MaterializeAsync(this.BuildPlan(1), ct).ConfigureAwait(false);
            return rows.FirstOrDefault();
        }, r => r == null ? 0 : 1);

    public virtual async Task<T> First(CancellationToken ct = default)
        => await this.FirstOrDefault(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"No '{typeof(T).Name}' matched the query.");

    // Two rows are fetched so "more than one" is detectable without a second round trip for a count.
    public virtual Task<T?> SingleOrDefault(CancellationToken ct = default)
        => this.Context.Tracker.Track("query.single", typeof(T).Name, async () =>
        {
            var rows = await this.MaterializeAsync(this.BuildPlan(2), ct).ConfigureAwait(false);
            var found = rows.Take(2).ToList();
            if (found.Count > 1)
                throw new InvalidOperationException($"More than one '{typeof(T).Name}' matched the query.");
            return found.Count == 0 ? null : found[0];
        }, r => r == null ? 0 : 1);

    public virtual async Task<T> Single(CancellationToken ct = default)
        => await this.SingleOrDefault(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"No '{typeof(T).Name}' matched the query.");

    public Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.Context.Tracker.Track("query.max", typeof(T).Name, () => this.MaxCore(this.BuildPlan(), selector, ct));

    public Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.Context.Tracker.Track("query.min", typeof(T).Name, () => this.MinCore(this.BuildPlan(), selector, ct));

    public Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.Context.Tracker.Track("query.sum", typeof(T).Name, () => this.SumCore(this.BuildPlan(), selector, ct));

    public Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
        => this.Context.Tracker.Track("query.average", typeof(T).Name, () => this.AverageCore(this.BuildPlan(), selector, ct));

    public virtual Task<CursorPage<T>> ToCursorPage(string? cursor, int take, CancellationToken ct = default)
        => this.Context.Tracker.Track("query.paginate", typeof(T).Name, () => this.CursorPageCore(cursor, take, ct), r => r.Items.Count);

    async Task<CursorPage<T>> CursorPageCore(string? cursor, int take, CancellationToken ct)
    {
        var getId = this.Context.GetId
            ?? throw new NotSupportedException("Cursor pagination is not supported by this provider.");

        var keys = new List<CursorSortKey<T>>(this.orderBys.Count);
        var specParts = new List<string>(this.orderBys.Count);
        foreach (var (selector, descending) in this.orderBys)
        {
            keys.Add(new CursorSortKey<T>(ExpressionInterpreter.Interpret<T, object>((Expression<Func<T, object>>)selector), descending));
            specParts.Add($"{selector}:{(descending ? "d" : "a")}");
        }

        var rows = await this.MaterializeAsync(this.BuildPredicatePlan(), ct).ConfigureAwait(false);
        return InMemoryCursorPager.Page(
            rows,
            keys,
            getId,
            this.Context.TypeName + "|" + String.Join("|", specParts),
            cursor,
            take);
    }

    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange(CancellationToken ct = default)
    {
        var plan = this.BuildPredicatePlan();
        var predicate = plan.HasPredicates ? plan.CompilePredicate() : null;
        return FilterChanges(this.ObserveChanges(ct), predicate, ct);
    }

    public Task<IReadOnlyList<FullTextResult<T>>> FullTextMatch(string searchText, int maxResults = 50, CancellationToken ct = default)
        => this.FullTextSearchCore(searchText, maxResults, this.BuildPredicatePlan().CombinedPredicate(), ct);

    public Task<IReadOnlyList<VectorResult<T>>> NearestVectors(ReadOnlyMemory<float> query, int k, CancellationToken ct = default)
        => this.NearestVectorsCore(query, k, this.BuildPredicatePlan().CombinedPredicate(), ct);

    // ── Set-based writes (interceptor plumbing lives here, once) ────────

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => this.Context.Tracker.Track("query.execute_delete", typeof(T).Name, () => this.ExecuteDeleteCore(ct), r => r);

    async Task<int> ExecuteDeleteCore(CancellationToken ct)
    {
        var interceptors = this.Context.Interceptors;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Delete, this.Context.TypeName, sourceQuery: this);
        if (!await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false))
            return bulkCtx!.CancelAffected;

        var affected = await this.DeleteMatchingAsync(this.BuildPredicatePlan(), ct).ConfigureAwait(false);

        await interceptors.AfterBulk(bulkCtx, affected, ct).ConfigureAwait(false);
        return affected;
    }

    public Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
        => this.Context.Tracker.Track("query.execute_update", typeof(T).Name, () => this.ExecuteUpdateCore(property, value, ct), r => r);

    async Task<int> ExecuteUpdateCore(Expression<Func<T, object>> property, object? value, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(property);
        return await this.ExecuteAssignmentsCore([(this.ResolveJsonPath(property), value)], ct).ConfigureAwait(false);
    }

    public virtual Task<int> ExecuteUpdate(Action<IDocumentUpdateBuilder<T>> build, CancellationToken ct = default)
    {
        var assignments = Internal.DocumentUpdateBuilder<T>.Collect(build);
        var resolved = new (string JsonPath, object? Value)[assignments.Count];
        for (var i = 0; i < assignments.Count; i++)
            resolved[i] = (this.ResolveJsonPath(assignments[i].Property), assignments[i].Value);
        Internal.DocumentUpdateBuilder<T>.RejectDuplicatePaths(resolved);

        return this.Context.Tracker.Track("query.execute_update", typeof(T).Name, () => this.ExecuteAssignmentsCore(resolved, ct), r => r);
    }

    async Task<int> ExecuteAssignmentsCore(IReadOnlyList<(string JsonPath, object? Value)> assignments, CancellationToken ct)
    {
        var interceptors = this.Context.Interceptors;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Update, this.Context.TypeName, assignments: assignments, sourceQuery: this);
        if (!await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false))
            return bulkCtx!.CancelAffected;

        var affected = await this.SetPropertiesMatchingAsync(this.BuildPredicatePlan(), assignments, ct).ConfigureAwait(false);

        await interceptors.AfterBulk(bulkCtx, affected, ct).ConfigureAwait(false);
        return affected;
    }

    /// <summary>Resolves a property selector to its stored JSON path, honoring the store's naming policy.</summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    protected string ResolveJsonPath(Expression<Func<T, object>> property)
        => this.Context.TypeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, this.Context.JsonOptions, this.Context.TypeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, this.Context.JsonOptions);

    static async IAsyncEnumerable<DocumentChange<T>> FilterChanges(
        IAsyncEnumerable<DocumentChange<T>> source,
        Func<T, bool>? predicate,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var change in source.WithCancellation(ct).ConfigureAwait(false))
        {
            // A property-level change carries no document — pass it through and let the consumer re-check.
            if (predicate == null || change.Document is null || predicate(change.Document))
                yield return change;
        }
    }

    static TValue DynamicAdd<TValue>(TValue a, TValue b) => a switch
    {
        int ai when b is int bi => (TValue)(object)(ai + bi),
        long al when b is long bl => (TValue)(object)(al + bl),
        double ad when b is double bd => (TValue)(object)(ad + bd),
        decimal am when b is decimal bm => (TValue)(object)(am + bm),
        float af when b is float bf => (TValue)(object)(af + bf),
        _ => throw new NotSupportedException($"Sum is not supported for type {typeof(TValue).Name}")
    };
}
