using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Redis;

/// <summary>
/// <see cref="IDocumentQuery{T}"/> for Redis Stack. Candidate documents are resolved by an <c>FT.SEARCH</c>
/// pushdown over declared index fields (or a full key scan when nothing pushes down), then predicates,
/// ordering, pagination, projections, and aggregates run in memory via <see cref="ExpressionInterpreter"/>.
/// The full predicate is always re-applied client-side, so pushdown only shrinks the candidate set.
/// </summary>
public class RedisDocumentQuery<T> : IDocumentQuery<T> where T : class
{
    readonly RedisDocumentStore store;
    readonly JsonTypeInfo<T>? typeInfo;

    public JsonTypeInfo<T>? QueryTypeInfo => this.typeInfo;
    readonly List<Expression<Func<T, bool>>> predicates = new();
    readonly List<(LambdaExpression Selector, bool Descending)> orderBys = new();
    int? skipCount;
    int? takeCount;
    bool ignoreAllFilters;
    HashSet<string>? ignoredFilterNames;

    internal RedisDocumentStore Store => this.store;

    internal RedisDocumentQuery(RedisDocumentStore store, JsonTypeInfo<T>? typeInfo)
    {
        this.store = store;
        this.typeInfo = typeInfo;
    }

    RedisDocumentQuery(RedisDocumentQuery<T> source)
    {
        this.store = source.store;
        this.typeInfo = source.typeInfo;
        this.predicates.AddRange(source.predicates);
        this.orderBys.AddRange(source.orderBys);
        this.skipCount = source.skipCount;
        this.takeCount = source.takeCount;
        this.ignoreAllFilters = source.ignoreAllFilters;
        this.ignoredFilterNames = source.ignoredFilterNames is null
            ? null
            : new HashSet<string>(source.ignoredFilterNames, StringComparer.Ordinal);
    }

    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate)
    {
        var clone = new RedisDocumentQuery<T>(this);
        clone.predicates.Add(predicate);
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters()
    {
        var clone = new RedisDocumentQuery<T>(this);
        clone.ignoreAllFilters = true;
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames)
    {
        ArgumentNullException.ThrowIfNull(filterNames);
        var clone = new RedisDocumentQuery<T>(this);
        clone.ignoredFilterNames ??= new HashSet<string>(StringComparer.Ordinal);
        foreach (var n in filterNames)
            if (!string.IsNullOrWhiteSpace(n))
                clone.ignoredFilterNames.Add(n);
        return clone;
    }

    internal IEnumerable<Expression<Func<T, bool>>> GetEffectivePredicateExpressions()
    {
        if (!this.ignoreAllFilters)
        {
            foreach (var f in this.store.Options.ResolveQueryFilters(typeof(T)))
            {
                if (f.Name == null || this.ignoredFilterNames?.Contains(f.Name) != true)
                    yield return (Expression<Func<T, bool>>)f.Predicate;
            }
        }
        foreach (var p in this.predicates)
            yield return p;
    }

    public IDocumentQuery<T> OrderBy(Expression<Func<T, object>> selector)
    {
        var clone = new RedisDocumentQuery<T>(this);
        clone.orderBys.Add((selector, false));
        return clone;
    }

    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector)
    {
        var clone = new RedisDocumentQuery<T>(this);
        clone.orderBys.Add((selector, true));
        return clone;
    }

    public IGroupedDocumentQuery<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        => new InMemoryGroupedQuery<T, TKey>(this.MaterializeForGroupingAsync, ExpressionInterpreter.Interpret(keySelector));

    public IGroupedDocumentQuery<T, object> GroupBy(string keyField)
        => throw new NotSupportedException(
            "String GroupBy(\"field\") is not supported on the Redis provider. Use the typed GroupBy(keySelector) form.");

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = new RedisDocumentQuery<T>(this);
        clone.skipCount = offset;
        clone.takeCount = take;
        return clone;
    }

    public IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<T, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
        => new RedisProjectedDocumentQuery<T, TResult>(this, selector);

    public Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
        => this.store.Tracker.Track("query.to_list", typeof(T).Name, () => this.ToListImpl(ct), r => r.Count);

    async Task<IReadOnlyList<T>> ToListImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).ToList().AsReadOnly();

    public async IAsyncEnumerable<T> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in await this.MaterializeAsync(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public Task<long> Count(CancellationToken ct = default)
        => this.store.Tracker.Track("query.count", typeof(T).Name, () => this.CountImpl(ct), r => r);

    async Task<long> CountImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Count();

    public Task<bool> Any(CancellationToken ct = default)
        => this.store.Tracker.Track("query.any", typeof(T).Name, () => this.AnyImpl(ct), r => r ? 1 : 0);

    async Task<bool> AnyImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Any();

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => this.store.Tracker.Track("query.execute_delete", typeof(T).Name, () => this.ExecuteDeleteImpl(ct), r => r);

    async Task<int> ExecuteDeleteImpl(CancellationToken ct)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var interceptors = this.store.Options.Interceptors;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Delete, typeName, sourceQuery: this);
        if (!await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false))
            return bulkCtx!.CancelAffected;

        var count = await this.store.DeleteWhereAsync(this.GetEffectivePredicateExpressions().ToList(), this.typeInfo, ct).ConfigureAwait(false);

        await interceptors.AfterBulk(bulkCtx, count, ct).ConfigureAwait(false);
        return count;
    }

    public Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
        => this.store.Tracker.Track("query.execute_update", typeof(T).Name, () => this.ExecuteUpdateImpl(property, value, ct), r => r);

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    async Task<int> ExecuteUpdateImpl(Expression<Func<T, object>> property, object? value, CancellationToken ct)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var jsonPath = this.typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions, this.typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions);

        var interceptors = this.store.Options.Interceptors;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Update, typeName, assignment: (jsonPath, value), sourceQuery: this);
        if (!await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false))
            return bulkCtx!.CancelAffected;

        var count = await this.store.UpdatePropertyWhereAsync(this.GetEffectivePredicateExpressions().ToList(), jsonPath, value, this.typeInfo, ct).ConfigureAwait(false);

        await interceptors.AfterBulk(bulkCtx, count, ct).ConfigureAwait(false);
        return count;
    }

    public Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.max", typeof(T).Name, () => this.MaxImpl(selector, ct));

    async Task<TValue> MaxImpl<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Max(ExpressionInterpreter.Interpret(selector))!;

    public Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.min", typeof(T).Name, () => this.MinImpl(selector, ct));

    async Task<TValue> MinImpl<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Min(ExpressionInterpreter.Interpret(selector))!;

    public Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.sum", typeof(T).Name, () => this.SumImpl(selector, ct));

    async Task<TValue> SumImpl<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var items = await this.MaterializeAsync(ct).ConfigureAwait(false);
        object result = items.Select(compiled).Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return (TValue)result;
    }

    public Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.average", typeof(T).Name, () => this.AverageImpl(selector, ct));

    async Task<double> AverageImpl(Expression<Func<T, object>> selector, CancellationToken ct)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        return (await this.MaterializeAsync(ct).ConfigureAwait(false)).Average(x => Convert.ToDouble(compiled(x)));
    }

    public DocumentQueryString ToQueryString()
    {
        var query = RedisSearchQueryBuilder.BuildPushdown(this.GetEffectivePredicateExpressions().ToList(), this.store.IndexFieldsFor<T>()) ?? "*";
        return new DocumentQueryString(query, new Dictionary<string, object?>());
    }

    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange(CancellationToken ct = default)
    {
        var compiled = this.GetEffectivePredicateExpressions().Select(p => ExpressionInterpreter.Interpret(p)).ToList();
        Func<T, bool>? predicate = compiled.Count == 0 ? null : item => compiled.All(p => p(item));
        return Filter(this.store.Broadcaster.Observe<T>(ct), predicate, ct);
    }

    static async IAsyncEnumerable<DocumentChange<T>> Filter(
        IAsyncEnumerable<DocumentChange<T>> source,
        Func<T, bool>? predicate,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var change in source.WithCancellation(ct).ConfigureAwait(false))
        {
            if (predicate == null || change.Document is null || predicate(change.Document))
                yield return change;
        }
    }

    public Task<IReadOnlyList<FullTextResult<T>>> FullTextMatch(string searchText, int maxResults = 50, CancellationToken ct = default)
    {
        var effective = this.GetEffectivePredicateExpressions().ToList();
        Expression<Func<T, bool>>? filter = effective.Count == 0 ? null : DocumentQuery<T>.CombinePredicates(effective);
        return this.store.FullTextSearch(searchText, maxResults, filter, ct);
    }

    public Task<IReadOnlyList<VectorResult<T>>> NearestVectors(ReadOnlyMemory<float> query, int k, CancellationToken ct = default)
    {
        var effective = this.GetEffectivePredicateExpressions().ToList();
        Expression<Func<T, bool>>? filter = effective.Count == 0 ? null : DocumentQuery<T>.CombinePredicates(effective);
        return this.store.NearestVectors(query, k, filter, ct);
    }

    // ── Internal ────────────────────────────────────────────────────────

    // Filtered source only (no ordering/paging) — what a grouped query aggregates over.
    internal async Task<IEnumerable<T>> MaterializeForGroupingAsync()
    {
        var predicateList = this.GetEffectivePredicateExpressions().ToList();
        var keys = await this.store.ResolveCandidateKeysAsync(predicateList, CancellationToken.None).ConfigureAwait(false);
        var docs = await this.store.LoadDocumentsAsync(keys, this.typeInfo, CancellationToken.None).ConfigureAwait(false);
        IEnumerable<T> query = docs;
        foreach (var p in predicateList)
            query = query.Where(ExpressionInterpreter.Interpret(p));
        return query.ToList();
    }

    internal async Task<IEnumerable<T>> MaterializeAsync(CancellationToken ct)
    {
        var predicateList = this.GetEffectivePredicateExpressions().ToList();
        var keys = await this.store.ResolveCandidateKeysAsync(predicateList, ct).ConfigureAwait(false);
        var docs = await this.store.LoadDocumentsAsync(keys, this.typeInfo, ct).ConfigureAwait(false);

        IEnumerable<T> query = docs;
        foreach (var predicate in predicateList)
            query = query.Where(ExpressionInterpreter.Interpret(predicate));

        IOrderedEnumerable<T>? ordered = null;
        foreach (var (selector, descending) in this.orderBys)
        {
            var typedFunc = ExpressionInterpreter.Interpret<T, object>((Expression<Func<T, object>>)selector);
            if (ordered == null)
                ordered = descending ? query.OrderByDescending(typedFunc) : query.OrderBy(typedFunc);
            else
                ordered = descending ? ordered.ThenByDescending(typedFunc) : ordered.ThenBy(typedFunc);
        }
        if (ordered != null)
            query = ordered;

        if (this.skipCount.HasValue)
            query = query.Skip(this.skipCount.Value);
        if (this.takeCount.HasValue)
            query = query.Take(this.takeCount.Value);

        return query.ToList();
    }

    static TVal DynamicAdd<TVal>(TVal a, TVal b)
    {
        if (a is int ai && b is int bi) return (TVal)(object)(ai + bi);
        if (a is long al && b is long bl) return (TVal)(object)(al + bl);
        if (a is double ad && b is double bd) return (TVal)(object)(ad + bd);
        if (a is decimal am && b is decimal bm) return (TVal)(object)(am + bm);
        if (a is float af && b is float bf) return (TVal)(object)(af + bf);
        throw new NotSupportedException($"Sum is not supported for type {typeof(TVal).Name}");
    }
}

internal class RedisProjectedDocumentQuery<TSource, TResult> : IDocumentQuery<TResult>
    where TSource : class
    where TResult : class
{
    readonly RedisDocumentQuery<TSource> source;
    readonly Func<TSource, TResult> compiledSelector;

    internal RedisProjectedDocumentQuery(RedisDocumentQuery<TSource> source, Expression<Func<TSource, TResult>> selector)
    {
        this.source = source;
        this.compiledSelector = ExpressionInterpreter.Interpret(selector);
    }

    async Task<IEnumerable<TResult>> MaterializeAsync(CancellationToken ct)
        => (await this.source.MaterializeAsync(ct).ConfigureAwait(false)).Select(this.compiledSelector);

    public IDocumentQuery<TResult> Where(Expression<Func<TResult, bool>> predicate)
        => throw new NotSupportedException("Cannot chain Where after Select. Apply filters before Select.");

    public IDocumentQuery<TResult> IgnoreQueryFilters()
        => throw new NotSupportedException("Cannot call IgnoreQueryFilters after Select. Call it on the source query before projecting.");

    public IDocumentQuery<TResult> IgnoreQueryFilters(params string[] filterNames)
        => throw new NotSupportedException("Cannot call IgnoreQueryFilters after Select. Call it on the source query before projecting.");

    public IDocumentQuery<TResult> OrderBy(Expression<Func<TResult, object>> selector)
        => throw new NotSupportedException("Cannot chain OrderBy after Select. Apply ordering before Select.");

    public IDocumentQuery<TResult> OrderByDescending(Expression<Func<TResult, object>> selector)
        => throw new NotSupportedException("Cannot chain OrderByDescending after Select. Apply ordering before Select.");

    public IGroupedDocumentQuery<TResult, TKey> GroupBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        => throw new NotSupportedException("Cannot chain GroupBy after Select.");

    public IDocumentQuery<TResult> Paginate(int offset, int take)
        => throw new NotSupportedException("Cannot chain Paginate after Select. Apply pagination before Select.");

    public IDocumentQuery<TNewResult> Select<TNewResult>(
        Expression<Func<TResult, TNewResult>> selector,
        JsonTypeInfo<TNewResult>? resultTypeInfo = null) where TNewResult : class
        => throw new NotSupportedException("Cannot chain Select after Select.");

    public Task<IReadOnlyList<TResult>> ToList(CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.to_list", typeof(TResult).Name, () => this.ToListImpl(ct), r => r.Count);

    async Task<IReadOnlyList<TResult>> ToListImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).ToList().AsReadOnly();

    public async IAsyncEnumerable<TResult> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in await this.MaterializeAsync(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public Task<long> Count(CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.count", typeof(TResult).Name, () => this.CountImpl(ct), r => r);

    async Task<long> CountImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Count();

    public Task<bool> Any(CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.any", typeof(TResult).Name, () => this.AnyImpl(ct), r => r ? 1 : 0);

    async Task<bool> AnyImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Any();

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteDelete after Select.");

    public Task<int> ExecuteUpdate(Expression<Func<TResult, object>> property, object? value, CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteUpdate after Select.");

    public Task<TValue> Max<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.max", typeof(TResult).Name, () => this.MaxImpl(selector, ct));

    async Task<TValue> MaxImpl<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Max(ExpressionInterpreter.Interpret(selector))!;

    public Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.min", typeof(TResult).Name, () => this.MinImpl(selector, ct));

    async Task<TValue> MinImpl<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Min(ExpressionInterpreter.Interpret(selector))!;

    public Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.sum", typeof(TResult).Name, () => this.SumImpl(selector, ct));

    async Task<TValue> SumImpl<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var items = (await this.MaterializeAsync(ct).ConfigureAwait(false)).Select(compiled);
        object result = items.Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return (TValue)result;
    }

    public Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.average", typeof(TResult).Name, () => this.AverageImpl(selector, ct));

    async Task<double> AverageImpl(Expression<Func<TResult, object>> selector, CancellationToken ct)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        return (await this.MaterializeAsync(ct).ConfigureAwait(false)).Average(x => Convert.ToDouble(compiled(x)));
    }

    public IAsyncEnumerable<DocumentChange<TResult>> NotifyOnChange(CancellationToken ct = default)
        => throw new NotSupportedException("NotifyOnChange is not supported after Select.");

    static TVal DynamicAdd<TVal>(TVal a, TVal b)
    {
        if (a is int ai && b is int bi) return (TVal)(object)(ai + bi);
        if (a is long al && b is long bl) return (TVal)(object)(al + bl);
        if (a is double ad && b is double bd) return (TVal)(object)(ad + bd);
        if (a is decimal am && b is decimal bm) return (TVal)(object)(am + bm);
        throw new NotSupportedException($"Sum is not supported for type {typeof(TVal).Name}");
    }
}
