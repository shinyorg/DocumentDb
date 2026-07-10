using Shiny.DocumentDb.Internal.Query;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.IndexedDb;

public class IndexedDbDocumentQuery<T> : IDocumentQuery<T> where T : class
{
    readonly IndexedDbDocumentStore store;
    readonly JsonTypeInfo<T>? typeInfo;

    public JsonTypeInfo<T>? QueryTypeInfo => this.typeInfo;
    readonly List<Expression<Func<T, bool>>> predicates = new();
    readonly List<(LambdaExpression Selector, bool Descending)> orderBys = new();
    int? skipCount;
    int? takeCount;
    bool ignoreAllFilters;
    HashSet<string>? ignoredFilterNames;

    internal IndexedDbDocumentQuery(IndexedDbDocumentStore store, JsonTypeInfo<T>? typeInfo)
    {
        this.store = store;
        this.typeInfo = typeInfo;
    }

    IndexedDbDocumentQuery(IndexedDbDocumentQuery<T> source)
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
        var clone = new IndexedDbDocumentQuery<T>(this);
        clone.predicates.Add(predicate);
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters()
    {
        var clone = new IndexedDbDocumentQuery<T>(this);
        clone.ignoreAllFilters = true;
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames)
    {
        ArgumentNullException.ThrowIfNull(filterNames);
        var clone = new IndexedDbDocumentQuery<T>(this);
        clone.ignoredFilterNames ??= new HashSet<string>(StringComparer.Ordinal);
        foreach (var n in filterNames)
            if (!string.IsNullOrWhiteSpace(n))
                clone.ignoredFilterNames.Add(n);
        return clone;
    }

    IEnumerable<Expression<Func<T, bool>>> GetEffectivePredicateExpressions()
    {
        if (!this.ignoreAllFilters)
        {
            foreach (var f in this.store.Options.ResolveQueryFilters(typeof(T)))
            {
                if (f.Name != null && this.ignoredFilterNames?.Contains(f.Name) == true)
                    continue;
                yield return (Expression<Func<T, bool>>)f.Predicate;
            }
        }
        foreach (var p in this.predicates)
            yield return p;
    }

    public IDocumentQuery<T> OrderBy(Expression<Func<T, object>> selector)
    {
        var clone = new IndexedDbDocumentQuery<T>(this);
        clone.orderBys.Add((selector, false));
        return clone;
    }

    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector)
    {
        var clone = new IndexedDbDocumentQuery<T>(this);
        clone.orderBys.Add((selector, true));
        return clone;
    }

    public IGroupedDocumentQuery<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        => new InMemoryGroupedQuery<T, TKey>(this.MaterializeForGroupingAsync, ExpressionInterpreter.Interpret(keySelector));

    public IGroupedDocumentQuery<T, object> GroupBy(string keyField)
        => throw new NotSupportedException(
            "String GroupBy(\"field\") is not supported on the in-memory providers. Use the typed " +
            "GroupBy(keySelector).Select(g => …) form, or a relational/MongoDB store for the string grammar.");

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = new IndexedDbDocumentQuery<T>(this);
        clone.skipCount = offset;
        clone.takeCount = take;
        return clone;
    }

    public async Task<CursorPage<T>> ToCursorPage(string? cursor, int take, CancellationToken ct = default)
    {
        var keys = new List<CursorSortKey<T>>(this.orderBys.Count);
        var specParts = new List<string>(this.orderBys.Count);
        foreach (var (selector, descending) in this.orderBys)
        {
            var getter = ExpressionInterpreter.Interpret<T, object>((Expression<Func<T, object>>)selector);
            keys.Add(new CursorSortKey<T>(getter, descending));
            specParts.Add($"{selector}:{(descending ? "d" : "a")}");
        }

        var accessor = this.store.IdCache.GetOrCreate(this.typeInfo);
        var spec = this.store.ResolveTypeNameFor<T>() + "|" + string.Join("|", specParts);
        var matching = await this.MaterializeForGroupingAsync().ConfigureAwait(false);
        return InMemoryCursorPager.Page(matching, keys, accessor.GetIdAsString, spec, cursor, take);
    }

    public IDocumentQuery<System.Text.Json.Nodes.JsonObject> Project(string fields, JsonTypeInfo<T>? jsonTypeInfo = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fields);
        var info = jsonTypeInfo ?? this.typeInfo
            ?? throw new InvalidOperationException(
                $"No JsonTypeInfo<{typeof(T).Name}> could be resolved for the projection. Pass one explicitly or register a JsonSerializerContext.");
        return new StringProjectionQuery<T>(this, StringProjection.BuildGetters(fields, info, this.store.Options.ResolveComputedLookup(typeof(T))));
    }

    public IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<T, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
    {
        return new IndexedDbProjectedDocumentQuery<T, TResult>(this, selector, this.store, resultTypeInfo);
    }

    public async Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
    {
        var results = await this.MaterializeAsync();
        return results.ToList().AsReadOnly();
    }

    public async IAsyncEnumerable<T> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        var items = await this.MaterializeAsync();
        foreach (var item in items)
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public async Task<long> Count(CancellationToken ct = default)
    {
        var items = await this.MaterializeAsync();
        return items.Count();
    }

    public async Task<bool> Any(CancellationToken ct = default)
    {
        var items = await this.MaterializeAsync();
        return items.Any();
    }

    public async Task<int> ExecuteDelete(CancellationToken ct = default)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var interceptors = this.store.InterceptorPipeline;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Delete, typeName);
        await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false);

        var predicate = this.BuildCombinedPredicate();
        var count = await this.store.DeleteDocumentsAsync(typeName, predicate, this.typeInfo);

        await interceptors.AfterBulk(bulkCtx, count, ct).ConfigureAwait(false);
        return count;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    public async Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var jsonPath = this.typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions, this.typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions);

        var interceptors = this.store.InterceptorPipeline;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Update, typeName, assignment: (jsonPath, value));
        await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false);

        var predicate = this.BuildCombinedPredicate();
        var count = await this.store.UpdateDocumentPropertyAsync(typeName, predicate, jsonPath, value, this.typeInfo);

        await interceptors.AfterBulk(bulkCtx, count, ct).ConfigureAwait(false);
        return count;
    }

    public async Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var items = await this.MaterializeAsync();
        return items.Max(compiled);
    }

    public async Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var items = await this.MaterializeAsync();
        return items.Min(compiled);
    }

    public async Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var items = await this.MaterializeAsync();
        object result = items.Select(compiled).Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return (TValue)result;
    }

    public async Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var items = await this.MaterializeAsync();
        return items.Average(x => Convert.ToDouble(compiled(x)));
    }

    // ── Internal ─────────────────────���──────────────────────────────────

    // Filtered source (computed populated + predicates applied) without ordering/pagination — the input
    // a grouped query aggregates over.
    internal async Task<IEnumerable<T>> MaterializeForGroupingAsync()
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        IEnumerable<T> items = await this.store.LoadDocumentsAsync(typeName, this.typeInfo);

        var computed = this.store.Options.ResolveComputedMappings(typeof(T));
        if (computed.Count > 0)
            items = items.Select(d =>
            {
                for (var i = 0; i < computed.Count; i++)
                    computed[i].SetValue(d, computed[i].Compute(d));
                return d;
            });

        foreach (var predicate in this.GetEffectivePredicateExpressions())
        {
            var compiled = ExpressionInterpreter.Interpret(predicate);
            items = items.Where(compiled);
        }
        return items;
    }

    internal async Task<IEnumerable<T>> MaterializeAsync()
    {
        IEnumerable<T> items = await this.MaterializeForGroupingAsync();

        IOrderedEnumerable<T>? ordered = null;
        foreach (var (selector, descending) in this.orderBys)
        {
            var typedFunc = ExpressionInterpreter.Interpret<T, object>((Expression<Func<T, object>>)selector);

            if (ordered == null)
                ordered = descending ? items.OrderByDescending(typedFunc) : items.OrderBy(typedFunc);
            else
                ordered = descending ? ordered.ThenByDescending(typedFunc) : ordered.ThenBy(typedFunc);
        }

        if (ordered != null)
            items = ordered;

        if (this.skipCount.HasValue)
            items = items.Skip(this.skipCount.Value);

        if (this.takeCount.HasValue)
            items = items.Take(this.takeCount.Value);

        return items;
    }

    Func<T, bool> BuildCombinedPredicate()
    {
        var compiled = this.GetEffectivePredicateExpressions().Select(p => ExpressionInterpreter.Interpret(p)).ToList();
        if (compiled.Count == 0)
            return _ => true;
        return item => compiled.All(p => p(item));
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

    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange(CancellationToken ct = default)
        => throw new NotSupportedException(
            "Per-query change observation is not supported by IndexedDbDocumentStore.");

    public Task<IReadOnlyList<FullTextResult<T>>> FullTextMatch(string searchText, int maxResults = 50, CancellationToken ct = default)
    {
        var effective = this.GetEffectivePredicateExpressions().ToList();
        Expression<Func<T, bool>>? filter = effective.Count == 0
            ? null
            : DocumentQuery<T>.CombinePredicates(effective);
        return this.store.FullTextSearch(searchText, maxResults, filter, ct);
    }
}

internal class IndexedDbProjectedDocumentQuery<TSource, TResult> : IDocumentQuery<TResult>
    where TSource : class
    where TResult : class
{
    readonly IndexedDbDocumentQuery<TSource> source;
    readonly Func<TSource, TResult> compiledSelector;

    internal IndexedDbProjectedDocumentQuery(
        IndexedDbDocumentQuery<TSource> source,
        Expression<Func<TSource, TResult>> selector,
        IndexedDbDocumentStore store,
        JsonTypeInfo<TResult>? resultTypeInfo)
    {
        this.source = source;
        this.compiledSelector = ExpressionInterpreter.Interpret(selector);
    }

    async Task<IEnumerable<TResult>> MaterializeAsync()
        => (await this.source.MaterializeAsync()).Select(this.compiledSelector);

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

    public async Task<IReadOnlyList<TResult>> ToList(CancellationToken ct = default)
        => (await this.MaterializeAsync()).ToList().AsReadOnly();

    public async IAsyncEnumerable<TResult> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        var items = await this.MaterializeAsync();
        foreach (var item in items)
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public async Task<long> Count(CancellationToken ct = default)
        => (await this.MaterializeAsync()).Count();

    public async Task<bool> Any(CancellationToken ct = default)
        => (await this.MaterializeAsync()).Any();

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteDelete after Select.");

    public Task<int> ExecuteUpdate(Expression<Func<TResult, object>> property, object? value, CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteUpdate after Select.");

    public async Task<TValue> Max<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        return (await this.MaterializeAsync()).Max(compiled);
    }

    public async Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        return (await this.MaterializeAsync()).Min(compiled);
    }

    public async Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var items = (await this.MaterializeAsync()).Select(compiled);
        object result = items.Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return (TValue)result;
    }

    static TVal DynamicAdd<TVal>(TVal a, TVal b)
    {
        if (a is int ai && b is int bi) return (TVal)(object)(ai + bi);
        if (a is long al && b is long bl) return (TVal)(object)(al + bl);
        if (a is double ad && b is double bd) return (TVal)(object)(ad + bd);
        if (a is decimal am && b is decimal bm) return (TVal)(object)(am + bm);
        throw new NotSupportedException($"Sum is not supported for type {typeof(TVal).Name}");
    }

    public async Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        return (await this.MaterializeAsync()).Average(x => Convert.ToDouble(compiled(x)));
    }

    public IAsyncEnumerable<DocumentChange<TResult>> NotifyOnChange(CancellationToken ct = default)
        => throw new NotSupportedException(
            "Per-query change observation is not supported by IndexedDbDocumentStore.");
}
