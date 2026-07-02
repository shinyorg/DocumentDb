using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.DynamoDb;

/// <summary>
/// Client-side <see cref="IDocumentQuery{T}"/> for DynamoDB. A single-partition native query
/// pulls every document of the type; predicates, ordering, pagination, projections, and aggregates then run
/// in memory via <see cref="ExpressionInterpreter"/> (the LiteDB model). Loading the whole partition is a
/// full type scan — map hot query paths accordingly.
/// </summary>
public class DynamoDbDocumentQuery<T> : IDocumentQuery<T> where T : class
{
    readonly DynamoDbDocumentStore store;
    readonly JsonTypeInfo<T>? typeInfo;

    public JsonTypeInfo<T>? QueryTypeInfo => this.typeInfo;
    readonly List<Expression<Func<T, bool>>> predicates = new();
    readonly List<(LambdaExpression Selector, bool Descending)> orderBys = new();
    int? skipCount;
    int? takeCount;
    bool ignoreAllFilters;
    HashSet<string>? ignoredFilterNames;

    internal DynamoDbDocumentQuery(DynamoDbDocumentStore store, JsonTypeInfo<T>? typeInfo)
    {
        this.store = store;
        this.typeInfo = typeInfo;
    }

    DynamoDbDocumentQuery(DynamoDbDocumentQuery<T> source)
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
        var clone = new DynamoDbDocumentQuery<T>(this);
        clone.predicates.Add(predicate);
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters()
    {
        var clone = new DynamoDbDocumentQuery<T>(this);
        clone.ignoreAllFilters = true;
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames)
    {
        ArgumentNullException.ThrowIfNull(filterNames);
        var clone = new DynamoDbDocumentQuery<T>(this);
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
        var clone = new DynamoDbDocumentQuery<T>(this);
        clone.orderBys.Add((selector, false));
        return clone;
    }

    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector)
    {
        var clone = new DynamoDbDocumentQuery<T>(this);
        clone.orderBys.Add((selector, true));
        return clone;
    }

    public IDocumentQuery<T> GroupBy(Expression<Func<T, object>> selector)
        => new DynamoDbDocumentQuery<T>(this);

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = new DynamoDbDocumentQuery<T>(this);
        clone.skipCount = offset;
        clone.takeCount = take;
        return clone;
    }

    public IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<T, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
        => new DynamoDbProjectedDocumentQuery<T, TResult>(this, selector);

    public async Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
    {
        var results = await this.MaterializeAsync(ct).ConfigureAwait(false);
        return results.ToList().AsReadOnly();
    }

    public async IAsyncEnumerable<T> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in await this.MaterializeAsync(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public async Task<long> Count(CancellationToken ct = default)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Count();

    public async Task<bool> Any(CancellationToken ct = default)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Any();

    public async Task<int> ExecuteDelete(CancellationToken ct = default)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var interceptors = this.store.InterceptorPipeline;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Delete, typeName);
        await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false);

        var predicate = this.BuildCombinedPredicate();
        var count = await this.store.DeleteWhereAsync(predicate, this.typeInfo, ct).ConfigureAwait(false);

        await interceptors.AfterBulk(bulkCtx, count, ct).ConfigureAwait(false);
        return count;
    }

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
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
        var count = await this.store.UpdatePropertyWhereAsync(predicate, jsonPath, value, this.typeInfo, ct).ConfigureAwait(false);

        await interceptors.AfterBulk(bulkCtx, count, ct).ConfigureAwait(false);
        return count;
    }

    public async Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Max(ExpressionInterpreter.Interpret(selector));

    public async Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Min(ExpressionInterpreter.Interpret(selector));

    public async Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var items = await this.MaterializeAsync(ct).ConfigureAwait(false);
        object result = items.Select(compiled).Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return (TValue)result;
    }

    public async Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        return (await this.MaterializeAsync(ct).ConfigureAwait(false)).Average(x => Convert.ToDouble(compiled(x)));
    }

    public DocumentQueryString ToQueryString()
    {
        var partitionKey = this.store.ResolvePartitionKeyFor<T>();
        var pushdown = this.store.BuildPushdown(this.GetEffectivePredicateExpressions());
        var sql = $"pk = '{partitionKey}'";
        var parameters = new Dictionary<string, object?>();
        if (pushdown is { } f)
        {
            sql += $" AND ({f.Expression})";
            foreach (var kv in f.Names)
                parameters[kv.Key] = kv.Value;
        }
        return new DocumentQueryString(sql, parameters);
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

    // ── Internal ────────────────────────────────────────────────────────

    internal async Task<IEnumerable<T>> MaterializeAsync(CancellationToken ct)
    {
        var pushdown = this.store.BuildPushdown(this.GetEffectivePredicateExpressions());
        var items = new List<T>();
        await foreach (var doc in this.store.LoadDocumentsAsync(this.typeInfo, pushdown, ct).ConfigureAwait(false))
            items.Add(doc);

        IEnumerable<T> query = items;
        foreach (var predicate in this.GetEffectivePredicateExpressions())
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

        return query;
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
}

internal class DynamoDbProjectedDocumentQuery<TSource, TResult> : IDocumentQuery<TResult>
    where TSource : class
    where TResult : class
{
    readonly DynamoDbDocumentQuery<TSource> source;
    readonly Func<TSource, TResult> compiledSelector;

    internal DynamoDbProjectedDocumentQuery(DynamoDbDocumentQuery<TSource> source, Expression<Func<TSource, TResult>> selector)
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

    public IDocumentQuery<TResult> GroupBy(Expression<Func<TResult, object>> selector)
        => throw new NotSupportedException("Cannot chain GroupBy after Select.");

    public IDocumentQuery<TResult> Paginate(int offset, int take)
        => throw new NotSupportedException("Cannot chain Paginate after Select. Apply pagination before Select.");

    public IDocumentQuery<TNewResult> Select<TNewResult>(
        Expression<Func<TResult, TNewResult>> selector,
        JsonTypeInfo<TNewResult>? resultTypeInfo = null) where TNewResult : class
        => throw new NotSupportedException("Cannot chain Select after Select.");

    public async Task<IReadOnlyList<TResult>> ToList(CancellationToken ct = default)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).ToList().AsReadOnly();

    public async IAsyncEnumerable<TResult> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in await this.MaterializeAsync(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public async Task<long> Count(CancellationToken ct = default)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Count();

    public async Task<bool> Any(CancellationToken ct = default)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Any();

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteDelete after Select.");

    public Task<int> ExecuteUpdate(Expression<Func<TResult, object>> property, object? value, CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteUpdate after Select.");

    public async Task<TValue> Max<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Max(ExpressionInterpreter.Interpret(selector))!;

    public async Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Min(ExpressionInterpreter.Interpret(selector))!;

    public async Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var items = (await this.MaterializeAsync(ct).ConfigureAwait(false)).Select(compiled);
        object result = items.Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return (TValue)result;
    }

    public async Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default)
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
