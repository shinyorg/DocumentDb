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
    readonly List<Expression<Func<T, bool>>> predicates = new();
    readonly List<(LambdaExpression Selector, bool Descending)> orderBys = new();
    int? skipCount;
    int? takeCount;

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
    }

    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate)
    {
        var clone = new IndexedDbDocumentQuery<T>(this);
        clone.predicates.Add(predicate);
        return clone;
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

    public IDocumentQuery<T> GroupBy(Expression<Func<T, object>> selector)
    {
        // GroupBy not meaningfully supported in client-side LINQ over IndexedDB
        return this;
    }

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = new IndexedDbDocumentQuery<T>(this);
        clone.skipCount = offset;
        clone.takeCount = take;
        return clone;
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
        var predicate = this.BuildCombinedPredicate();
        return await this.store.DeleteDocumentsAsync(typeName, predicate, this.typeInfo);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    public async Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var predicate = this.BuildCombinedPredicate();
        var jsonPath = this.typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions, this.typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions);

        return await this.store.UpdateDocumentPropertyAsync(typeName, predicate, jsonPath, value, this.typeInfo);
    }

    public async Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        var items = await this.MaterializeAsync();
        return items.Max(compiled);
    }

    public async Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        var items = await this.MaterializeAsync();
        return items.Min(compiled);
    }

    public async Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        var items = await this.MaterializeAsync();
        object result = items.Select(compiled).Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return (TValue)result;
    }

    public async Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        var items = await this.MaterializeAsync();
        return items.Average(x => Convert.ToDouble(compiled(x)));
    }

    // ── Internal ─────────────────────���──────────────────────────────────

    internal async Task<IEnumerable<T>> MaterializeAsync()
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        IEnumerable<T> items = await this.store.LoadDocumentsAsync(typeName, this.typeInfo);

        foreach (var predicate in this.predicates)
        {
            var compiled = predicate.Compile();
            items = items.Where(compiled);
        }

        IOrderedEnumerable<T>? ordered = null;
        foreach (var (selector, descending) in this.orderBys)
        {
            var compiled = selector.Compile();
            var typedFunc = (Func<T, object>)compiled;

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
        if (this.predicates.Count == 0)
            return _ => true;

        var compiled = this.predicates.Select(p => p.Compile()).ToList();
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
        this.compiledSelector = selector.Compile();
    }

    async Task<IEnumerable<TResult>> MaterializeAsync()
        => (await this.source.MaterializeAsync()).Select(this.compiledSelector);

    public IDocumentQuery<TResult> Where(Expression<Func<TResult, bool>> predicate)
        => throw new NotSupportedException("Cannot chain Where after Select. Apply filters before Select.");

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
        var compiled = selector.Compile();
        return (await this.MaterializeAsync()).Max(compiled);
    }

    public async Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        return (await this.MaterializeAsync()).Min(compiled);
    }

    public async Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
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
        var compiled = selector.Compile();
        return (await this.MaterializeAsync()).Average(x => Convert.ToDouble(compiled(x)));
    }
}
