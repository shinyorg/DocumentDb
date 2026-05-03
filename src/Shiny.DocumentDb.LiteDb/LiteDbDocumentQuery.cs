using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.LiteDb;

public class LiteDbDocumentQuery<T> : IDocumentQuery<T> where T : class
{
    readonly LiteDbDocumentStore store;
    readonly JsonTypeInfo<T>? typeInfo;
    readonly List<Expression<Func<T, bool>>> predicates = new();
    readonly List<(LambdaExpression Selector, bool Descending)> orderBys = new();
    Expression<Func<T, object>>? groupBySelector;
    int? skipCount;
    int? takeCount;

    internal LiteDbDocumentQuery(LiteDbDocumentStore store, JsonTypeInfo<T>? typeInfo)
    {
        this.store = store;
        this.typeInfo = typeInfo;
    }

    LiteDbDocumentQuery(LiteDbDocumentQuery<T> source)
    {
        this.store = source.store;
        this.typeInfo = source.typeInfo;
        this.predicates.AddRange(source.predicates);
        this.orderBys.AddRange(source.orderBys);
        this.groupBySelector = source.groupBySelector;
        this.skipCount = source.skipCount;
        this.takeCount = source.takeCount;
    }

    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate)
    {
        var clone = new LiteDbDocumentQuery<T>(this);
        clone.predicates.Add(predicate);
        return clone;
    }

    public IDocumentQuery<T> OrderBy(Expression<Func<T, object>> selector)
    {
        var clone = new LiteDbDocumentQuery<T>(this);
        clone.orderBys.Add((selector, false));
        return clone;
    }

    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector)
    {
        var clone = new LiteDbDocumentQuery<T>(this);
        clone.orderBys.Add((selector, true));
        return clone;
    }

    public IDocumentQuery<T> GroupBy(Expression<Func<T, object>> selector)
    {
        var clone = new LiteDbDocumentQuery<T>(this);
        clone.groupBySelector = selector;
        return clone;
    }

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = new LiteDbDocumentQuery<T>(this);
        clone.skipCount = offset;
        clone.takeCount = take;
        return clone;
    }

    public IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<T, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
    {
        return new LiteDbProjectedDocumentQuery<T, TResult>(this, selector, this.store, resultTypeInfo);
    }

    public async Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
    {
        var results = this.Materialize();
        return results.ToList().AsReadOnly();
    }

    public async IAsyncEnumerable<T> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in this.Materialize())
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public Task<long> Count(CancellationToken ct = default)
    {
        var count = (long)this.Materialize().Count();
        return Task.FromResult(count);
    }

    public Task<bool> Any(CancellationToken ct = default)
    {
        var any = this.Materialize().Any();
        return Task.FromResult(any);
    }

    public Task<int> ExecuteDelete(CancellationToken ct = default)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var predicate = this.BuildCombinedPredicate();
        var count = this.store.DeleteDocuments(typeName, predicate, this.typeInfo);
        return Task.FromResult(count);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    public Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var predicate = this.BuildCombinedPredicate();
        var jsonPath = this.typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions, this.typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions);

        var count = this.store.UpdateDocumentProperty(typeName, predicate, jsonPath, value, this.typeInfo);
        return Task.FromResult(count);
    }

    public Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        return Task.FromResult(this.Materialize().Max(compiled));
    }

    public Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        return Task.FromResult(this.Materialize().Min(compiled));
    }

    public Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        var items = this.Materialize();
        object result = items.Select(compiled).Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return Task.FromResult((TValue)result);
    }

    public Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        var avg = this.Materialize().Average(x => Convert.ToDouble(compiled(x)));
        return Task.FromResult(avg);
    }

    // ── Internal ────────────────────────────────────────────────────────

    internal IEnumerable<T> Materialize()
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        IEnumerable<T> items = this.store.LoadDocuments(typeName, this.typeInfo);

        // Apply predicates
        foreach (var predicate in this.predicates)
        {
            var compiled = predicate.Compile();
            items = items.Where(compiled);
        }

        // Apply ordering
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

        // Apply pagination
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

internal class LiteDbProjectedDocumentQuery<TSource, TResult> : IDocumentQuery<TResult>
    where TSource : class
    where TResult : class
{
    readonly LiteDbDocumentQuery<TSource> source;
    readonly Expression<Func<TSource, TResult>> selector;
    readonly Func<TSource, TResult> compiledSelector;
    readonly LiteDbDocumentStore store;
    readonly JsonTypeInfo<TResult>? resultTypeInfo;

    internal LiteDbProjectedDocumentQuery(
        LiteDbDocumentQuery<TSource> source,
        Expression<Func<TSource, TResult>> selector,
        LiteDbDocumentStore store,
        JsonTypeInfo<TResult>? resultTypeInfo)
    {
        this.source = source;
        this.selector = selector;
        this.compiledSelector = selector.Compile();
        this.store = store;
        this.resultTypeInfo = resultTypeInfo;
    }

    IEnumerable<TResult> Materialize()
        => this.source.Materialize().Select(this.compiledSelector);

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
        => this.Materialize().ToList().AsReadOnly();

    public async IAsyncEnumerable<TResult> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in this.Materialize())
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public Task<long> Count(CancellationToken ct = default)
        => Task.FromResult((long)this.Materialize().Count());

    public Task<bool> Any(CancellationToken ct = default)
        => Task.FromResult(this.Materialize().Any());

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteDelete after Select.");

    public Task<int> ExecuteUpdate(Expression<Func<TResult, object>> property, object? value, CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteUpdate after Select.");

    public Task<TValue> Max<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        return Task.FromResult(this.Materialize().Max(compiled)!);
    }

    public Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        return Task.FromResult(this.Materialize().Min(compiled)!);
    }

    public Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        var items = this.Materialize().Select(compiled);
        object result = items.Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return Task.FromResult((TValue)result);
    }

    static TVal DynamicAdd<TVal>(TVal a, TVal b)
    {
        if (a is int ai && b is int bi) return (TVal)(object)(ai + bi);
        if (a is long al && b is long bl) return (TVal)(object)(al + bl);
        if (a is double ad && b is double bd) return (TVal)(object)(ad + bd);
        if (a is decimal am && b is decimal bm) return (TVal)(object)(am + bm);
        throw new NotSupportedException($"Sum is not supported for type {typeof(TVal).Name}");
    }

    public Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        return Task.FromResult(this.Materialize().Average(x => Convert.ToDouble(compiled(x))));
    }
}
