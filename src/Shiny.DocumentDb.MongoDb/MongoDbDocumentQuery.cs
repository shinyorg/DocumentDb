using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using MongoDB.Bson;
using MongoDB.Driver;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.MongoDb;

public class MongoDbDocumentQuery<T> : IDocumentQuery<T> where T : class
{
    readonly MongoDbDocumentStore store;
    readonly JsonTypeInfo<T>? typeInfo;

    public JsonTypeInfo<T>? QueryTypeInfo => this.typeInfo;
    readonly List<Expression<Func<T, bool>>> predicates = new();
    readonly List<(Expression<Func<T, object>> Selector, bool Descending)> orderBys = new();
    int? skipCount;
    int? takeCount;
    bool ignoreAllFilters;
    HashSet<string>? ignoredFilterNames;

    internal MongoDbDocumentQuery(MongoDbDocumentStore store, JsonTypeInfo<T>? typeInfo)
    {
        this.store = store;
        this.typeInfo = typeInfo;
    }

    MongoDbDocumentQuery(MongoDbDocumentQuery<T> source)
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
        var clone = new MongoDbDocumentQuery<T>(this);
        clone.predicates.Add(predicate);
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters()
    {
        var clone = new MongoDbDocumentQuery<T>(this);
        clone.ignoreAllFilters = true;
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames)
    {
        ArgumentNullException.ThrowIfNull(filterNames);
        var clone = new MongoDbDocumentQuery<T>(this);
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
        var clone = new MongoDbDocumentQuery<T>(this);
        clone.orderBys.Add((selector, false));
        return clone;
    }

    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector)
    {
        var clone = new MongoDbDocumentQuery<T>(this);
        clone.orderBys.Add((selector, true));
        return clone;
    }

    public IDocumentQuery<T> GroupBy(Expression<Func<T, object>> selector)
        => throw new NotSupportedException("GroupBy is only supported with Select projections containing aggregate functions.");

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = new MongoDbDocumentQuery<T>(this);
        clone.skipCount = offset;
        clone.takeCount = take;
        return clone;
    }

    public IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<T, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
        => new MongoDbProjectedDocumentQuery<T, TResult>(this, selector, this.store, resultTypeInfo);

    public Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
    {
        var filter = this.BuildFilter();
        var sort = this.BuildSort();
        return this.store.ExecuteFindAsync(filter, sort, this.skipCount, this.takeCount, this.typeInfo, ct);
    }

    public async IAsyncEnumerable<T> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        var list = await this.ToList(ct).ConfigureAwait(false);
        foreach (var item in list)
            yield return item;
    }

    public Task<long> Count(CancellationToken ct = default)
        => this.store.ExecuteCountAsync<T>(this.BuildFilter(), ct);

    public async Task<bool> Any(CancellationToken ct = default)
    {
        var count = await this.Count(ct).ConfigureAwait(false);
        return count > 0;
    }

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => this.store.ExecuteDeleteAsync<T>(this.BuildFilter(), ct);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    public Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
    {
        var jsonPath = this.typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions, this.typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions);
        return this.store.ExecuteUpdatePropertyAsync<T>(this.BuildFilter(), jsonPath, value, ct);
    }

    public async Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var items = await this.ToList(ct).ConfigureAwait(false);
        var compiled = selector.Compile();
        return items.Max(compiled)!;
    }

    public async Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var items = await this.ToList(ct).ConfigureAwait(false);
        var compiled = selector.Compile();
        return items.Min(compiled)!;
    }

    public async Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var items = await this.ToList(ct).ConfigureAwait(false);
        var compiled = selector.Compile();
        object result = items.Select(compiled).Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return (TValue)result;
    }

    public async Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
    {
        var items = await this.ToList(ct).ConfigureAwait(false);
        var compiled = selector.Compile();
        return items.Average(x => Convert.ToDouble(compiled(x)));
    }

    // ── Internal ────────────────────────────────────────────────────────

    internal FilterDefinition<BsonDocument> BuildFilter()
    {
        var effective = this.GetEffectivePredicateExpressions().ToList();
        if (effective.Count == 0)
            return Builders<BsonDocument>.Filter.Empty;

        var translated = effective
            .Select(p => MongoExpressionVisitor.Translate(p, this.store.JsonOptions, this.typeInfo))
            .ToList();

        return translated.Count == 1
            ? translated[0]
            : Builders<BsonDocument>.Filter.And(translated);
    }

    internal SortDefinition<BsonDocument>? BuildSort()
    {
        if (this.orderBys.Count == 0)
            return null;

        var sorts = new List<SortDefinition<BsonDocument>>();
        foreach (var (selector, desc) in this.orderBys)
        {
            var field = ResolveOrderField(selector);
            sorts.Add(desc
                ? Builders<BsonDocument>.Sort.Descending(field)
                : Builders<BsonDocument>.Sort.Ascending(field));
        }
        return sorts.Count == 1 ? sorts[0] : Builders<BsonDocument>.Sort.Combine(sorts);
    }

    string ResolveOrderField<TVal>(Expression<Func<T, TVal>> selector)
    {
        var body = selector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;

        var parts = new List<string>();
        while (body is MemberExpression member)
        {
            var name = this.store.JsonOptions.PropertyNamingPolicy?.ConvertName(member.Member.Name) ?? member.Member.Name;
            parts.Insert(0, name);
            body = member.Expression;
        }

        return $"{MongoFields.Data}.{string.Join(".", parts)}";
    }

    internal IEnumerable<T> MaterializeSync()
        => this.ToList(CancellationToken.None).GetAwaiter().GetResult();

    static TVal DynamicAdd<TVal>(TVal a, TVal b)
    {
        if (a is int ai && b is int bi) return (TVal)(object)(ai + bi);
        if (a is long al && b is long bl) return (TVal)(object)(al + bl);
        if (a is double ad && b is double bd) return (TVal)(object)(ad + bd);
        if (a is decimal am && b is decimal bm) return (TVal)(object)(am + bm);
        throw new NotSupportedException($"Sum is not supported for type {typeof(TVal).Name}");
    }

    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange(CancellationToken ct = default)
        => throw new NotSupportedException(
            "Per-query change observation is not supported by MongoDbDocumentStore.");
}

internal class MongoDbProjectedDocumentQuery<TSource, TResult> : IDocumentQuery<TResult>
    where TSource : class
    where TResult : class
{
    readonly MongoDbDocumentQuery<TSource> source;
    readonly Func<TSource, TResult> compiledSelector;

    internal MongoDbProjectedDocumentQuery(
        MongoDbDocumentQuery<TSource> source,
        Expression<Func<TSource, TResult>> selector,
        MongoDbDocumentStore store,
        JsonTypeInfo<TResult>? resultTypeInfo)
    {
        this.source = source;
        this.compiledSelector = selector.Compile();
    }

    IEnumerable<TResult> Materialize()
        => this.source.MaterializeSync().Select(this.compiledSelector);

    public IDocumentQuery<TResult> Where(Expression<Func<TResult, bool>> predicate)
        => throw new NotSupportedException("Cannot chain Where after Select.");

    public IDocumentQuery<TResult> IgnoreQueryFilters()
        => throw new NotSupportedException("Cannot call IgnoreQueryFilters after Select. Call it on the source query before projecting.");

    public IDocumentQuery<TResult> IgnoreQueryFilters(params string[] filterNames)
        => throw new NotSupportedException("Cannot call IgnoreQueryFilters after Select. Call it on the source query before projecting.");

    public IDocumentQuery<TResult> OrderBy(Expression<Func<TResult, object>> selector)
        => throw new NotSupportedException("Cannot chain OrderBy after Select.");

    public IDocumentQuery<TResult> OrderByDescending(Expression<Func<TResult, object>> selector)
        => throw new NotSupportedException("Cannot chain OrderByDescending after Select.");

    public IDocumentQuery<TResult> GroupBy(Expression<Func<TResult, object>> selector)
        => throw new NotSupportedException("Cannot chain GroupBy after Select.");

    public IDocumentQuery<TResult> Paginate(int offset, int take)
        => throw new NotSupportedException("Cannot chain Paginate after Select.");

    public IDocumentQuery<TNewResult> Select<TNewResult>(
        Expression<Func<TResult, TNewResult>> selector,
        JsonTypeInfo<TNewResult>? resultTypeInfo = null) where TNewResult : class
        => throw new NotSupportedException("Cannot chain Select after Select.");

    public Task<IReadOnlyList<TResult>> ToList(CancellationToken ct = default)
        => Task.FromResult((IReadOnlyList<TResult>)this.Materialize().ToList().AsReadOnly());

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
        return Task.FromResult(this.Materialize().Max(compiled))!;
    }

    public Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        return Task.FromResult(this.Materialize().Min(compiled))!;
    }

    public Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        object result = this.Materialize().Select(compiled).Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return Task.FromResult((TValue)result);
    }

    public Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
        return Task.FromResult(this.Materialize().Average(x => Convert.ToDouble(compiled(x))));
    }

    public IAsyncEnumerable<DocumentChange<TResult>> NotifyOnChange(CancellationToken ct = default)
        => throw new NotSupportedException(
            "Per-query change observation is not supported by MongoDbDocumentStore.");

    static TVal DynamicAdd<TVal>(TVal a, TVal b)
    {
        if (a is int ai && b is int bi) return (TVal)(object)(ai + bi);
        if (a is long al && b is long bl) return (TVal)(object)(al + bl);
        if (a is double ad && b is double bd) return (TVal)(object)(ad + bd);
        if (a is decimal am && b is decimal bm) return (TVal)(object)(am + bm);
        throw new NotSupportedException($"Sum is not supported for type {typeof(TVal).Name}");
    }
}
