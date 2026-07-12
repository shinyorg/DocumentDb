using Shiny.DocumentDb.Internal.Query;
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

    public IGroupedDocumentQuery<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        => new InMemoryGroupedQuery<T, TKey>(this.MaterializeForGroupingAsync, ExpressionInterpreter.Interpret(keySelector));

    public IGroupedDocumentQuery<T, object> GroupBy(string keyField)
        => throw new NotSupportedException(
            "String GroupBy(\"field\") is not supported on the MongoDB provider. Use the typed " +
            "GroupBy(keySelector).Select(g => …) form, or a relational store for the string grammar.");

    // Filtered source only (no sort/skip/take) — the documents a grouped query aggregates over.
    internal async Task<IEnumerable<T>> MaterializeForGroupingAsync()
        => await this.store.ExecuteFindAsync(this.BuildFilter(), null, null, null, this.typeInfo, CancellationToken.None).ConfigureAwait(false);

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = new MongoDbDocumentQuery<T>(this);
        clone.skipCount = offset;
        clone.takeCount = take;
        return clone;
    }

    // MongoDB pages the cursor client-side over the filtered set (as its GroupBy does) — the same stable
    // keyset contract as the relational engine, but without the server-side seek.
    public Task<CursorPage<T>> ToCursorPage(string? cursor, int take, CancellationToken ct = default)
        => this.store.Tracker.Track("query.paginate", typeof(T).Name, () => this.ToCursorPageImpl(cursor, take, ct));

    async Task<CursorPage<T>> ToCursorPageImpl(string? cursor, int take, CancellationToken ct)
    {
        var keys = new List<CursorSortKey<T>>(this.orderBys.Count);
        var specParts = new List<string>(this.orderBys.Count);
        foreach (var (selector, descending) in this.orderBys)
        {
            var getter = ExpressionInterpreter.Interpret<T, object>(selector);
            keys.Add(new CursorSortKey<T>(getter, descending));
            specParts.Add($"{selector}:{(descending ? "d" : "a")}");
        }

        var accessor = this.store.IdCache.GetOrCreate(this.typeInfo);
        var spec = this.store.ResolveTypeNameFor<T>() + "|" + string.Join("|", specParts);
        var matching = (await this.MaterializeForGroupingAsync().ConfigureAwait(false)).ToList();
        ComputedReadBack.Apply(matching, this.store.Options.ResolveComputedMappings(typeof(T)));
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
        => new MongoDbProjectedDocumentQuery<T, TResult>(this, selector, this.store, resultTypeInfo);

    public Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
        => this.store.Tracker.Track("query.to_list", typeof(T).Name, () => this.ToListImpl(ct), r => r.Count);

    async Task<IReadOnlyList<T>> ToListImpl(CancellationToken ct)
    {
        var filter = this.BuildFilter();
        var sort = this.BuildSort();
        var list = await this.store.ExecuteFindAsync(filter, sort, this.skipCount, this.takeCount, this.typeInfo, ct).ConfigureAwait(false);
        Shiny.DocumentDb.Internal.ComputedReadBack.Apply(list, this.store.Options.ResolveComputedMappings(typeof(T)));
        return list;
    }

    public DocumentQueryString ToQueryString()
    {
        var registry = MongoDB.Bson.Serialization.BsonSerializer.SerializerRegistry;
        var serializer = registry.GetSerializer<BsonDocument>();
        var args = new RenderArgs<BsonDocument>(serializer, registry);

        var filterDoc = this.BuildFilter().Render(args);
        var sort = this.BuildSort();

        // No ordering/pagination → return just the translated filter; otherwise render the full find command.
        BsonDocument query;
        if (sort == null && !this.skipCount.HasValue && !this.takeCount.HasValue)
        {
            query = filterDoc;
        }
        else
        {
            query = new BsonDocument { { "filter", filterDoc } };
            if (sort != null)
                query.Add("sort", sort.Render(args));
            if (this.skipCount.HasValue)
                query.Add("skip", this.skipCount.Value);
            if (this.takeCount.HasValue)
                query.Add("limit", this.takeCount.Value);
        }

        var json = query.ToJson(new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.RelaxedExtendedJson });
        return new DocumentQueryString(json, new Dictionary<string, object?>());
    }

    public async IAsyncEnumerable<T> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        var list = await this.ToList(ct).ConfigureAwait(false);
        foreach (var item in list)
            yield return item;
    }

    public Task<long> Count(CancellationToken ct = default)
        => this.store.Tracker.Track("query.count", typeof(T).Name, () => this.store.ExecuteCountAsync<T>(this.BuildFilter(), ct), r => r);

    public Task<bool> Any(CancellationToken ct = default)
        => this.store.Tracker.Track("query.any", typeof(T).Name, () => this.AnyImpl(ct), r => r ? 1 : 0);

    async Task<bool> AnyImpl(CancellationToken ct)
    {
        var count = await this.Count(ct).ConfigureAwait(false);
        return count > 0;
    }

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => this.store.Tracker.Track("query.execute_delete", typeof(T).Name, () => this.store.ExecuteDeleteAsync<T>(this.BuildFilter(), ct), r => r);

    public Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
        => this.store.Tracker.Track("query.execute_update", typeof(T).Name, () => this.ExecuteUpdateImpl(property, value, ct), r => r);

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    Task<int> ExecuteUpdateImpl(Expression<Func<T, object>> property, object? value, CancellationToken ct)
    {
        var jsonPath = this.typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions, this.typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions);
        return this.store.ExecuteUpdatePropertyAsync<T>(this.BuildFilter(), jsonPath, value, ct);
    }

    public Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.max", typeof(T).Name, () => this.MaxImpl(selector, ct));

    async Task<TValue> MaxImpl<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct)
    {
        var items = await this.ToList(ct).ConfigureAwait(false);
        var compiled = ExpressionInterpreter.Interpret(selector);
        return items.Max(compiled)!;
    }

    public Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.min", typeof(T).Name, () => this.MinImpl(selector, ct));

    async Task<TValue> MinImpl<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct)
    {
        var items = await this.ToList(ct).ConfigureAwait(false);
        var compiled = ExpressionInterpreter.Interpret(selector);
        return items.Min(compiled)!;
    }

    public Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.sum", typeof(T).Name, () => this.SumImpl(selector, ct));

    async Task<TValue> SumImpl<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct)
    {
        var items = await this.ToList(ct).ConfigureAwait(false);
        var compiled = ExpressionInterpreter.Interpret(selector);
        object result = items.Select(compiled).Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return (TValue)result;
    }

    public Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.average", typeof(T).Name, () => this.AverageImpl(selector, ct));

    async Task<double> AverageImpl(Expression<Func<T, object>> selector, CancellationToken ct)
    {
        var items = await this.ToList(ct).ConfigureAwait(false);
        var compiled = ExpressionInterpreter.Interpret(selector);
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

    public Task<IReadOnlyList<FullTextResult<T>>> FullTextMatch(string searchText, int maxResults = 50, CancellationToken ct = default)
    {
        var effective = this.GetEffectivePredicateExpressions().ToList();
        Expression<Func<T, bool>>? filter = effective.Count == 0
            ? null
            : DocumentQuery<T>.CombinePredicates(effective);
        return this.store.FullTextSearch(searchText, maxResults, filter, ct);
    }
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
        this.compiledSelector = ExpressionInterpreter.Interpret(selector);
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

    public IGroupedDocumentQuery<TResult, TKey> GroupBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
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
        var compiled = ExpressionInterpreter.Interpret(selector);
        return Task.FromResult(this.Materialize().Max(compiled))!;
    }

    public Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        return Task.FromResult(this.Materialize().Min(compiled))!;
    }

    public Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        object result = this.Materialize().Select(compiled).Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return Task.FromResult((TValue)result);
    }

    public Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
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
