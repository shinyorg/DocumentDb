using Shiny.DocumentDb.Internal.Query;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

public class CosmosDbDocumentQuery<T> : IDocumentQuery<T> where T : class
{
    readonly CosmosDbDocumentStore store;
    readonly JsonTypeInfo<T>? typeInfo;

    public JsonTypeInfo<T>? QueryTypeInfo => this.typeInfo;
    readonly List<Expression<Func<T, bool>>> predicates = new();
    readonly List<(Expression<Func<T, object>> Selector, bool Descending)> orderBys = new();
    int? skipCount;
    int? takeCount;
    bool ignoreAllFilters;
    HashSet<string>? ignoredFilterNames;

    internal CosmosDbDocumentQuery(CosmosDbDocumentStore store, JsonTypeInfo<T>? typeInfo)
    {
        this.store = store;
        this.typeInfo = typeInfo;
    }

    CosmosDbDocumentQuery(CosmosDbDocumentQuery<T> source)
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
        var clone = new CosmosDbDocumentQuery<T>(this);
        clone.predicates.Add(predicate);
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters()
    {
        var clone = new CosmosDbDocumentQuery<T>(this);
        clone.ignoreAllFilters = true;
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames)
    {
        ArgumentNullException.ThrowIfNull(filterNames);
        var clone = new CosmosDbDocumentQuery<T>(this);
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
        var clone = new CosmosDbDocumentQuery<T>(this);
        clone.orderBys.Add((selector, false));
        return clone;
    }

    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector)
    {
        var clone = new CosmosDbDocumentQuery<T>(this);
        clone.orderBys.Add((selector, true));
        return clone;
    }

    public IGroupedDocumentQuery<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        => new InMemoryGroupedQuery<T, TKey>(this.MaterializeForGroupingAsync, ExpressionInterpreter.Interpret(keySelector));

    public IGroupedDocumentQuery<T, object> GroupBy(string keyField)
        => throw new NotSupportedException(
            "String GroupBy(\"field\") is not supported on the Cosmos provider. Use the typed " +
            "GroupBy(keySelector).Select(g => …) form, or a relational store for the string grammar.");

    // Filtered source only (no sort/skip/take) — the documents a grouped query aggregates over.
    internal async Task<IEnumerable<T>> MaterializeForGroupingAsync()
    {
        var filtered = new CosmosDbDocumentQuery<T>(this);
        filtered.orderBys.Clear();
        filtered.skipCount = null;
        filtered.takeCount = null;
        return await filtered.ToList(CancellationToken.None).ConfigureAwait(false);
    }

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = new CosmosDbDocumentQuery<T>(this);
        clone.skipCount = offset;
        clone.takeCount = take;
        return clone;
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
        // Use in-memory projection for simplicity and correctness
        return new CosmosDbProjectedDocumentQuery<T, TResult>(this, selector, this.store, resultTypeInfo);
    }

    public async Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
    {
        var (queryDef, typeName, container) = await this.BuildQueryAsync("c.data", ct).ConfigureAwait(false);
        var list = await this.store.ExecuteQueryAsync(container, queryDef, typeName, this.typeInfo, ct).ConfigureAwait(false);
        ComputedReadBack.Apply(list, this.store.Options.ResolveComputedMappings(typeof(T)));
        return list;
    }

    public async IAsyncEnumerable<T> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        var list = await this.ToList(ct).ConfigureAwait(false);
        foreach (var item in list)
            yield return item;
    }

    public async Task<long> Count(CancellationToken ct = default)
    {
        var (queryDef, typeName, container) = await this.BuildQueryAsync("VALUE COUNT(1)", ct, isAggregate: true).ConfigureAwait(false);
        return await this.store.ExecuteCountQueryAsync(container, queryDef, typeName, ct).ConfigureAwait(false);
    }

    public async Task<bool> Any(CancellationToken ct = default)
    {
        var count = await this.Count(ct).ConfigureAwait(false);
        return count > 0;
    }

    public async Task<int> ExecuteDelete(CancellationToken ct = default)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var container = await this.store.GetContainerForTypeAsync<T>(ct).ConfigureAwait(false);

        var interceptors = this.store.InterceptorPipeline;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Delete, typeName);
        await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false);

        // Query matching IDs, then delete each
        var (queryDef, _, _) = await this.BuildQueryAsync("c.id", ct).ConfigureAwait(false);
        var ids = new List<string>();

        using var iterator = container.GetItemQueryIterator<CosmosDocument>(queryDef, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
            ids.AddRange(response.Select(d => d.Id));
        }

        await CosmosDbDocumentStore.DeleteItemsConcurrentlyAsync(container, typeName, ids, ct).ConfigureAwait(false);

        await interceptors.AfterBulk(bulkCtx, ids.Count, ct).ConfigureAwait(false);
        return ids.Count;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    public async Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var container = await this.store.GetContainerForTypeAsync<T>(ct).ConfigureAwait(false);
        var jsonPath = this.typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions, this.typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions);

        var interceptors = this.store.InterceptorPipeline;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Update, typeName, assignment: (jsonPath, value));
        await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false);

        // Query matching docs, then update each
        var (queryDef, _, _) = await this.BuildQueryAsync("c.id, c.data", ct).ConfigureAwait(false);
        var count = 0;

        using var iterator = container.GetItemQueryIterator<CosmosDocument>(queryDef, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
            foreach (var doc in response)
            {
                var node = System.Text.Json.Nodes.JsonNode.Parse(doc.Data)!.AsObject();
                node[jsonPath] = value == null ? null : System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(value, this.store.JsonOptions));
                doc.Data = node.ToJsonString();
                doc.UpdatedAt = DateTimeOffset.UtcNow.ToString("o");

                await container.ReplaceItemAsync(doc, doc.Id, new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);
                count++;
            }
        }

        await interceptors.AfterBulk(bulkCtx, count, ct).ConfigureAwait(false);
        return count;
    }

    public async Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var jsonPath = ResolveDataPath(selector);
        var (queryDef, typeName, container) = await this.BuildQueryAsync($"VALUE MAX({jsonPath})", ct, isAggregate: true).ConfigureAwait(false);
        return await ExecuteScalarAsync<TValue>(container, queryDef, typeName, ct).ConfigureAwait(false);
    }

    public async Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var jsonPath = ResolveDataPath(selector);
        var (queryDef, typeName, container) = await this.BuildQueryAsync($"VALUE MIN({jsonPath})", ct, isAggregate: true).ConfigureAwait(false);
        return await ExecuteScalarAsync<TValue>(container, queryDef, typeName, ct).ConfigureAwait(false);
    }

    public async Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
    {
        var jsonPath = ResolveDataPath(selector);
        var (queryDef, typeName, container) = await this.BuildQueryAsync($"VALUE SUM({jsonPath})", ct, isAggregate: true).ConfigureAwait(false);
        return await ExecuteScalarAsync<TValue>(container, queryDef, typeName, ct).ConfigureAwait(false);
    }

    public async Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
    {
        var jsonPath = ResolveDataPath(selector);
        var (queryDef, typeName, container) = await this.BuildQueryAsync($"VALUE AVG({jsonPath})", ct, isAggregate: true).ConfigureAwait(false);
        return await this.store.ExecuteScalarDoubleQueryAsync(container, queryDef, typeName, ct).ConfigureAwait(false);
    }

    // ── Internal ────────────────────────────────────────────────────────

    internal async Task<(QueryDefinition queryDef, string typeName, Container container)> BuildQueryAsync(
        string selectClause,
        CancellationToken ct,
        bool isAggregate = false)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var container = await this.store.GetContainerForTypeAsync<T>(ct).ConfigureAwait(false);

        var (sql, allParams) = this.BuildQuerySql(selectClause, isAggregate);
        var queryDef = new QueryDefinition(sql);
        foreach (var kv in allParams)
            queryDef.WithParameter(kv.Key, kv.Value);

        return (queryDef, typeName, container);
    }

    /// <summary>
    /// Builds the Cosmos SQL text and parameter values for the given SELECT clause without touching the
    /// container — shared by execution (<see cref="BuildQueryAsync"/>) and <see cref="ToQueryString"/>.
    /// </summary>
    internal (string Sql, Dictionary<string, object?> Parameters) BuildQuerySql(string selectClause, bool isAggregate = false)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var allParams = new Dictionary<string, object?>();
        var sb = new StringBuilder();
        sb.Append($"SELECT {selectClause} FROM c WHERE c.typeName = @typeName");
        allParams["@typeName"] = typeName;

        // Build WHERE predicates (global filters + user predicates). Each predicate is translated with a
        // start ordinal so its @pN names are globally unique — no post-hoc string remapping (which corrupts
        // predicates with multiple parameters).
        var paramOrdinal = 0;
        foreach (var predicate in this.GetEffectivePredicateExpressions())
        {
            var (predicateSql, predicateParams) = CosmosExpressionVisitor.Translate(predicate, this.store.JsonOptions, this.typeInfo, paramOrdinal);
            sb.Append($" AND ({predicateSql})");
            foreach (var kv in predicateParams)
                allParams[kv.Key] = kv.Value;
            paramOrdinal += predicateParams.Count;
        }

        // ORDER BY (not valid for aggregates)
        if (!isAggregate && this.orderBys.Count > 0)
        {
            var orderParts = this.orderBys.Select(o =>
            {
                var path = ResolveDataPath(o.Selector);
                return o.Descending ? $"{path} DESC" : $"{path} ASC";
            });
            sb.Append($" ORDER BY {string.Join(", ", orderParts)}");
        }

        // OFFSET/LIMIT
        if (!isAggregate && (this.skipCount.HasValue || this.takeCount.HasValue))
        {
            sb.Append($" OFFSET {this.skipCount ?? 0} LIMIT {this.takeCount ?? int.MaxValue}");
        }

        return (sb.ToString(), allParams);
    }

    public DocumentQueryString ToQueryString()
    {
        var (sql, parameters) = this.BuildQuerySql("c.data");
        return new DocumentQueryString(sql, parameters);
    }

    internal IEnumerable<T> MaterializeSync()
    {
        return this.ToList(CancellationToken.None).GetAwaiter().GetResult();
    }

    string ResolveDataPath<TVal>(Expression<Func<T, TVal>> selector)
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

        return $"c.data.{string.Join(".", parts)}";
    }

    static async Task<TValue> ExecuteScalarAsync<TValue>(Container container, QueryDefinition queryDef, string typeName, CancellationToken ct)
    {
        using var iterator = container.GetItemQueryIterator<TValue>(queryDef, requestOptions: new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(typeName)
        });

        var response = await iterator.ReadNextAsync(ct).ConfigureAwait(false);
        return response.FirstOrDefault()!;
    }

    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange(CancellationToken ct = default)
        => throw new NotSupportedException(
            "Per-query change observation is not supported by CosmosDbDocumentStore. " +
            "Use SubscribeChanges<T>() to consume the native Cosmos change feed.");

    public Task<IReadOnlyList<FullTextResult<T>>> FullTextMatch(string searchText, int maxResults = 50, CancellationToken ct = default)
    {
        var effective = this.GetEffectivePredicateExpressions().ToList();
        Expression<Func<T, bool>>? filter = effective.Count == 0
            ? null
            : DocumentQuery<T>.CombinePredicates(effective);
        return this.store.FullTextSearch(searchText, maxResults, filter, ct);
    }
}

internal class CosmosDbProjectedDocumentQuery<TSource, TResult> : IDocumentQuery<TResult>
    where TSource : class
    where TResult : class
{
    readonly CosmosDbDocumentQuery<TSource> source;
    readonly Expression<Func<TSource, TResult>> selector;
    readonly Func<TSource, TResult> compiledSelector;
    readonly CosmosDbDocumentStore store;
    readonly JsonTypeInfo<TResult>? resultTypeInfo;

    internal CosmosDbProjectedDocumentQuery(
        CosmosDbDocumentQuery<TSource> source,
        Expression<Func<TSource, TResult>> selector,
        CosmosDbDocumentStore store,
        JsonTypeInfo<TResult>? resultTypeInfo)
    {
        this.source = source;
        this.selector = selector;
        this.compiledSelector = ExpressionInterpreter.Interpret(selector);
        this.store = store;
        this.resultTypeInfo = resultTypeInfo;
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
        var items = this.Materialize().Select(compiled);
        object result = items.Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return Task.FromResult((TValue)result);
    }

    public Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        return Task.FromResult(this.Materialize().Average(x => Convert.ToDouble(compiled(x))));
    }

    static TVal DynamicAdd<TVal>(TVal a, TVal b)
    {
        if (a is int ai && b is int bi) return (TVal)(object)(ai + bi);
        if (a is long al && b is long bl) return (TVal)(object)(al + bl);
        if (a is double ad && b is double bd) return (TVal)(object)(ad + bd);
        if (a is decimal am && b is decimal bm) return (TVal)(object)(am + bm);
        throw new NotSupportedException($"Sum is not supported for type {typeof(TVal).Name}");
    }

    public IAsyncEnumerable<DocumentChange<TResult>> NotifyOnChange(CancellationToken ct = default)
        => throw new NotSupportedException(
            "Per-query change observation is not supported by CosmosDbDocumentStore.");
}
