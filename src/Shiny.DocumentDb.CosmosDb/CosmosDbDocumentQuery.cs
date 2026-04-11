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
    readonly List<Expression<Func<T, bool>>> predicates = new();
    readonly List<(Expression<Func<T, object>> Selector, bool Descending)> orderBys = new();
    int? skipCount;
    int? takeCount;

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
    }

    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate)
    {
        var clone = new CosmosDbDocumentQuery<T>(this);
        clone.predicates.Add(predicate);
        return clone;
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

    public IDocumentQuery<T> GroupBy(Expression<Func<T, object>> selector)
    {
        // GroupBy requires special handling with aggregates — for now, not supported as a standalone operation
        throw new NotSupportedException("GroupBy is only supported with Select projections containing aggregate functions.");
    }

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = new CosmosDbDocumentQuery<T>(this);
        clone.skipCount = offset;
        clone.takeCount = take;
        return clone;
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
        return await this.store.ExecuteQueryAsync(container, queryDef, typeName, this.typeInfo, ct).ConfigureAwait(false);
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

        foreach (var id in ids)
        {
            await container.DeleteItemAsync<CosmosDocument>(id, new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);
        }

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

        var allParams = new Dictionary<string, object?>();
        var sb = new StringBuilder();
        sb.Append($"SELECT {selectClause} FROM c WHERE c.typeName = @typeName");
        allParams["@typeName"] = typeName;

        // Build WHERE predicates
        foreach (var predicate in this.predicates)
        {
            var (predicateSql, predicateParams) = CosmosExpressionVisitor.Translate(predicate, this.store.JsonOptions, this.typeInfo);
            sb.Append($" AND ({predicateSql})");
            foreach (var kv in predicateParams)
            {
                // Remap parameter names to avoid collisions
                var newName = $"@p{allParams.Count}";
                sb.Replace(kv.Key, newName);
                allParams[newName] = kv.Value;
            }
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

        var queryDef = new QueryDefinition(sb.ToString());
        foreach (var kv in allParams)
            queryDef.WithParameter(kv.Key, kv.Value);

        return (queryDef, typeName, container);
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
        this.compiledSelector = selector.Compile();
        this.store = store;
        this.resultTypeInfo = resultTypeInfo;
    }

    IEnumerable<TResult> Materialize()
        => this.source.MaterializeSync().Select(this.compiledSelector);

    public IDocumentQuery<TResult> Where(Expression<Func<TResult, bool>> predicate)
        => throw new NotSupportedException("Cannot chain Where after Select.");

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
        var items = this.Materialize().Select(compiled);
        object result = items.Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return Task.FromResult((TValue)result);
    }

    public Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default)
    {
        var compiled = selector.Compile();
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
}
