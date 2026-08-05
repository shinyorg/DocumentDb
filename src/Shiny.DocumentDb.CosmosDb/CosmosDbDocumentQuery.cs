using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

/// <summary>
/// <see cref="IDocumentQuery{T}"/> for Azure Cosmos DB. Predicates, ordering, paging, COUNT and the scalar
/// aggregates are translated into Cosmos SQL and run server-side, so <see cref="DocumentQueryBase{T}"/>
/// re-applies nothing; grouping and cursor pagination stay client-side over the filtered set.
/// </summary>
public class CosmosDbDocumentQuery<T> : DocumentQueryBase<T> where T : class
{
    readonly CosmosDbDocumentStore store;

    internal CosmosDbDocumentQuery(CosmosDbDocumentStore store, JsonTypeInfo<T>? typeInfo)
        : base(store.BuildQueryContext(typeInfo))
        => this.store = store;

    CosmosDbDocumentQuery(CosmosDbDocumentQuery<T> source) : base(source)
        => this.store = source.store;

    protected override DocumentQueryBase<T> Clone() => new CosmosDbDocumentQuery<T>(this);

    protected override async Task<QueryExecution<T>> ExecuteAsync(QueryPlan<T> plan, CancellationToken ct)
    {
        var (queryDef, typeName, container) = await this.BuildQueryAsync(plan, "c.data", ct).ConfigureAwait(false);
        var list = await this.store.ExecuteQueryAsync(container, queryDef, typeName, this.TypeInfo, ct).ConfigureAwait(false);
        ComputedReadBack.Apply(list, this.store.Options.ResolveComputedMappings(typeof(T)));
        return QueryExecution<T>.Complete(list);
    }

    // Cosmos satisfies the whole plan server-side and stores the body as JSON, so the raw terminals skip the
    // base's materialize-and-re-serialize fallback entirely.
    protected override async IAsyncEnumerable<string> MaterializeRawAsync(QueryPlan<T> plan, [EnumeratorCancellation] CancellationToken ct)
    {
        var (queryDef, typeName, container) = await this.BuildQueryAsync(plan, "c.data", ct).ConfigureAwait(false);
        await foreach (var raw in this.store.ExecuteRawQueryAsync(container, queryDef, typeName, ct).ConfigureAwait(false))
            yield return raw;
    }

    protected override async Task<long> CountCore(QueryPlan<T> plan, CancellationToken ct)
    {
        var (queryDef, typeName, container) = await this.BuildQueryAsync(plan, "VALUE COUNT(1)", ct, isAggregate: true).ConfigureAwait(false);
        return await this.store.ExecuteCountQueryAsync(container, queryDef, typeName, ct).ConfigureAwait(false);
    }

    protected override async Task<bool> AnyCore(QueryPlan<T> plan, CancellationToken ct)
        => await this.CountCore(plan, ct).ConfigureAwait(false) > 0;

    protected override Task<TValue> MaxCore<TValue>(QueryPlan<T> plan, Expression<Func<T, TValue>> selector, CancellationToken ct)
        => this.ScalarAggregate<TValue>(plan, "MAX", this.ResolveDataPath(selector), ct);

    protected override Task<TValue> MinCore<TValue>(QueryPlan<T> plan, Expression<Func<T, TValue>> selector, CancellationToken ct)
        => this.ScalarAggregate<TValue>(plan, "MIN", this.ResolveDataPath(selector), ct);

    protected override Task<TValue> SumCore<TValue>(QueryPlan<T> plan, Expression<Func<T, TValue>> selector, CancellationToken ct)
        => this.ScalarAggregate<TValue>(plan, "SUM", this.ResolveDataPath(selector), ct);

    protected override Task<double> AverageCore(QueryPlan<T> plan, Expression<Func<T, object>> selector, CancellationToken ct)
        => this.ScalarAggregate<double>(plan, "AVG", this.ResolveDataPath(selector), ct);

    async Task<TValue> ScalarAggregate<TValue>(QueryPlan<T> plan, string fn, string jsonPath, CancellationToken ct)
    {
        var (queryDef, typeName, container) = await this.BuildQueryAsync(plan, $"VALUE {fn}({jsonPath})", ct, isAggregate: true).ConfigureAwait(false);
        return await ExecuteScalarAsync<TValue>(container, queryDef, typeName, ct).ConfigureAwait(false);
    }

    protected override async Task<int> DeleteMatchingAsync(QueryPlan<T> plan, CancellationToken ct)
    {
        var typeName = this.Context.TypeName;
        var container = await this.store.GetContainerForTypeAsync<T>(ct).ConfigureAwait(false);
        var (queryDef, _, _) = await this.BuildQueryAsync(plan, "c.id", ct).ConfigureAwait(false);

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
        return ids.Count;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "The field-update value is a scalar in every supported usage (string/number/bool/date/Guid/enum), "
                      + "all of which have built-in converters. A complex object here is not trim-safe — the relational "
                      + "providers funnel the same value through DocumentStore.ToJsonLiteral, which is scalar-only; "
                      + "unifying Mongo/Cosmos onto it would change enum and complex-value formatting, so it is tracked "
                      + "separately rather than folded into an AOT fix.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Same scalar-value path; see the IL2026 justification.")]
    protected override Task<int> SetPropertyMatchingAsync(QueryPlan<T> plan, string jsonPath, object? value, CancellationToken ct)
        => this.SetPropertiesMatchingAsync(plan, [(jsonPath, value)], ct);

    // Cosmos rewrites each matched item, so every assignment is applied in the same pass — N properties cost one
    // read-modify-write per document rather than N.
    protected override async Task<int> SetPropertiesMatchingAsync(QueryPlan<T> plan, IReadOnlyList<(string JsonPath, object? Value)> assignments, CancellationToken ct)
    {
        var typeName = this.Context.TypeName;
        var container = await this.store.GetContainerForTypeAsync<T>(ct).ConfigureAwait(false);

        // The whole item is selected, not just id+data: ReplaceItemAsync writes the body back verbatim, so a
        // narrower projection would null out typeName — the partition key — and Cosmos answers with a 404.
        var (queryDef, _, _) = await this.BuildQueryAsync(plan, "*", ct).ConfigureAwait(false);
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
                foreach (var (jsonPath, value) in assignments)
                {
                    node[jsonPath] = value == null
                        ? null
                        : System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(value, this.Context.JsonOptions));
                }
                doc.Data = node.ToJsonString();
                doc.UpdatedAt = DateTimeOffset.UtcNow.ToString("o");

                await container.ReplaceItemAsync(doc, doc.Id, new PartitionKey(typeName), cancellationToken: ct).ConfigureAwait(false);
                count++;
            }
        }
        return count;
    }

    protected override IAsyncEnumerable<DocumentChange<T>> ObserveChanges(CancellationToken ct)
        => throw new NotSupportedException("Per-query change observation is not supported by CosmosDbDocumentStore.");

    protected override Task<IReadOnlyList<FullTextResult<T>>> FullTextSearchCore(
        string searchText, int maxResults, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => this.store.FullTextSearch(searchText, maxResults, filter, ct);

    protected override Task<IReadOnlyList<VectorResult<T>>> NearestVectorsCore(
        ReadOnlyMemory<float> query, int k, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => this.store.NearestVectors(query, k, filter, ct);

    public override IGroupedDocumentQuery<T, object> GroupBy(string keyField)
        => throw new NotSupportedException(
            "String GroupBy(\"field\") is not supported on the Cosmos provider. Use the typed " +
            "GroupBy(keySelector).Select(g => …) form, or a relational store for the string grammar.");

    public override DocumentQueryString ToQueryString()
    {
        var (sql, parameters) = this.BuildQuerySql(this.BuildPlan(), "c.data");
        return new DocumentQueryString(sql, parameters);
    }

    internal async Task<(QueryDefinition queryDef, string typeName, Container container)> BuildQueryAsync(
        QueryPlan<T> plan,
        string selectClause,
        CancellationToken ct,
        bool isAggregate = false)
    {
        var typeName = this.Context.TypeName;
        var container = await this.store.GetContainerForTypeAsync<T>(ct).ConfigureAwait(false);

        var (sql, allParams) = this.BuildQuerySql(plan, selectClause, isAggregate);
        var queryDef = new QueryDefinition(sql);
        foreach (var kv in allParams)
            queryDef.WithParameter(kv.Key, kv.Value);

        return (queryDef, typeName, container);
    }

    /// <summary>
    /// Builds the Cosmos SQL text and parameter values for the given SELECT clause without touching the
    /// container — shared by execution and <see cref="ToQueryString"/>.
    /// </summary>
    internal (string Sql, Dictionary<string, object?> Parameters) BuildQuerySql(QueryPlan<T> plan, string selectClause, bool isAggregate = false)
    {
        var typeName = this.Context.TypeName;
        var allParams = new Dictionary<string, object?>();
        var sb = new StringBuilder();
        sb.Append($"SELECT {selectClause} FROM c WHERE c.typeName = @typeName");
        allParams["@typeName"] = typeName;

        // Each predicate is translated with a start ordinal so its @pN names are globally unique — no post-hoc
        // string remapping (which corrupts predicates with multiple parameters).
        var paramOrdinal = 0;
        foreach (var predicate in plan.Predicates)
        {
            var (predicateSql, predicateParams) = CosmosExpressionVisitor.Translate(predicate, this.Context.JsonOptions, this.TypeInfo, this.store.Options.Mappings.SpatialJsonPathsFor(typeof(T)), paramOrdinal);
            sb.Append($" AND ({predicateSql})");
            foreach (var kv in predicateParams)
                allParams[kv.Key] = kv.Value;
            paramOrdinal += predicateParams.Count;
        }

        if (!isAggregate && plan.OrderBy.Count > 0)
        {
            var orderParts = plan.OrderBy.Select(o =>
            {
                var path = this.ResolveDataPath((Expression<Func<T, object>>)o.Selector);
                return o.Descending ? $"{path} DESC" : $"{path} ASC";
            });
            sb.Append($" ORDER BY {String.Join(", ", orderParts)}");
        }

        if (!isAggregate && (plan.Skip.HasValue || plan.Take.HasValue))
            sb.Append($" OFFSET {plan.Skip ?? 0} LIMIT {plan.Take ?? Int32.MaxValue}");

        return (sb.ToString(), allParams);
    }

    string ResolveDataPath<TVal>(Expression<Func<T, TVal>> selector)
    {
        var body = selector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;

        var parts = new List<string>();
        while (body is MemberExpression member)
        {
            var name = this.Context.JsonOptions.PropertyNamingPolicy?.ConvertName(member.Member.Name) ?? member.Member.Name;
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
