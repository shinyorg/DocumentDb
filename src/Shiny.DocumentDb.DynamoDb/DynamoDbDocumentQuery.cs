using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.DynamoDb;

/// <summary>
/// <see cref="IDocumentQuery{T}"/> for Amazon DynamoDB. Predicates over <c>MapIndexedProperty</c> attributes
/// push down into the server-side filter expression; the full predicate, ordering, and paging are applied by
/// <see cref="DocumentQueryBase{T}"/> over the returned candidates.
/// </summary>
public class DynamoDbDocumentQuery<T> : DocumentQueryBase<T> where T : class
{
    readonly DynamoDbDocumentStore store;

    internal DynamoDbDocumentQuery(DynamoDbDocumentStore store, JsonTypeInfo<T>? typeInfo)
        : base(store.BuildQueryContext(typeInfo))
        => this.store = store;

    DynamoDbDocumentQuery(DynamoDbDocumentQuery<T> source) : base(source)
        => this.store = source.store;

    protected override DocumentQueryBase<T> Clone() => new DynamoDbDocumentQuery<T>(this);

    protected override async Task<QueryExecution<T>> ExecuteAsync(QueryPlan<T> plan, CancellationToken ct)
    {
        var pushdown = this.store.BuildPushdown(plan.Predicates);
        var items = new List<T>();
        await foreach (var doc in this.store.LoadDocumentsAsync(this.TypeInfo, pushdown, ct).ConfigureAwait(false))
            items.Add(doc);
        return QueryExecution<T>.Candidates(items);
    }

    protected override Task<int> DeleteMatchingAsync(QueryPlan<T> plan, CancellationToken ct)
        => this.store.DeleteWhereAsync(plan.CompilePredicate(), this.TypeInfo, ct);

    protected override Task<int> SetPropertyMatchingAsync(QueryPlan<T> plan, string jsonPath, object? value, CancellationToken ct)
        => this.store.UpdatePropertyWhereAsync(plan.CompilePredicate(), jsonPath, value, this.TypeInfo, ct);

    protected override IAsyncEnumerable<DocumentChange<T>> ObserveChanges(CancellationToken ct)
        => this.store.Broadcaster.Observe<T>(ct);

    public override DocumentQueryString ToQueryString()
    {
        var partitionKey = this.store.ResolvePartitionKeyFor<T>();
        var pushdown = this.store.BuildPushdown(this.BuildPredicatePlan().Predicates);
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
}
