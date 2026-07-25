using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.AzureTable;

/// <summary>
/// <see cref="IDocumentQuery{T}"/> for Azure Table Storage. Predicates over <c>MapIndexedProperty</c> columns
/// push down into the server-side OData filter; everything else — the full predicate, ordering, paging —
/// is applied by <see cref="DocumentQueryBase{T}"/> over the returned candidates.
/// </summary>
public class AzureTableDocumentQuery<T> : DocumentQueryBase<T> where T : class
{
    readonly AzureTableDocumentStore store;

    internal AzureTableDocumentQuery(AzureTableDocumentStore store, JsonTypeInfo<T>? typeInfo)
        : base(store.BuildQueryContext(typeInfo))
        => this.store = store;

    AzureTableDocumentQuery(AzureTableDocumentQuery<T> source) : base(source)
        => this.store = source.store;

    protected override DocumentQueryBase<T> Clone() => new AzureTableDocumentQuery<T>(this);

    protected override async Task<QueryExecution<T>> ExecuteAsync(QueryPlan<T> plan, CancellationToken ct)
    {
        var pushdown = this.store.BuildPushdownFilter(plan.Predicates);
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
        var pushdown = this.store.BuildPushdownFilter(this.BuildPredicatePlan().Predicates);
        var filter = $"PartitionKey eq '{partitionKey}'";
        if (!String.IsNullOrEmpty(pushdown))
            filter += $" and ({pushdown})";
        return new DocumentQueryString(filter, new Dictionary<string, object?>());
    }
}
