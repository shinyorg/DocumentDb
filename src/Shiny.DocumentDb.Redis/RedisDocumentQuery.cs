using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.Redis;

/// <summary>
/// <see cref="IDocumentQuery{T}"/> for Redis Stack. Candidate documents are resolved by an <c>FT.SEARCH</c>
/// pushdown over declared index fields (or a full key scan when nothing pushes down); the full predicate,
/// ordering, and paging are then applied by <see cref="DocumentQueryBase{T}"/>, so the pushdown only ever
/// shrinks the candidate set.
/// </summary>
public class RedisDocumentQuery<T> : DocumentQueryBase<T> where T : class
{
    readonly RedisDocumentStore store;

    internal RedisDocumentQuery(RedisDocumentStore store, JsonTypeInfo<T>? typeInfo)
        : base(store.BuildQueryContext(typeInfo))
        => this.store = store;

    RedisDocumentQuery(RedisDocumentQuery<T> source) : base(source)
        => this.store = source.store;

    internal RedisDocumentStore Store => this.store;

    protected override DocumentQueryBase<T> Clone() => new RedisDocumentQuery<T>(this);

    protected override async Task<QueryExecution<T>> ExecuteAsync(QueryPlan<T> plan, CancellationToken ct)
    {
        var keys = await this.store.ResolveCandidateKeysAsync(plan.Predicates, ct).ConfigureAwait(false);
        var docs = await this.store.LoadDocumentsAsync(keys, this.TypeInfo, ct).ConfigureAwait(false);
        return QueryExecution<T>.Candidates(docs);
    }

    protected override Task<int> DeleteMatchingAsync(QueryPlan<T> plan, CancellationToken ct)
        => this.store.DeleteWhereAsync(plan.Predicates, this.TypeInfo, ct);

    protected override Task<int> SetPropertyMatchingAsync(QueryPlan<T> plan, string jsonPath, object? value, CancellationToken ct)
        => this.store.UpdatePropertyWhereAsync(plan.Predicates, jsonPath, value, this.TypeInfo, ct);

    protected override IAsyncEnumerable<DocumentChange<T>> ObserveChanges(CancellationToken ct)
        => this.store.Broadcaster.Observe<T>(ct);

    protected override Task<IReadOnlyList<FullTextResult<T>>> FullTextSearchCore(
        string searchText, int maxResults, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => this.store.FullTextSearch(searchText, maxResults, filter, ct);

    protected override Task<IReadOnlyList<VectorResult<T>>> NearestVectorsCore(
        ReadOnlyMemory<float> query, int k, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => this.store.NearestVectors(query, k, filter, ct);

    public override DocumentQueryString ToQueryString()
    {
        var query = RedisSearchQueryBuilder.BuildPushdown(this.BuildPredicatePlan().Predicates, this.store.IndexFieldsFor<T>()) ?? "*";
        return new DocumentQueryString(query, new Dictionary<string, object?>());
    }
}
