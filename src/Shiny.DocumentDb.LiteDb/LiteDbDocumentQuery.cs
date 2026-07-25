using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.LiteDb;

/// <summary>
/// LiteDB has no JSON predicate push-down, so the query hands the type's documents to
/// <see cref="DocumentQueryBase{T}"/> as candidates and the shared machinery filters, orders, and pages them.
/// </summary>
public class LiteDbDocumentQuery<T> : DocumentQueryBase<T> where T : class
{
    readonly LiteDbDocumentStore store;

    internal LiteDbDocumentQuery(LiteDbDocumentStore store, JsonTypeInfo<T>? typeInfo)
        : base(store.BuildQueryContext(typeInfo))
        => this.store = store;

    LiteDbDocumentQuery(LiteDbDocumentQuery<T> source) : base(source)
        => this.store = source.store;

    protected override DocumentQueryBase<T> Clone() => new LiteDbDocumentQuery<T>(this);

    protected override Task<QueryExecution<T>> ExecuteAsync(QueryPlan<T> plan, CancellationToken ct)
        => Task.FromResult(QueryExecution<T>.Candidates(
            this.store.LoadDocuments(this.Context.TypeName, this.TypeInfo)));

    protected override Task<int> DeleteMatchingAsync(QueryPlan<T> plan, CancellationToken ct)
        => Task.FromResult(this.store.DeleteDocuments(this.Context.TypeName, plan.CompilePredicate(), this.TypeInfo));

    protected override Task<int> SetPropertyMatchingAsync(QueryPlan<T> plan, string jsonPath, object? value, CancellationToken ct)
        => Task.FromResult(this.store.UpdateDocumentProperty(
            this.Context.TypeName, plan.CompilePredicate(), jsonPath, value, this.TypeInfo));

    protected override IAsyncEnumerable<DocumentChange<T>> ObserveChanges(CancellationToken ct)
        => this.store.Broadcaster.Observe<T>(ct);

    protected override Task<IReadOnlyList<FullTextResult<T>>> FullTextSearchCore(
        string searchText, int maxResults, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => this.store.FullTextSearch(searchText, maxResults, filter, ct);
}
