using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.IndexedDb;

/// <summary>
/// <see cref="IDocumentQuery{T}"/> for IndexedDB (Blazor WebAssembly). The object store has no predicate
/// push-down, so the type's documents are handed to <see cref="DocumentQueryBase{T}"/> as candidates and
/// filtered, ordered, and paged in the browser.
/// </summary>
public class IndexedDbDocumentQuery<T> : DocumentQueryBase<T> where T : class
{
    readonly IndexedDbDocumentStore store;

    internal IndexedDbDocumentQuery(IndexedDbDocumentStore store, JsonTypeInfo<T>? typeInfo)
        : base(store.BuildQueryContext(typeInfo))
        => this.store = store;

    IndexedDbDocumentQuery(IndexedDbDocumentQuery<T> source) : base(source)
        => this.store = source.store;

    protected override DocumentQueryBase<T> Clone() => new IndexedDbDocumentQuery<T>(this);

    protected override async Task<QueryExecution<T>> ExecuteAsync(QueryPlan<T> plan, CancellationToken ct)
        => QueryExecution<T>.Candidates(
            await this.store.LoadDocumentsAsync(this.Context.TypeName, this.TypeInfo).ConfigureAwait(false));

    protected override Task<int> DeleteMatchingAsync(QueryPlan<T> plan, CancellationToken ct)
        => this.store.DeleteDocumentsAsync(this.Context.TypeName, plan.CompilePredicate(), this.TypeInfo);

    protected override Task<int> SetPropertyMatchingAsync(QueryPlan<T> plan, string jsonPath, object? value, CancellationToken ct)
        => this.store.UpdateDocumentPropertyAsync(this.Context.TypeName, plan.CompilePredicate(), jsonPath, value, this.TypeInfo);

    protected override IAsyncEnumerable<DocumentChange<T>> ObserveChanges(CancellationToken ct)
        => throw new NotSupportedException("Per-query change observation is not supported by IndexedDbDocumentStore.");

    protected override Task<IReadOnlyList<FullTextResult<T>>> FullTextSearchCore(
        string searchText, int maxResults, Expression<Func<T, bool>>? filter, CancellationToken ct)
        => this.store.FullTextSearch(searchText, maxResults, filter, ct);
}
