using System.Linq.Expressions;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

public interface IDocumentQuery<T> where T : class
{
    /// <summary>
    /// The <see cref="JsonTypeInfo{T}"/> this query resolved when it was created (from the explicit
    /// argument to <c>Query</c> or the registered <c>JsonSerializerContext</c>). The string-based
    /// <c>Where</c> / <c>OrderBy</c> / <c>Project</c> helpers fall back to this when no
    /// <c>JsonTypeInfo</c> is supplied, so callers rarely need to pass one. Returns <c>null</c> when
    /// none could be resolved (reflection-only queries) — then a <c>JsonTypeInfo</c> must be passed explicitly.
    /// </summary>
    JsonTypeInfo<T>? QueryTypeInfo => null;

    /// <summary>
    /// Filters documents matching the given predicate. Multiple calls are combined with AND.
    /// </summary>
    IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate);

    /// <summary>
    /// Disables every global query filter registered for <typeparamref name="T"/> on this query
    /// (both named and unnamed). The query then sees the full unfiltered set.
    /// </summary>
    IDocumentQuery<T> IgnoreQueryFilters();

    /// <summary>
    /// Disables the named global query filters supplied. Other registered filters (named or unnamed)
    /// continue to apply.
    /// </summary>
    IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames);

    /// <summary>
    /// Sorts results by the selected property in ascending order.
    /// </summary>
    IDocumentQuery<T> OrderBy(Expression<Func<T, object>> selector);

    /// <summary>
    /// Sorts results by the selected property in descending order.
    /// </summary>
    IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector);

    /// <summary>
    /// Groups the filtered documents by <paramref name="keySelector"/> and switches the query into
    /// aggregate mode: the following <see cref="IGroupedDocumentQuery{T,TKey}.Select"/> /
    /// <see cref="IGroupedDocumentQuery{T,TKey}.Project"/> projects one row per group, using
    /// <c>g.Key</c> for the group value and the <see cref="Sql"/> group aggregates
    /// (<c>g.Count()</c>, <c>g.Sum</c>, <c>g.Min</c>, <c>g.Max</c>, <c>g.Avg</c>) over the group's
    /// members. The key may be a JSON property (<c>o =&gt; o.Status</c>), a derived scalar
    /// (<c>o =&gt; o.CreatedAt.Year</c>), or an anonymous type for a multi-column key
    /// (<c>o =&gt; new { o.Status, o.Region }</c>). Not supported on key-partitioned providers
    /// (Azure Table / DynamoDB) — those throw <see cref="NotSupportedException"/>.
    /// </summary>
    IGroupedDocumentQuery<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        => throw new NotSupportedException(
            "GroupBy is not supported by this provider. Read with a filter and aggregate client-side, " +
            "or use a relational or MongoDB store.");

    /// <summary>
    /// String-grammar grouping — groups by the named JSON field (e.g. <c>GroupBy("status")</c>) and
    /// exposes the string projection surface: <c>Project("status, count() as n, sum(total) as revenue")</c>
    /// and <c>Having("sum(total) &gt; 10000")</c>. The typed <see cref="GroupBy{TKey}"/> overload is the
    /// richer surface (multi-column and derived keys, typed <c>Select</c>). Not supported on key-partitioned
    /// providers (Azure Table / DynamoDB).
    /// </summary>
    IGroupedDocumentQuery<T, object> GroupBy(string keyField)
        => throw new NotSupportedException(
            "GroupBy is not supported by this provider. Read with a filter and aggregate client-side, " +
            "or use a relational or MongoDB store.");

    /// <summary>
    /// Limits results to the specified page.
    /// </summary>
    /// <param name="offset">Number of rows to skip.</param>
    /// <param name="take">Maximum number of rows to return.</param>
    IDocumentQuery<T> Paginate(int offset, int take);

    /// <summary>
    /// Projects each document into a new shape using a server-side SQL projection.
    /// </summary>
    /// <param name="selector">Expression defining the projection.</param>
    /// <param name="resultTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<T, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class;

    /// <summary>
    /// Projects each document into a <see cref="JsonObject"/> containing only the named fields,
    /// selected at runtime (e.g. a REST <c>?fields=</c> sparse fieldset). Fields are comma-separated
    /// and follow the same matching rules as the string <c>OrderBy</c>/<c>Where</c> overloads
    /// (case-insensitive CLR or JSON name, dotted paths for nested values). A field's output key is its
    /// leaf JSON property name unless overridden with <c>as alias</c>; two fields resolving to the same
    /// key throws. Scalar functions from the string <c>Where</c> grammar (<c>lower</c>, <c>length</c>,
    /// <c>substring</c>, <c>year</c>, <c>soundex</c>, …) may be projected and <b>require</b> an alias —
    /// e.g. <c>"name, lower(email) as email, year(created) as yr"</c>. Supported on every provider
    /// (relational providers project in SQL; the document/in-memory providers project client-side).
    /// </summary>
    /// <param name="fields">Comma-separated list of field paths or <c>func(field) as alias</c> projections.</param>
    /// <param name="jsonTypeInfo">
    /// Optional source-generated type metadata used to resolve the fields. When omitted, the query's
    /// <see cref="QueryTypeInfo"/> is used.
    /// </param>
    IDocumentQuery<JsonObject> Project(string fields, JsonTypeInfo<T>? jsonTypeInfo = null)
        => throw new NotSupportedException("String projection is not supported by this provider.");

    /// <summary>
    /// Builds the provider query this configuration would execute (the <see cref="ToList"/> form)
    /// without running it — useful for debugging, diagnostics, and logging. Relational providers and
    /// Cosmos return their SQL plus the bound parameter values; MongoDB returns its rendered BSON
    /// filter (or full find command) as JSON. Providers that evaluate queries in-memory (LiteDB,
    /// IndexedDB) and queries that project client-side (after <c>Select</c>/<c>Project</c> on the
    /// document providers) throw <see cref="NotSupportedException"/>.
    /// </summary>
    DocumentQueryString ToQueryString()
        => throw new NotSupportedException("This provider does not produce a query string.");

    /// <summary>
    /// Materializes all matching documents into a list.
    /// </summary>
    Task<IReadOnlyList<T>> ToList(CancellationToken ct = default);

    /// <summary>
    /// Streams matching documents one at a time without buffering.
    /// </summary>
    IAsyncEnumerable<T> ToAsyncEnumerable(CancellationToken ct = default);

    /// <summary>
    /// Returns the number of matching documents.
    /// </summary>
    Task<long> Count(CancellationToken ct = default);

    /// <summary>
    /// Returns true if at least one document matches the current filters.
    /// </summary>
    Task<bool> Any(CancellationToken ct = default);

    /// <summary>
    /// Deletes all documents matching the current filters and returns the number deleted.
    /// </summary>
    Task<int> ExecuteDelete(CancellationToken ct = default);

    /// <summary>
    /// Updates a single property on all documents matching the current filters
    /// and returns the number of rows updated.
    /// </summary>
    /// <param name="property">Expression selecting the property to update.</param>
    /// <param name="value">The new value (scalar: string, int, bool, etc., or null).</param>
    /// <param name="ct">Cancellation token.</param>
    Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default);

    /// <summary>
    /// Returns the maximum value of the selected property across matching documents.
    /// </summary>
    Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default);

    /// <summary>
    /// Returns the minimum value of the selected property across matching documents.
    /// </summary>
    Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default);

    /// <summary>
    /// Returns the sum of the selected property across matching documents.
    /// </summary>
    Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default);

    /// <summary>
    /// Returns the average of the selected numeric property across matching documents.
    /// </summary>
    Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default);

    /// <summary>
    /// Returns an async stream of in-process changes whose document matches the query's
    /// <see cref="Where"/> predicates. <see cref="OrderBy"/>, <see cref="Paginate"/>, and
    /// <see cref="GroupBy{TKey}"/> are ignored — they affect result shape, not membership.
    /// <para>
    /// Changes that do not materialize the document (<see cref="DocumentChangeType.Removed"/>,
    /// <see cref="DocumentChangeType.Cleared"/>, and the property-level update paths) are passed
    /// through unfiltered so the consumer can re-query if needed.
    /// </para>
    /// <para>Throws <see cref="NotSupportedException"/> if the underlying store does not support change observation.</para>
    /// </summary>
    /// <param name="ct">Cancels the subscription.</param>
    IAsyncEnumerable<DocumentChange<T>> NotifyOnChange(CancellationToken ct = default);

    /// <summary>
    /// Terminates the query with an ANN search against the vector mapped via
    /// <c>MapVectorProperty</c>. Current <see cref="Where"/> predicates act as a pre-filter
    /// where the provider supports it; <see cref="OrderBy"/>, <see cref="GroupBy{TKey}"/>, and
    /// <see cref="Paginate"/> are ignored — <paramref name="k"/> controls the result count.
    /// </summary>
    Task<IReadOnlyList<VectorResult<T>>> NearestVectors(
        ReadOnlyMemory<float> query,
        int k,
        CancellationToken ct = default)
        => throw new NotSupportedException("Vector queries are not supported by this provider.");

    /// <summary>
    /// Terminates the query with a relevance-ranked full-text search over the property/properties
    /// registered via <c>MapFullTextProperty</c>. Current <see cref="Where"/> predicates act as a
    /// pre-filter where the provider supports it; <see cref="OrderBy"/>, <see cref="GroupBy{TKey}"/>, and
    /// <see cref="Paginate"/> are ignored — <paramref name="maxResults"/> controls the result count and
    /// results come back ordered by relevance descending.
    /// </summary>
    Task<IReadOnlyList<FullTextResult<T>>> FullTextMatch(
        string searchText,
        int maxResults = 50,
        CancellationToken ct = default)
        => throw new NotSupportedException("Full-text queries are not supported by this provider.");
}
