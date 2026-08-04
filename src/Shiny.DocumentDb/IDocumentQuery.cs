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
    /// Reads one forward page using seek/keyset pagination derived from the current <see cref="OrderBy"/>
    /// (an <c>Id</c> tiebreaker is appended automatically to guarantee a total order). Pass <c>null</c> for
    /// the first page; pass the previous page's <see cref="CursorPage{T}.NextCursor"/> for each subsequent
    /// page. A null <c>NextCursor</c> on the result marks the last page.
    /// <para>
    /// Unlike a deep <see cref="Paginate"/> loop this stays O(log n) per page (with an index over the sort
    /// key) and is stable under concurrent writes. There is no total count — use
    /// <see cref="Paginate"/>/<c>PageResult</c> when a page number or total is required.
    /// </para>
    /// <para>
    /// The query's filters and ordering MUST be identical to the call that produced <paramref name="cursor"/>;
    /// a cursor is only valid for the query shape that created it (enforced by a shape hash). Not valid after
    /// <see cref="Select"/> / <see cref="Project"/> / <see cref="GroupBy{TKey}"/>.
    /// </para>
    /// </summary>
    /// <param name="cursor">Opaque continuation token from a prior page, or null for the first page.</param>
    /// <param name="take">Maximum items to return in this page. Must be &gt; 0.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<CursorPage<T>> ToCursorPage(string? cursor, int take, CancellationToken ct = default)
        => throw new NotSupportedException("Cursor pagination is not supported by this provider.");

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
    /// The first matching document, or <c>null</c> when nothing matches. Honors <see cref="OrderBy"/> and the
    /// current <see cref="Paginate"/> window — <c>Paginate(20, 10).FirstOrDefault()</c> is the 21st document,
    /// not the 1st.
    /// <para>
    /// Providers that page server-side fetch a single row instead of the whole match set. The interface default
    /// materializes the list, so only a query shape without push-down pays for the extra rows.
    /// </para>
    /// </summary>
    async Task<T?> FirstOrDefault(CancellationToken ct = default)
        => (await this.ToList(ct).ConfigureAwait(false)).FirstOrDefault();

    /// <summary>
    /// The first matching document. Throws <see cref="InvalidOperationException"/> when nothing matches — use
    /// <see cref="FirstOrDefault"/> when an empty result is expected. See <see cref="FirstOrDefault"/> for the
    /// <see cref="Paginate"/> interaction.
    /// </summary>
    async Task<T> First(CancellationToken ct = default)
        => await this.FirstOrDefault(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"No '{typeof(T).Name}' matched the query.");

    /// <summary>
    /// The only matching document, or <c>null</c> when nothing matches. Throws
    /// <see cref="InvalidOperationException"/> when more than one matches — providers fetch two rows to detect
    /// it, so the check costs one round trip rather than a separate count.
    /// </summary>
    async Task<T?> SingleOrDefault(CancellationToken ct = default)
    {
        var rows = await this.ToList(ct).ConfigureAwait(false);
        if (rows.Count > 1)
            throw new InvalidOperationException($"More than one '{typeof(T).Name}' matched the query.");
        return rows.Count == 0 ? null : rows[0];
    }

    /// <summary>
    /// The only matching document. Throws <see cref="InvalidOperationException"/> when none or more than one
    /// matches.
    /// </summary>
    async Task<T> Single(CancellationToken ct = default)
        => await this.SingleOrDefault(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"No '{typeof(T).Name}' matched the query.");

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
    /// Updates several properties on all documents matching the current filters and returns the number of rows
    /// updated: <c>ExecuteUpdate(b =&gt; b.Set(x =&gt; x.Status, "closed").Set(x =&gt; x.ClosedAt, now))</c>.
    /// <para>
    /// Providers whose engine can set several JSON paths at once do it in <b>one</b> statement, so the predicate
    /// is evaluated once and the write is atomic on its own. The interface default applies the assignments one
    /// at a time, which is correct but neither — put it in a session/transaction when that matters.
    /// </para>
    /// </summary>
    /// <param name="build">Declares the assignments. Must set at least one property.</param>
    /// <param name="ct">Cancellation token.</param>
    async Task<int> ExecuteUpdate(Action<IDocumentUpdateBuilder<T>> build, CancellationToken ct = default)
    {
        var assignments = Internal.DocumentUpdateBuilder<T>.Collect(build);

        var affected = 0;
        for (var i = 0; i < assignments.Count; i++)
        {
            // Every assignment runs over the same predicate, so the first pass reports the row count and the
            // rest necessarily match the same rows.
            var updated = await this.ExecuteUpdate(assignments[i].Property, assignments[i].Value, ct).ConfigureAwait(false);
            if (i == 0)
                affected = updated;
        }
        return affected;
    }

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
