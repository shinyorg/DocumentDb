using System.Linq.Expressions;
using System.Text.Json.Nodes;

namespace Shiny.DocumentDb;

/// <summary>
/// A query across two document types, started with <c>Query&lt;TLeft&gt;().Join&lt;TRight&gt;(…)</c>. Filters,
/// ordering and paging run in the engine; finish with <see cref="Select{TResult}"/> or <see cref="Project"/>.
/// Every builder call returns a new query.
/// </summary>
/// <remarks>
/// The string overloads qualify every field with one of the join's two aliases — the names given to the string
/// <c>Join</c>, or the parameter names of the LINQ join condition — e.g. <c>"o.total &gt; 100 and c.region = 'eu'"</c>.
/// A bare or unknown qualifier is an error rather than a nested path.
/// </remarks>
public interface IJoinQuery<TLeft, TRight> where TLeft : class where TRight : class
{
    /// <summary>Filters the joined pairs. Multiple calls are combined with AND.</summary>
    IJoinQuery<TLeft, TRight> Where(Expression<Func<TLeft, TRight, bool>> predicate);

    /// <summary>Filters the joined pairs with the string grammar, every field qualified with a join alias.</summary>
    IJoinQuery<TLeft, TRight> Where(string filter);

    /// <summary>
    /// Filters the joined pairs with an interpolated filter string; each <c>{value}</c> is bound as a parameter.
    /// </summary>
    IJoinQuery<TLeft, TRight> Where(FilterInterpolatedStringHandler filter);

    /// <summary>Sorts the joined pairs ascending. Multiple calls add secondary keys.</summary>
    IJoinQuery<TLeft, TRight> OrderBy(Expression<Func<TLeft, TRight, object>> selector);

    /// <summary>Sorts the joined pairs ascending by an alias-qualified field or value function.</summary>
    IJoinQuery<TLeft, TRight> OrderBy(string field);

    /// <summary>Sorts the joined pairs descending. Multiple calls add secondary keys.</summary>
    IJoinQuery<TLeft, TRight> OrderByDescending(Expression<Func<TLeft, TRight, object>> selector);

    /// <summary>Sorts the joined pairs descending by an alias-qualified field or value function.</summary>
    IJoinQuery<TLeft, TRight> OrderByDescending(string field);

    /// <summary>Skips <paramref name="offset"/> pairs and returns at most <paramref name="take"/>.</summary>
    IJoinQuery<TLeft, TRight> Paginate(int offset, int take);

    /// <summary>Disables every global query filter on both sides.</summary>
    IJoinQuery<TLeft, TRight> IgnoreQueryFilters();

    /// <summary>Disables the named global query filters on both sides.</summary>
    IJoinQuery<TLeft, TRight> IgnoreQueryFilters(params string[] filterNames);

    /// <summary>
    /// Shapes each joined pair into <typeparamref name="TResult"/>. The selector runs over the materialized
    /// documents, so any shape works — a named type, an anonymous type or a scalar — and encrypted properties read
    /// as plaintext. On a <see cref="JoinKind.Left"/> join the right document is <c>null</c> when nothing
    /// matched, so guard it: <c>(o, c) =&gt; new { o.Id, Customer = c == null ? null : c.Name }</c>.
    /// </summary>
    IJoinResult<TResult> Select<TResult>(Expression<Func<TLeft, TRight, TResult>> selector);

    /// <summary>
    /// Shapes each joined pair into a <see cref="JsonObject"/> holding the named alias-qualified fields, e.g.
    /// <c>"o.id as orderId, c.name as customer, o.total"</c>. An unaliased field's key is its leaf JSON name, and two
    /// fields resolving to the same key throw — alias one of them. Value functions (<c>lower</c>, <c>year</c>, …)
    /// require an alias.
    /// </summary>
    IJoinResult<JsonObject> Project(string fields);
}

/// <summary>The terminal operations of a projected join.</summary>
public interface IJoinResult<TResult>
{
    /// <summary>Runs the join and returns every projected row.</summary>
    Task<IReadOnlyList<TResult>> ToList(CancellationToken ct = default);

    /// <summary>Runs the join and streams the projected rows.</summary>
    IAsyncEnumerable<TResult> ToAsyncEnumerable(CancellationToken ct = default);

    /// <summary>Counts the joined pairs. Paging does not apply.</summary>
    Task<long> Count(CancellationToken ct = default);

    /// <summary>True when at least one pair matches. Paging does not apply.</summary>
    Task<bool> Any(CancellationToken ct = default);

    /// <summary>The first projected row, or the default value when nothing matched.</summary>
    Task<TResult?> FirstOrDefault(CancellationToken ct = default);

    /// <summary>The first projected row; throws when nothing matched.</summary>
    Task<TResult> First(CancellationToken ct = default);

    /// <summary>
    /// The engine query the join would run, without running it: SQL plus bound parameters on the relational
    /// providers, the aggregation pipeline as JSON on MongoDB.
    /// </summary>
    DocumentQueryString ToQueryString();
}
