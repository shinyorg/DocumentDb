using System.Linq.Expressions;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// A grouped query awaiting an aggregate projection. Produced by
/// <see cref="IDocumentQuery{T}.GroupBy{TKey}"/>. Project one row per group with
/// <see cref="Select{TResult}"/> (or <see cref="Project"/>), using <c>g.Key</c> for the group value
/// and the <see cref="Sql"/> group aggregates (<c>g.Count()</c>, <c>g.Sum(x =&gt; x.Total)</c>, …).
/// </summary>
public interface IGroupedDocumentQuery<T, TKey> where T : class
{
    /// <summary>
    /// Filters groups by an aggregate predicate (SQL <c>HAVING</c>). The predicate compares group
    /// aggregates (<c>g.Sum(x =&gt; x.Total) &gt; 10_000</c>) and/or the group key (<c>g.Key</c>) to
    /// constants. May be called more than once — the predicates are combined with AND.
    /// </summary>
    IGroupedDocumentQuery<T, TKey> Having(Expression<Func<IDocumentGroup<TKey, T>, bool>> predicate);

    /// <summary>
    /// String-grammar <c>HAVING</c> — filters groups by an aggregate predicate written as text, e.g.
    /// <c>"sum(total) &gt; 10000"</c> or <c>"count() &gt;= 5 and status = 'Shipped'"</c>. Available on the
    /// string-grouping surface produced by <see cref="IDocumentQuery{T}.GroupBy(string)"/>.
    /// </summary>
    IGroupedDocumentQuery<T, TKey> Having(string predicate)
        => throw new NotSupportedException("String HAVING is only available on a string GroupBy(\"field\") query.");

    /// <summary>
    /// Projects one row per group. Use <c>g.Key</c> for the group value and the <see cref="Sql"/>
    /// group aggregates (<c>g.Count()</c>, <c>g.Sum(x =&gt; x.Total)</c>, <c>g.Avg</c>, <c>g.Min</c>,
    /// <c>g.Max</c>) in the selector. The returned query is an ordinary <see cref="IDocumentQuery{TResult}"/>
    /// — <c>OrderBy</c> (over an output column), <c>Paginate</c>, <c>ToList</c>, and
    /// <c>ToAsyncEnumerable</c> all flow through to sort/page/materialize the group rows.
    /// </summary>
    IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<IDocumentGroup<TKey, T>, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class;

    /// <summary>
    /// String-grammar projection of the group into <see cref="JsonObject"/> rows. Bare identifiers
    /// resolve to the group key column(s); the aggregate functions <c>count()</c>, <c>sum(x)</c>,
    /// <c>avg(x)</c>, <c>min(x)</c>, and <c>max(x)</c> are available and require an alias — e.g.
    /// <c>"status, count() as orders, sum(total) as revenue"</c>.
    /// </summary>
    IDocumentQuery<JsonObject> Project(string fields, JsonTypeInfo<T>? jsonTypeInfo = null)
        => throw new NotSupportedException("String projection is not supported by this provider.");
}

/// <summary>
/// The group handed to an aggregate projection: its key plus the <see cref="Sql"/>-aggregable members.
/// The aggregate methods (<see cref="Sql.Count"/>, <see cref="Sql.Sum"/>, …) are pure expression-tree
/// markers — they are never executed; the query engine translates them to SQL aggregate terms.
/// </summary>
public interface IDocumentGroup<out TKey, T>
{
    /// <summary>The group key (the value the query grouped by).</summary>
    TKey Key { get; }
}
