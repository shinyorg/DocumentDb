namespace Shiny.DocumentDb;

/// <summary>
/// Marker class for SQL aggregate functions used in expression trees.
/// These methods are never executed — they exist only for expression-tree translation
/// in aggregate projections.
/// </summary>
public static class Sql
{
    /// <summary>Translates to SQL COUNT(*).</summary>
    public static int Count() => throw new InvalidOperationException("Sql.Count() is only supported inside aggregate expression trees.");

    /// <summary>Translates to SQL MAX(expression).</summary>
    public static TValue Max<TValue>(TValue value) => throw new InvalidOperationException("Sql.Max() is only supported inside aggregate expression trees.");

    /// <summary>Translates to SQL MIN(expression).</summary>
    public static TValue Min<TValue>(TValue value) => throw new InvalidOperationException("Sql.Min() is only supported inside aggregate expression trees.");

    /// <summary>Translates to SQL SUM(expression).</summary>
    public static TValue Sum<TValue>(TValue value) => throw new InvalidOperationException("Sql.Sum() is only supported inside aggregate expression trees.");

    /// <summary>Translates to SQL AVG(expression).</summary>
    public static double Avg(object value) => throw new InvalidOperationException("Sql.Avg() is only supported inside aggregate expression trees.");

    // ── Grouped aggregates (IGroupedDocumentQuery.Select) ───────────────────────────────
    // Read off the group handed to a grouped projection: g.Count(), g.Sum(x => x.Total), etc.
    // Never executed — pure expression-tree markers translated to SQL aggregate terms per group.

    // On the relational path these are pure markers translated to SQL. On the in-memory path (LiteDB /
    // IndexedDB) the group is a materialized set, so they compute the real value directly.

    /// <summary>Translates to SQL COUNT(*) over the group's members.</summary>
    public static int Count<TKey, TSource>(this IDocumentGroup<TKey, TSource> group)
        => group is Internal.IMaterializedGroup<TSource> m
            ? m.Members.Count
            : throw MarkerOnly("Count");

    /// <summary>Translates to SQL SUM(selector) over the group's members.</summary>
    public static TValue Sum<TKey, TSource, TValue>(this IDocumentGroup<TKey, TSource> group, Func<TSource, TValue> selector)
        => group is Internal.IMaterializedGroup<TSource> m
            ? Internal.GroupAggregateMath.Sum(m.Members.Select(selector))
            : throw MarkerOnly("Sum");

    /// <summary>Translates to SQL MIN(selector) over the group's members.</summary>
    public static TValue Min<TKey, TSource, TValue>(this IDocumentGroup<TKey, TSource> group, Func<TSource, TValue> selector)
        => group is Internal.IMaterializedGroup<TSource> m
            ? Internal.GroupAggregateMath.Min(m.Members.Select(selector))
            : throw MarkerOnly("Min");

    /// <summary>Translates to SQL MAX(selector) over the group's members.</summary>
    public static TValue Max<TKey, TSource, TValue>(this IDocumentGroup<TKey, TSource> group, Func<TSource, TValue> selector)
        => group is Internal.IMaterializedGroup<TSource> m
            ? Internal.GroupAggregateMath.Max(m.Members.Select(selector))
            : throw MarkerOnly("Max");

    /// <summary>Translates to SQL AVG(selector) over the group's members.</summary>
    public static double Avg<TKey, TSource>(this IDocumentGroup<TKey, TSource> group, Func<TSource, object?> selector)
        => group is Internal.IMaterializedGroup<TSource> m
            ? Internal.GroupAggregateMath.Avg(m.Members, selector)
            : throw MarkerOnly("Avg");

    static InvalidOperationException MarkerOnly(string name)
        => new($"g.{name}() is only supported inside a grouped projection.");
}
