using System.Linq.Expressions;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The two adapters every provider needs when it builds a <see cref="DocumentQueryContext{T}"/> — mapping its
/// registered query filters and computed mappings into the provider-agnostic shapes the context exposes.
/// </summary>
public static class QueryContextFilters
{
    /// <summary>Projects registered query filters into the (name, predicate) pairs the query context takes.</summary>
    public static IReadOnlyList<(string? Name, Expression<Func<T, bool>> Predicate)> Resolve<T>(IReadOnlyList<QueryFilter> filters)
        where T : class
    {
        if (filters.Count == 0)
            return [];
        var list = new List<(string?, Expression<Func<T, bool>>)>(filters.Count);
        foreach (var filter in filters)
            list.Add((filter.Name, (Expression<Func<T, bool>>)filter.Predicate));
        return list;
    }

    /// <summary>Builds the delegate that populates a materialized document's computed properties, or null when
    /// the type maps none.</summary>
    public static Action<T>? ApplyComputed<T>(IReadOnlyList<ComputedMapping> computed) where T : class
    {
        if (computed.Count == 0)
            return null;
        return document =>
        {
            for (var i = 0; i < computed.Count; i++)
                computed[i].SetValue(document!, computed[i].Compute(document!));
        };
    }
}
