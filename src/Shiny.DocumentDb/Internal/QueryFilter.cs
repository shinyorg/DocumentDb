using System.Linq.Expressions;

namespace Shiny.DocumentDb.Internal;

public sealed record QueryFilter(string? Name, LambdaExpression Predicate)
{
    /// <summary>
    /// True when <c>IgnoreQueryFilters</c> lifts the filter named <paramref name="name"/>. The discriminator filter of
    /// a mapped derived type is never lifted: it is what makes <c>Query&lt;Derived&gt;()</c> a query for that type,
    /// not a rule about which documents to show.
    /// </summary>
    internal static bool IsLifted(string? name, bool ignoreAll, HashSet<string>? ignoredNames)
        => name != DocumentHierarchy.FilterName
           && (ignoreAll || (name != null && ignoredNames?.Contains(name) == true));
}
