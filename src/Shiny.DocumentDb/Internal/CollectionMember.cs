using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace Shiny.DocumentDb.Internal;

enum CollectionSizeKind
{
    /// <summary>Not a collection-size member access.</summary>
    None,

    /// <summary>A collection <c>.Count</c> or array <c>.Length</c> that maps to a JSON array length.</summary>
    ArrayLength,

    /// <summary>A size-like member (<c>string.Length</c>, dictionary <c>.Count</c>) with no JSON array-length meaning.</summary>
    Unsupported
}

/// <summary>
/// Classifies member access expressions that read a collection's size, so providers can translate
/// <c>x.Items.Count</c> / <c>x.Items.Length</c> to a native array-length function instead of silently
/// extracting a non-existent JSON path.
/// </summary>
static class CollectionMember
{
    /// <summary>
    /// Determines whether <paramref name="node"/> is a collection-size accessor. When it maps to an array
    /// length, <paramref name="collection"/> is set to the underlying collection expression.
    /// </summary>
    public static CollectionSizeKind Classify(MemberExpression node, out Expression collection)
    {
        collection = node.Expression!;
        if (node.Expression == null)
            return CollectionSizeKind.None;

        var parentType = node.Expression.Type;
        return node.Member.Name switch
        {
            "Length" when parentType.IsArray => CollectionSizeKind.ArrayLength,
            "Length" when parentType == typeof(string) => CollectionSizeKind.Unsupported,
            "Count" when IsDictionary(parentType) => CollectionSizeKind.Unsupported,
            "Count" when IsArrayCollection(parentType) => CollectionSizeKind.ArrayLength,
            _ => CollectionSizeKind.None
        };
    }

    [UnconditionalSuppressMessage("Trimming", "IL2070",
        Justification = "Collection types come from expression trees referencing known model types; interfaces are always preserved.")]
    static bool IsArrayCollection(Type type)
    {
        // A string is IEnumerable<char> but never a JSON array.
        if (type == typeof(string))
            return false;

        if (typeof(ICollection).IsAssignableFrom(type))
            return true;

        foreach (var iface in type.GetInterfaces())
        {
            if (iface.IsGenericType)
            {
                var def = iface.GetGenericTypeDefinition();
                if (def == typeof(ICollection<>) || def == typeof(IReadOnlyCollection<>))
                    return true;
            }
        }

        return false;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2070",
        Justification = "Dictionary types come from expression trees referencing known model types; interfaces are always preserved.")]
    static bool IsDictionary(Type type)
    {
        // Dictionaries serialize as JSON objects, so their Count is not an array length.
        if (typeof(IDictionary).IsAssignableFrom(type))
            return true;

        foreach (var iface in type.GetInterfaces())
        {
            if (iface.IsGenericType)
            {
                var def = iface.GetGenericTypeDefinition();
                if (def == typeof(IDictionary<,>) || def == typeof(IReadOnlyDictionary<,>))
                    return true;
            }
        }

        return false;
    }
}
