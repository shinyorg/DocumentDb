using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

/// <summary>
/// Convenience helpers for executing common patterns against <see cref="IDocumentQuery{T}"/>.
/// </summary>
public static class DocumentQueryExtensions
{
    /// <summary>
    /// Sorts results by a property identified at runtime by name. Supports dotted paths
    /// (e.g. <c>"Address.City"</c>) and is matched case-insensitively against either the
    /// CLR property name or the JSON property name from <paramref name="jsonTypeInfo"/>.
    /// AOT-safe: resolution walks <see cref="JsonTypeInfo.Properties"/> (source-generated)
    /// rather than reflecting on <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="query">The query builder.</param>
    /// <param name="propertyPath">CLR or JSON property name; supports dotted paths for nested properties.</param>
    /// <param name="jsonTypeInfo">Source-generated type metadata used to resolve the property.</param>
    public static IDocumentQuery<T> OrderBy<T>(
        this IDocumentQuery<T> query,
        string propertyPath,
        JsonTypeInfo<T> jsonTypeInfo
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(jsonTypeInfo);
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyPath);

        return query.OrderBy(BuildSelector(propertyPath, jsonTypeInfo));
    }

    /// <summary>
    /// Sorts results by a property identified at runtime by name, in descending order.
    /// See <see cref="OrderBy{T}(IDocumentQuery{T}, string, JsonTypeInfo{T})"/> for matching rules.
    /// </summary>
    public static IDocumentQuery<T> OrderByDescending<T>(
        this IDocumentQuery<T> query,
        string propertyPath,
        JsonTypeInfo<T> jsonTypeInfo
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(jsonTypeInfo);
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyPath);

        return query.OrderByDescending(BuildSelector(propertyPath, jsonTypeInfo));
    }

    static Expression<Func<T, object>> BuildSelector<T>(string propertyPath, JsonTypeInfo<T> jsonTypeInfo)
    {
        var parameter = Expression.Parameter(typeof(T), "x");
        Expression body = parameter;
        JsonTypeInfo currentTypeInfo = jsonTypeInfo;

        var segments = propertyPath.Split('.');
        for (var i = 0; i < segments.Length; i++)
        {
            var name = segments[i].Trim();
            if (name.Length == 0)
                throw new ArgumentException("Property path contains an empty segment.", nameof(propertyPath));

            var propertyInfo = ResolvePropertyInfo(currentTypeInfo, name)
                ?? throw new ArgumentException(
                    $"Property '{name}' not found on type '{currentTypeInfo.Type.Name}'.",
                    nameof(propertyPath));

            body = Expression.Property(body, propertyInfo);

            if (i < segments.Length - 1)
                currentTypeInfo = jsonTypeInfo.Options.GetTypeInfo(propertyInfo.PropertyType);
        }

        if (body.Type.IsValueType)
            body = Expression.Convert(body, typeof(object));

        return Expression.Lambda<Func<T, object>>(body, parameter);
    }

    static PropertyInfo? ResolvePropertyInfo(JsonTypeInfo typeInfo, string name)
    {
        foreach (var prop in typeInfo.Properties)
        {
            if (prop.AttributeProvider is PropertyInfo pi &&
                (pi.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                 prop.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
            {
                return pi;
            }
        }
        return null;
    }

    /// <summary>
    /// Executes the query for a specific page and returns the records along with the total count
    /// across all pages. Any previous <c>Paginate</c> call on the query is overridden.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="query">The query builder.</param>
    /// <param name="page">
    /// The page coordinate. By default this is 1-based (page 1 is the first page). Set
    /// <paramref name="zeroBased"/> to <c>true</c> to treat it as a 0-based index.
    /// </param>
    /// <param name="pageSize">The maximum number of records to return per page. Must be greater than zero.</param>
    /// <param name="zeroBased">
    /// When <c>false</c> (default), <paramref name="page"/> is 1-based. When <c>true</c>,
    /// <paramref name="page"/> is a 0-based index.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task<PagedResults<T>> PageResult<T>(
        this IDocumentQuery<T> query,
        int page,
        int pageSize,
        bool zeroBased = false,
        CancellationToken ct = default
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        var minPage = zeroBased ? 0 : 1;
        if (page < minPage)
            throw new ArgumentOutOfRangeException(nameof(page), page, $"Page must be >= {minPage}.");

        var offset = zeroBased ? page * pageSize : (page - 1) * pageSize;

        var totalCount = await query.Count(ct).ConfigureAwait(false);
        var records = await query.Paginate(offset, pageSize).ToList(ct).ConfigureAwait(false);

        return new PagedResults<T>(
            records,
            checked((int)totalCount),
            page,
            pageSize
        );
    }
}
