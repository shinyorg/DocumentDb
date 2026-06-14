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
        JsonTypeInfo<T>? jsonTypeInfo = null
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyPath);

        return query.OrderBy(BuildSelector(propertyPath, ResolveTypeInfo(query, jsonTypeInfo)));
    }

    /// <summary>
    /// Sorts results by a property identified at runtime by name, in descending order.
    /// See <see cref="OrderBy{T}(IDocumentQuery{T}, string, JsonTypeInfo{T})"/> for matching rules.
    /// </summary>
    public static IDocumentQuery<T> OrderByDescending<T>(
        this IDocumentQuery<T> query,
        string propertyPath,
        JsonTypeInfo<T>? jsonTypeInfo = null
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyPath);

        return query.OrderByDescending(BuildSelector(propertyPath, ResolveTypeInfo(query, jsonTypeInfo)));
    }

    /// <summary>
    /// Sorts results by a property identified at runtime by name, with the sort direction
    /// supplied as a string. An empty, <c>null</c>, or whitespace <paramref name="direction"/>
    /// defaults to ascending. Accepted (case-insensitive) values are <c>"asc"</c>/<c>"ascending"</c>
    /// and <c>"desc"</c>/<c>"descending"</c>. See
    /// <see cref="OrderBy{T}(IDocumentQuery{T}, string, JsonTypeInfo{T})"/> for property matching rules.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="query">The query builder.</param>
    /// <param name="propertyPath">CLR or JSON property name; supports dotted paths for nested properties.</param>
    /// <param name="direction">Sort direction; <c>"asc"</c>/<c>"ascending"</c> or <c>"desc"</c>/<c>"descending"</c>. Empty defaults to ascending.</param>
    /// <param name="jsonTypeInfo">Source-generated type metadata used to resolve the property.</param>
    public static IDocumentQuery<T> OrderBy<T>(
        this IDocumentQuery<T> query,
        string propertyPath,
        string? direction,
        JsonTypeInfo<T>? jsonTypeInfo = null
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyPath);

        var typeInfo = ResolveTypeInfo(query, jsonTypeInfo);
        return IsDescending(direction)
            ? query.OrderByDescending(propertyPath, typeInfo)
            : query.OrderBy(propertyPath, typeInfo);
    }

    /// <summary>
    /// Returns the supplied <paramref name="provided"/> type info, or falls back to the query's
    /// <see cref="IDocumentQuery{T}.QueryTypeInfo"/> (resolved when the query was created). Throws if
    /// neither is available.
    /// </summary>
    static JsonTypeInfo<T> ResolveTypeInfo<T>(IDocumentQuery<T> query, JsonTypeInfo<T>? provided) where T : class
        => provided
           ?? query.QueryTypeInfo
           ?? throw new InvalidOperationException(
               $"No JsonTypeInfo<{typeof(T).Name}> was supplied and none could be resolved from the query. " +
               "Pass one explicitly or register a JsonSerializerContext on the store.");

    static bool IsDescending(string? direction)
    {
        if (String.IsNullOrWhiteSpace(direction))
            return false;

        switch (direction.Trim().ToLowerInvariant())
        {
            case "asc":
            case "ascending":
                return false;

            case "desc":
            case "descending":
                return true;

            default:
                throw new ArgumentException(
                    $"'{direction}' is not a valid sort direction. Use 'asc', 'ascending', 'desc', or 'descending'.",
                    nameof(direction));
        }
    }

    /// <summary>
    /// Filters documents using a human-friendly filter string evaluated at runtime, e.g.
    /// <c>"Age &gt;= 30 and Status == 'open'"</c>. The string is parsed into an expression and
    /// runs through the same <see cref="IDocumentQuery{T}.Where"/> pipeline as a compiled predicate.
    /// </summary>
    /// <remarks>
    /// <para>Supported syntax:</para>
    /// <list type="bullet">
    /// <item>Logical operators <c>and</c>, <c>or</c>, <c>not</c> and parentheses.</item>
    /// <item>Comparisons <c>==</c> (or <c>=</c>), <c>!=</c> (or <c>&lt;&gt;</c>), <c>&gt;</c>, <c>&gt;=</c>, <c>&lt;</c>, <c>&lt;=</c>.</item>
    /// <item><c>field is null</c> / <c>field is not null</c> and <c>field in (a, b, c)</c>.</item>
    /// <item>String functions <c>contains(field, 'x')</c>, <c>startsWith(field, 'x')</c>, <c>endsWith(field, 'x')</c>.</item>
    /// </list>
    /// <para>
    /// Field names follow the same matching rules as
    /// <see cref="OrderBy{T}(IDocumentQuery{T}, string, JsonTypeInfo{T})"/> (case-insensitive CLR or
    /// JSON name, dotted paths). String literals use single or double quotes; double the quote to escape.
    /// AOT-safe: resolution walks <see cref="JsonTypeInfo.Properties"/> and never compiles expressions.
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="query">The query builder.</param>
    /// <param name="filter">The filter expression string.</param>
    /// <param name="jsonTypeInfo">Source-generated type metadata used to resolve fields.</param>
    public static IDocumentQuery<T> Where<T>(
        this IDocumentQuery<T> query,
        string filter,
        JsonTypeInfo<T>? jsonTypeInfo = null
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentException.ThrowIfNullOrWhiteSpace(filter);

        return query.Where(Internal.FilterExpressionParser.Parse(filter, ResolveTypeInfo(query, jsonTypeInfo)));
    }

    static Expression<Func<T, object>> BuildSelector<T>(string propertyPath, JsonTypeInfo<T> jsonTypeInfo)
    {
        var parameter = Expression.Parameter(typeof(T), "x");
        var (body, _) = BuildMemberAccess(parameter, propertyPath, jsonTypeInfo);

        if (body.Type.IsValueType)
            body = Expression.Convert(body, typeof(object));

        return Expression.Lambda<Func<T, object>>(body, parameter);
    }

    /// <summary>
    /// Builds a member-access expression (<c>x.Prop</c> / <c>x.Nav.Prop</c>) for a dotted property path,
    /// resolving each segment against <see cref="JsonTypeInfo.Properties"/> (AOT-safe). Returns the access
    /// expression and the CLR type of the leaf property.
    /// </summary>
    internal static (Expression Body, Type LeafType) BuildMemberAccess<T>(
        ParameterExpression parameter, string propertyPath, JsonTypeInfo<T> jsonTypeInfo)
    {
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

        return (body, body.Type);
    }

    /// <summary>
    /// Resolves a dotted property path to its JSON path (segments joined by <c>.</c>, after the naming
    /// policy / <c>[JsonPropertyName]</c>) and the JSON name of the leaf segment. AOT-safe.
    /// </summary>
    internal static (string JsonPath, string LeafJsonName) ResolveJsonPath<T>(
        string propertyPath, JsonTypeInfo<T> jsonTypeInfo)
    {
        JsonTypeInfo currentTypeInfo = jsonTypeInfo;
        var segments = propertyPath.Split('.');
        var jsonNames = new string[segments.Length];

        for (var i = 0; i < segments.Length; i++)
        {
            var name = segments[i].Trim();
            if (name.Length == 0)
                throw new ArgumentException("Property path contains an empty segment.", nameof(propertyPath));

            var jsonProperty = ResolveJsonProperty(currentTypeInfo, name)
                ?? throw new ArgumentException(
                    $"Property '{name}' not found on type '{currentTypeInfo.Type.Name}'.",
                    nameof(propertyPath));

            jsonNames[i] = jsonProperty.Name;

            if (i < segments.Length - 1 && jsonProperty.AttributeProvider is PropertyInfo pi)
                currentTypeInfo = jsonTypeInfo.Options.GetTypeInfo(pi.PropertyType);
        }

        return (string.Join('.', jsonNames), jsonNames[^1]);
    }

    static JsonPropertyInfo? ResolveJsonProperty(JsonTypeInfo typeInfo, string name)
    {
        foreach (var prop in typeInfo.Properties)
        {
            if (prop.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                (prop.AttributeProvider is PropertyInfo pi && pi.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
            {
                return prop;
            }
        }
        return null;
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
