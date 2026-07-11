using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
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

        return query.OrderBy(BuildSelector(propertyPath, ResolveTypeInfo(query, jsonTypeInfo), ResolveComputed(query)));
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

        return query.OrderByDescending(BuildSelector(propertyPath, ResolveTypeInfo(query, jsonTypeInfo), ResolveComputed(query)));
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
    /// <item>Predicate functions <c>contains(field, 'x')</c>, <c>startsWith</c>, <c>endsWith</c>, <c>isnullorempty(field)</c>, <c>hasflag(field, 'Flag')</c>.</item>
    /// <item>Scalar functions usable on either side of a comparison: <c>lower</c>/<c>upper</c>, <c>length</c>, <c>trim</c>/<c>ltrim</c>/<c>rtrim</c>, <c>substring(f, start[, len])</c>, <c>replace(f, 'a', 'b')</c>, <c>indexof(f, 'x')</c>, <c>abs</c>/<c>ceiling</c>/<c>floor</c>/<c>round</c>/<c>sqrt</c>/<c>sign</c>, <c>year</c>/<c>month</c>/<c>day</c>/<c>hour</c>/<c>minute</c>/<c>second</c>, <c>soundex</c> — e.g. <c>lower(name) = 'alice'</c>, <c>year(created) = 2026</c>, <c>soundex(name) = soundex('Smith')</c>.</item>
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

        return query.Where(Internal.FilterExpressionParser.Parse(filter, ResolveTypeInfo(query, jsonTypeInfo), ResolveComputed(query)));
    }

    /// <summary>
    /// Filters documents using an interpolated filter string, e.g.
    /// <c>query.Where($"Status == {status} and Age &gt;= {minAge}")</c>. Each interpolated <c>{value}</c>
    /// is captured as a typed argument and bound as a parameter rather than formatted into the filter, so
    /// values never need quoting and the filter cannot be injection-tampered. The supported syntax is
    /// identical to <see cref="Where{T}(IDocumentQuery{T}, string, JsonTypeInfo{T})"/>; placeholders are
    /// only valid where a literal value would appear (comparison right-hand side, <c>in (...)</c> list, or a
    /// string-function argument), never as a field name.
    /// </summary>
    /// <remarks>
    /// An interpolated string literal binds to this overload in preference to the raw
    /// <see cref="Where{T}(IDocumentQuery{T}, string, JsonTypeInfo{T})"/> overload; pass a plain
    /// <see cref="string"/> variable to use the raw form deliberately.
    /// </remarks>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="query">The query builder.</param>
    /// <param name="filter">The interpolated filter expression.</param>
    /// <param name="jsonTypeInfo">Source-generated type metadata used to resolve fields.</param>
    public static IDocumentQuery<T> Where<T>(
        this IDocumentQuery<T> query,
        FilterInterpolatedStringHandler filter,
        JsonTypeInfo<T>? jsonTypeInfo = null
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);

        var info = ResolveTypeInfo(query, jsonTypeInfo);
        return query.Where(Internal.FilterExpressionParser.Parse(filter.Filter, filter.Arguments, info, ResolveComputed(query)));
    }

    /// <summary>
    /// Filters to documents whose <paramref name="selector"/> value is one of <paramref name="values"/>
    /// (an <c>IN</c> set membership test). The collection is passed in-memory and lowered to the store's
    /// native construct (<c>IN</c> / <c>$in</c>), not expanded into the filter text.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <typeparam name="TValue">The selected property type.</typeparam>
    /// <param name="query">The query builder.</param>
    /// <param name="selector">Selects the property to test, e.g. <c>x =&gt; x.Status</c>.</param>
    /// <param name="values">The candidate values. An empty set matches nothing.</param>
    /// <param name="nulls">How <c>null</c> values in <paramref name="values"/> are treated. Defaults to <see cref="NullHandling.Ignore"/>.</param>
    public static IDocumentQuery<T> WhereIn<T, TValue>(
        this IDocumentQuery<T> query,
        Expression<Func<T, TValue>> selector,
        IEnumerable<TValue> values,
        NullHandling nulls = NullHandling.Ignore
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(values);

        var body = Internal.InExpressionBuilder.Build(selector.Body, values, nulls);
        return query.Where(Expression.Lambda<Func<T, bool>>(body, selector.Parameters));
    }

    /// <summary>
    /// Filters to documents whose <paramref name="selector"/> value is <em>not</em> one of
    /// <paramref name="values"/> (a <c>NOT IN</c> test). See
    /// <see cref="WhereIn{T, TValue}(IDocumentQuery{T}, Expression{Func{T, TValue}}, IEnumerable{TValue}, NullHandling)"/>.
    /// An empty set matches everything. Under <see cref="NullHandling.Match"/>, a <c>null</c> in
    /// <paramref name="values"/> excludes <c>null</c> fields (<c>… AND field IS NOT NULL</c>).
    /// </summary>
    public static IDocumentQuery<T> WhereNotIn<T, TValue>(
        this IDocumentQuery<T> query,
        Expression<Func<T, TValue>> selector,
        IEnumerable<TValue> values,
        NullHandling nulls = NullHandling.Ignore
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(values);

        var body = Internal.InExpressionBuilder.Build(selector.Body, values, nulls);
        return query.Where(Expression.Lambda<Func<T, bool>>(Expression.Not(body), selector.Parameters));
    }

    /// <summary>
    /// Filters to documents whose property (identified at runtime by name, with the same matching rules
    /// as <see cref="OrderBy{T}(IDocumentQuery{T}, string, JsonTypeInfo{T})"/>) is one of
    /// <paramref name="values"/>. See the strongly-typed
    /// <see cref="WhereIn{T, TValue}(IDocumentQuery{T}, Expression{Func{T, TValue}}, IEnumerable{TValue}, NullHandling)"/>.
    /// </summary>
    public static IDocumentQuery<T> WhereIn<T>(
        this IDocumentQuery<T> query,
        string propertyPath,
        IEnumerable values,
        NullHandling nulls = NullHandling.Ignore,
        JsonTypeInfo<T>? jsonTypeInfo = null
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyPath);
        ArgumentNullException.ThrowIfNull(values);

        var parameter = Expression.Parameter(typeof(T), "x");
        var (member, _) = BuildMemberAccess(parameter, propertyPath, ResolveTypeInfo(query, jsonTypeInfo), ResolveComputed(query));
        var body = Internal.InExpressionBuilder.Build(member, values, nulls);
        return query.Where(Expression.Lambda<Func<T, bool>>(body, parameter));
    }

    /// <summary>
    /// Filters to documents whose property (identified at runtime by name) is <em>not</em> one of
    /// <paramref name="values"/>. See
    /// <see cref="WhereIn{T}(IDocumentQuery{T}, string, IEnumerable, NullHandling, JsonTypeInfo{T})"/>.
    /// </summary>
    public static IDocumentQuery<T> WhereNotIn<T>(
        this IDocumentQuery<T> query,
        string propertyPath,
        IEnumerable values,
        NullHandling nulls = NullHandling.Ignore,
        JsonTypeInfo<T>? jsonTypeInfo = null
    ) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyPath);
        ArgumentNullException.ThrowIfNull(values);

        var parameter = Expression.Parameter(typeof(T), "x");
        var (member, _) = BuildMemberAccess(parameter, propertyPath, ResolveTypeInfo(query, jsonTypeInfo), ResolveComputed(query));
        var body = Internal.InExpressionBuilder.Build(member, values, nulls);
        return query.Where(Expression.Lambda<Func<T, bool>>(Expression.Not(body), parameter));
    }

    static Expression<Func<T, object>> BuildSelector<T>(string propertyPath, JsonTypeInfo<T> jsonTypeInfo,
        IReadOnlyDictionary<string, Internal.ComputedMapping>? computed = null) where T : class
    {
        // A value-function order key (e.g. "distance(area, '<geojson>')" or "lower(name)") runs through the
        // same grammar as Where/projections; a plain dotted path resolves directly.
        if (propertyPath.Contains('('))
            return Internal.FilterExpressionParser.ParseValueSelector(propertyPath, jsonTypeInfo, computed);

        var parameter = Expression.Parameter(typeof(T), "x");
        var (body, _) = BuildMemberAccess(parameter, propertyPath, jsonTypeInfo, computed);

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
        ParameterExpression parameter, string propertyPath, JsonTypeInfo<T> jsonTypeInfo,
        IReadOnlyDictionary<string, Internal.ComputedMapping>? computed = null)
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
                ?? ResolveComputedPropertyInfo(currentTypeInfo, name, computed)
                ?? throw new ArgumentException(
                    $"Property '{name}' not found on type '{currentTypeInfo.Type.Name}'.",
                    nameof(propertyPath));

            body = Expression.Property(body, propertyInfo);

            if (i < segments.Length - 1)
                currentTypeInfo = jsonTypeInfo.Options.GetTypeInfo(propertyInfo.PropertyType);
        }

        return (body, body.Type);
    }

    /// <summary>The computed-property lookup for the query's type, if it exposes one (used by the string helpers).</summary>
    static IReadOnlyDictionary<string, Internal.ComputedMapping>? ResolveComputed<T>(IDocumentQuery<T> query) where T : class
        => (query as Internal.IComputedAwareQuery)?.ComputedLookup;

    /// <summary>
    /// Resolves a computed property (which is <c>[JsonIgnore]</c>'d and so absent from
    /// <see cref="JsonTypeInfo.Properties"/>) to its CLR <see cref="PropertyInfo"/> by name.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "Computed property resolved by name on a user-constructed type that is not subject to trimming.")]
    static PropertyInfo? ResolveComputedPropertyInfo(JsonTypeInfo typeInfo, string name, IReadOnlyDictionary<string, Internal.ComputedMapping>? computed)
        => computed != null && computed.TryGetValue(name, out var mapping)
            ? typeInfo.Type.GetProperty(mapping.PropertyName)
            : null;

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

    /// <summary>
    /// Resolves a dotted property path to its JSON path and the CLR type of the leaf property, so callers
    /// that only have a string field name (e.g. the group-by string grammar) can request a typed extraction.
    /// </summary>
    internal static (string JsonPath, Type LeafType) ResolveJsonPathWithType<T>(
        string propertyPath, JsonTypeInfo<T> jsonTypeInfo)
    {
        JsonTypeInfo currentTypeInfo = jsonTypeInfo;
        var segments = propertyPath.Split('.');
        var jsonNames = new string[segments.Length];
        Type leafType = typeof(object);

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
            leafType = jsonProperty.PropertyType;

            if (i < segments.Length - 1 && jsonProperty.AttributeProvider is PropertyInfo pi)
                currentTypeInfo = jsonTypeInfo.Options.GetTypeInfo(pi.PropertyType);
        }

        return (string.Join('.', jsonNames), leafType);
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
    /// Enumerates every matching document by walking cursor pages of <paramref name="pageSize"/> until
    /// exhausted. Unlike a deep <see cref="IDocumentQuery{T}.Paginate"/> loop this stays O(log n) per page
    /// (with an index over the sort key) and is stable under concurrent writes. Requires the provider
    /// supports <see cref="IDocumentQuery{T}.ToCursorPage"/>.
    /// </summary>
    /// <param name="query">The query builder (its <c>OrderBy</c> defines the keyset).</param>
    /// <param name="pageSize">Documents fetched per underlying cursor page. Must be greater than zero.</param>
    /// <param name="ct">Cancellation token.</param>
    public static async IAsyncEnumerable<T> ToCursorStream<T>(
        this IDocumentQuery<T> query,
        int pageSize = 100,
        [EnumeratorCancellation] CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        string? cursor = null;
        do
        {
            var page = await query.ToCursorPage(cursor, pageSize, ct).ConfigureAwait(false);
            foreach (var item in page.Items)
                yield return item;
            cursor = page.NextCursor;
        }
        while (cursor != null && !ct.IsCancellationRequested);
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
