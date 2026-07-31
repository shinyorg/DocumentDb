using System.Text.Json.Nodes;

namespace Shiny.DocumentDb;

/// <summary>
/// A string-grammar query over an <see cref="IJsonDocumentCollection"/>, returning raw JSON. Immutable —
/// every builder call returns a copy, matching <see cref="IDocumentQuery{T}"/>.
/// </summary>
/// <remarks>
/// <para>
/// Deliberately <b>not</b> <c>IDocumentQuery&lt;JsonObject&gt;</c>: that interface carries five typed-LINQ
/// members which are meaningless without a CLR type and would all have to throw, and it already has an
/// implementation (the projection query) that throws from the <em>opposite</em> members — two shapes with
/// inverted semantics behind one interface would make a type test on it meaningless.
/// </para>
/// <para>
/// Field paths are dot-separated. On a schema-free collection a path may carry a <c>:type</c> suffix
/// (<c>string</c>, <c>number</c>, <c>int</c>, <c>long</c>, <c>double</c>, <c>decimal</c>, <c>bool</c>,
/// <c>date</c>, <c>guid</c>) where nothing else pins the type — required for an <c>OrderBy</c>,
/// <c>min</c>/<c>max</c> or <c>Project</c> over a numeric field on any provider whose plain JSON extract
/// returns text. On a type-keyed collection paths resolve through the type's metadata and a <c>:type</c>
/// suffix is an error.
/// </para>
/// </remarks>
public interface IJsonDocumentQuery
{
    /// <summary>
    /// Filters with the string grammar, e.g. <c>"customer.name == 'bob' and total:number &gt; 100"</c>.
    /// Multiple calls AND together.
    /// </summary>
    IJsonDocumentQuery Where(string filter);

    /// <summary>
    /// Interpolated form — each <c>{value}</c> is captured as a typed argument and bound as a parameter
    /// rather than formatted into the filter text.
    /// </summary>
    IJsonDocumentQuery Where(FilterInterpolatedStringHandler filter);

    /// <summary>
    /// Comma-separated sort keys, each optionally suffixed with a direction, e.g.
    /// <c>"total:number desc, name"</c>.
    /// </summary>
    IJsonDocumentQuery OrderBy(string ordering);

    /// <summary>Skips <paramref name="offset"/> documents and takes <paramref name="take"/>.</summary>
    IJsonDocumentQuery Paginate(int offset, int take);

    /// <summary>Groups by a field path; project and filter the groups with the grouped string grammar.</summary>
    IGroupedDocumentQuery<JsonObject, object> GroupBy(string keyField);

    /// <summary>
    /// Projects at the database level, e.g. <c>"name, lower(email) as email, total:number"</c>. Function
    /// projections require an alias.
    /// </summary>
    IDocumentQuery<JsonObject> Project(string fields);

    /// <summary>The SQL this query would run, with its parameter values, without executing it.</summary>
    DocumentQueryString ToQueryString();

    Task<IReadOnlyList<JsonObject>> ToList(CancellationToken cancellationToken = default);
    IAsyncEnumerable<JsonObject> ToAsyncEnumerable(CancellationToken cancellationToken = default);
    Task<long> Count(CancellationToken cancellationToken = default);
    Task<bool> Any(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sums <paramref name="jsonPath"/> across every matching document, or <c>null</c> when nothing matched.
    /// </summary>
    /// <remarks>
    /// The path-based twin of <see cref="IDocumentQuery{T}.Sum{TValue}"/>, which needs a member selector.
    /// Extraction is always numeric — unlike <c>OrderBy</c>/<c>min</c>/<c>max</c> in the string grammar, a
    /// <c>:type</c> hint is neither needed nor meaningful here, though one is tolerated and ignored.
    /// </remarks>
    Task<double?> Sum(string jsonPath, CancellationToken cancellationToken = default);

    /// <summary>Averages <paramref name="jsonPath"/> numerically, or <c>null</c> when nothing matched.</summary>
    Task<double?> Average(string jsonPath, CancellationToken cancellationToken = default);

    /// <summary>
    /// The numeric minimum of <paramref name="jsonPath"/>, or <c>null</c> when nothing matched. Numeric on
    /// every provider — unlike the grammar's <c>min(...)</c>, which compares an unhinted path as text.
    /// </summary>
    Task<double?> Min(string jsonPath, CancellationToken cancellationToken = default);

    /// <summary>The numeric maximum of <paramref name="jsonPath"/>, or <c>null</c> when nothing matched.</summary>
    Task<double?> Max(string jsonPath, CancellationToken cancellationToken = default);

    /// <summary>Forward-only keyset page. The keyset is derived from this query's own <c>OrderBy</c>.</summary>
    Task<CursorPage<JsonObject>> ToCursorPage(string? cursor, int take, CancellationToken cancellationToken = default);

    /// <summary>Deletes every matching document in one statement. Returns the number removed.</summary>
    Task<int> ExecuteDelete(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets one JSON field on every matching document in one statement. Returns the number updated.
    /// <paramref name="jsonPath"/> is validated before reaching the SQL.
    /// </summary>
    Task<int> ExecuteUpdate(string jsonPath, object? value, CancellationToken cancellationToken = default);
}
