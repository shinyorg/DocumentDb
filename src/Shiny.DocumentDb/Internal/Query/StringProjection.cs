using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// Shared client-side string projection used by the document/in-memory providers (CosmosDB, MongoDB,
/// LiteDB, IndexedDB) for <c>Project(string)</c>. Parses the projection list (fields + scalar functions),
/// builds a compile-free getter per item via <see cref="ExpressionInterpreter"/>, and maps each source
/// document to a <see cref="JsonObject"/>. The relational providers project server-side in SQL instead.
/// </summary>
static class StringProjection
{
    public static List<(string Alias, Func<T, object?> Get)> BuildGetters<T>(string fields, JsonTypeInfo<T> typeInfo,
        IReadOnlyDictionary<string, ComputedMapping>? computed = null) where T : class
    {
        var (param, items) = FilterExpressionParser.ParseProjection(fields, typeInfo, computed);
        var getters = new List<(string, Func<T, object?>)>(items.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var item in items)
        {
            Expression body;
            string alias;
            if (item.FieldPath != null)
            {
                var (memberBody, _) = DocumentQueryExtensions.BuildMemberAccess(param, item.FieldPath, typeInfo, computed);
                body = memberBody;
                var leaf = computed != null && computed.TryGetValue(item.FieldPath, out var cm)
                    ? cm.JsonName
                    : DocumentQueryExtensions.ResolveJsonPath(item.FieldPath, typeInfo).LeafJsonName;
                alias = item.Alias ?? leaf;
            }
            else
            {
                body = item.ValueExpr!;
                alias = item.Alias!;
            }

            if (!seen.Add(alias))
                throw new ArgumentException(
                    $"Projection resolves to duplicate output key '{alias}'. Projected fields must have unique names.",
                    nameof(fields));

            var lambda = Expression.Lambda<Func<T, object?>>(Expression.Convert(body, typeof(object)), param);
            getters.Add((alias, ExpressionInterpreter.Interpret(lambda)));
        }

        if (getters.Count == 0)
            throw new ArgumentException("At least one field must be specified.", nameof(fields));

        return getters;
    }

    public static JsonObject ToJsonObject<T>(T document, List<(string Alias, Func<T, object?> Get)> getters)
    {
        var obj = new JsonObject();
        foreach (var (alias, get) in getters)
            obj[alias] = ToNode(get(document));
        return obj;
    }

    static JsonNode? ToNode(object? value) => value switch
    {
        null => null,
        JsonNode n => n.DeepClone(),
        string s => JsonValue.Create(s),
        bool b => JsonValue.Create(b),
        int i => JsonValue.Create(i),
        long l => JsonValue.Create(l),
        short sh => JsonValue.Create(sh),
        byte bt => JsonValue.Create(bt),
        double d => JsonValue.Create(d),
        float f => JsonValue.Create(f),
        decimal m => JsonValue.Create(m),
        Guid g => JsonValue.Create(g.ToString()),
        DateTime dt => JsonValue.Create(dt),
        DateTimeOffset dto => JsonValue.Create(dto),
        Enum e => JsonValue.Create(Convert.ToInt64(e)),
        _ => JsonValue.Create(value.ToString())
    };
}

/// <summary>
/// Client-side <c>Project(string)</c> result over a source query — wraps an <see cref="IDocumentQuery{T}"/>
/// and maps each materialized document to a <see cref="JsonObject"/>. Terminal operations delegate to the
/// source; query-shaping operations after a projection throw.
/// </summary>
sealed class StringProjectionQuery<T>(IDocumentQuery<T> source, List<(string Alias, Func<T, object?> Get)> getters)
    : IDocumentQuery<JsonObject> where T : class
{
    public JsonTypeInfo<JsonObject>? QueryTypeInfo => null;

    public async Task<IReadOnlyList<JsonObject>> ToList(CancellationToken ct = default)
    {
        var docs = await source.ToList(ct).ConfigureAwait(false);
        var result = new List<JsonObject>(docs.Count);
        foreach (var d in docs)
            result.Add(StringProjection.ToJsonObject(d, getters));
        return result;
    }

    public async IAsyncEnumerable<JsonObject> ToAsyncEnumerable([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var d in source.ToAsyncEnumerable(ct).ConfigureAwait(false))
            yield return StringProjection.ToJsonObject(d, getters);
    }

    public IDocumentQuery<JsonObject> Paginate(int offset, int take)
        => new StringProjectionQuery<T>(source.Paginate(offset, take), getters);

    public Task<long> Count(CancellationToken ct = default) => source.Count(ct);
    public Task<bool> Any(CancellationToken ct = default) => source.Any(ct);

    public IDocumentQuery<JsonObject> Where(Expression<Func<JsonObject, bool>> predicate) => throw NotAfterProject();
    public IDocumentQuery<JsonObject> IgnoreQueryFilters() => throw NotAfterProject();
    public IDocumentQuery<JsonObject> IgnoreQueryFilters(params string[] filterNames) => throw NotAfterProject();
    public IDocumentQuery<JsonObject> OrderBy(Expression<Func<JsonObject, object>> selector) => throw NotAfterProject();
    public IDocumentQuery<JsonObject> OrderByDescending(Expression<Func<JsonObject, object>> selector) => throw NotAfterProject();
    public IGroupedDocumentQuery<JsonObject, TKey> GroupBy<TKey>(Expression<Func<JsonObject, TKey>> keySelector) => throw NotAfterProject();

    [UnconditionalSuppressMessage("Trimming", "IL2091", Justification = "Throwing stub — TResult is never materialized.")]
    public IDocumentQuery<TResult> Select<TResult>(Expression<Func<JsonObject, TResult>> selector, JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class => throw NotAfterProject();
    public Task<int> ExecuteDelete(CancellationToken ct = default) => throw NotAfterProject();
    public Task<int> ExecuteUpdate(Expression<Func<JsonObject, object>> property, object? value, CancellationToken ct = default) => throw NotAfterProject();
    public Task<TValue> Max<TValue>(Expression<Func<JsonObject, TValue>> selector, CancellationToken ct = default) => throw NotAfterProject();
    public Task<TValue> Min<TValue>(Expression<Func<JsonObject, TValue>> selector, CancellationToken ct = default) => throw NotAfterProject();
    public Task<TValue> Sum<TValue>(Expression<Func<JsonObject, TValue>> selector, CancellationToken ct = default) => throw NotAfterProject();
    public Task<double> Average(Expression<Func<JsonObject, object>> selector, CancellationToken ct = default) => throw NotAfterProject();
    public IAsyncEnumerable<DocumentChange<JsonObject>> NotifyOnChange(CancellationToken ct = default) => throw NotAfterProject();

    static InvalidOperationException NotAfterProject() => new("Cannot modify or aggregate a query after Project.");
}
