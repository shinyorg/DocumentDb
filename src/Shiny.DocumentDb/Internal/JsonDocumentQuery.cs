using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The one query implementation behind <see cref="IJsonDocumentQuery"/>, serving both keyings. It is a thin
/// adapter over <see cref="DocumentQuery{T}"/> of <see cref="JsonObject"/> bound to an explicit collection:
/// all the SQL building, cursor pagination, tenancy, telemetry and bulk operations are the shared ones.
/// </summary>
/// <remarks>
/// The only thing that differs between a name-keyed and a type-keyed collection here is which parser the
/// strings go through — schema-free paths become <c>DocumentFieldExpression</c>, typed paths resolve through
/// the document's metadata and so reach the full function set (<c>hasflag</c>, spatial, full-text).
/// </remarks>
sealed class JsonDocumentQuery : IJsonDocumentQuery
{
    readonly JsonLaneTarget target;
    readonly IDocumentQuery<JsonObject> inner;

    internal JsonDocumentQuery(JsonLaneTarget target, IDocumentQuery<JsonObject> inner)
    {
        this.target = target;
        this.inner = inner;
    }

    JsonDocumentQuery With(IDocumentQuery<JsonObject> next) => new(this.target, next);

    public IJsonDocumentQuery Where(string filter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filter);
        return this.With(this.inner.Where(this.ParseFilter(filter, null)));
    }

    public IJsonDocumentQuery Where(FilterInterpolatedStringHandler filter)
        => this.With(this.inner.Where(this.ParseFilter(filter.Filter, filter.Arguments)));

    System.Linq.Expressions.Expression<Func<JsonObject, bool>> ParseFilter(string filter, IReadOnlyList<object?>? args)
        => FilterExpressionParser.ParseJson(filter, args, this.target.TypeInfo);

    public IJsonDocumentQuery OrderBy(string ordering)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ordering);
        var next = this.inner;
        foreach (var key in DynamicOrderByParser.Parse(ordering))
        {
            var selector = FilterExpressionParser.ParseJsonValueSelector(key.Expression, this.target.TypeInfo);
            next = key.Descending ? next.OrderByDescending(selector) : next.OrderBy(selector);
        }
        return this.With(next);
    }

    public IJsonDocumentQuery Paginate(int offset, int take) => this.With(this.inner.Paginate(offset, take));

    public IGroupedDocumentQuery<JsonObject, object> GroupBy(string keyField) => this.inner.GroupBy(keyField);

    public IDocumentQuery<JsonObject> Project(string fields) => this.inner.Project(fields);

    public DocumentQueryString ToQueryString() => this.inner.ToQueryString();

    public Task<IReadOnlyList<JsonObject>> ToList(CancellationToken cancellationToken = default)
        => this.inner.ToList(cancellationToken);

    public IAsyncEnumerable<JsonObject> ToAsyncEnumerable(CancellationToken cancellationToken = default)
        => this.inner.ToAsyncEnumerable(cancellationToken);

    public Task<long> Count(CancellationToken cancellationToken = default) => this.inner.Count(cancellationToken);

    public Task<bool> Any(CancellationToken cancellationToken = default) => this.inner.Any(cancellationToken);

    public Task<double?> Sum(string jsonPath, CancellationToken cancellationToken = default)
        => this.Aggregate("SUM", jsonPath, cancellationToken);

    public Task<double?> Average(string jsonPath, CancellationToken cancellationToken = default)
        => this.Aggregate("AVG", jsonPath, cancellationToken);

    public Task<double?> Min(string jsonPath, CancellationToken cancellationToken = default)
        => this.Aggregate("MIN", jsonPath, cancellationToken);

    public Task<double?> Max(string jsonPath, CancellationToken cancellationToken = default)
        => this.Aggregate("MAX", jsonPath, cancellationToken);

    Task<double?> Aggregate(string sqlFunc, string jsonPath, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jsonPath);
        return ((DocumentQuery<JsonObject>)this.inner)
            .ScalarAggregateByPath(sqlFunc, this.ResolvePath(jsonPath), cancellationToken);
    }

    public Task<CursorPage<JsonObject>> ToCursorPage(string? cursor, int take, CancellationToken cancellationToken = default)
        => this.inner.ToCursorPage(cursor, take, cancellationToken);

    public Task<int> ExecuteDelete(CancellationToken cancellationToken = default)
        => this.inner.ExecuteDelete(cancellationToken);

    public Task<int> ExecuteUpdate(string jsonPath, object? value, CancellationToken cancellationToken = default)
        => ((DocumentQuery<JsonObject>)this.inner)
            .ExecuteUpdatePath(this.ResolvePath(jsonPath), value, cancellationToken);

    /// <summary>
    /// Resolves a caller-supplied path for the APIs that bypass the parser (<c>ExecuteUpdate</c>, the scalar
    /// aggregates), so it is validated before it reaches the interpolated SQL.
    /// </summary>
    string ResolvePath(string jsonPath)
        => this.target.TypeInfo is { } info
            ? JsonPathFieldBinder.ResolvePath(info, jsonPath).JsonPath
            : DynamicPathResolver.ResolveJsonPathWithType(jsonPath).JsonPath;
}
