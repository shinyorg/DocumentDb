using System.Linq.Expressions;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Diagnostics;

/// <summary>
/// Wraps an <see cref="IDocumentQuery{T}"/> so the fluent builder stays composable (each operator
/// returns another instrumented query) while the <b>terminal</b> operators — ToList, ToAsyncEnumerable,
/// Count, Any, ExecuteDelete/Update, the aggregates, and NearestVectors — emit a metric and span.
/// <c>NotifyOnChange</c> is a long-lived subscription and is passed through without per-event telemetry.
/// </summary>
sealed class InstrumentedDocumentQuery<T>(IDocumentQuery<T> inner, OperationTracker tracker) : IDocumentQuery<T>
    where T : class
{
    static string Coll => typeof(T).Name;

    IDocumentQuery<T> Wrap(IDocumentQuery<T> q) => new InstrumentedDocumentQuery<T>(q, tracker);

    public JsonTypeInfo<T>? QueryTypeInfo => inner.QueryTypeInfo;

    // ── builder operators (no I/O) ──────────────────────────────────────
    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate) => this.Wrap(inner.Where(predicate));
    public IDocumentQuery<T> IgnoreQueryFilters() => this.Wrap(inner.IgnoreQueryFilters());
    public IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames) => this.Wrap(inner.IgnoreQueryFilters(filterNames));
    public IDocumentQuery<T> OrderBy(Expression<Func<T, object>> selector) => this.Wrap(inner.OrderBy(selector));
    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector) => this.Wrap(inner.OrderByDescending(selector));
    public IDocumentQuery<T> GroupBy(Expression<Func<T, object>> selector) => this.Wrap(inner.GroupBy(selector));
    public IDocumentQuery<T> Paginate(int offset, int take) => this.Wrap(inner.Paginate(offset, take));

    public IDocumentQuery<TResult> Select<TResult>(Expression<Func<T, TResult>> selector, JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
        => new InstrumentedDocumentQuery<TResult>(inner.Select(selector, resultTypeInfo), tracker);

    public IDocumentQuery<JsonObject> Project(string fields, JsonTypeInfo<T>? jsonTypeInfo = null)
        => new InstrumentedDocumentQuery<JsonObject>(inner.Project(fields, jsonTypeInfo), tracker);

    // ── terminal operators (instrumented) ───────────────────────────────
    public Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
        => tracker.Track("query.to_list", Coll, () => inner.ToList(ct), r => r.Count);

    public IAsyncEnumerable<T> ToAsyncEnumerable(CancellationToken ct = default)
        => tracker.TrackStream("query.stream", Coll, inner.ToAsyncEnumerable(ct), ct);

    public Task<long> Count(CancellationToken ct = default)
        => tracker.Track("query.count", Coll, () => inner.Count(ct), r => r);

    public Task<bool> Any(CancellationToken ct = default)
        => tracker.Track("query.any", Coll, () => inner.Any(ct), r => r ? 1 : 0);

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => tracker.Track("query.execute_delete", Coll, () => inner.ExecuteDelete(ct), r => r);

    public Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
        => tracker.Track("query.execute_update", Coll, () => inner.ExecuteUpdate(property, value, ct), r => r);

    public Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => tracker.Track("query.max", Coll, () => inner.Max(selector, ct));

    public Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => tracker.Track("query.min", Coll, () => inner.Min(selector, ct));

    public Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => tracker.Track("query.sum", Coll, () => inner.Sum(selector, ct));

    public Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
        => tracker.Track("query.average", Coll, () => inner.Average(selector, ct));

    public Task<IReadOnlyList<VectorResult<T>>> NearestVectors(ReadOnlyMemory<float> query, int k, CancellationToken ct = default)
        => tracker.Track("query.nearest_vectors", Coll, () => inner.NearestVectors(query, k, ct), r => r.Count);

    // Long-lived subscription — passed through unwrapped.
    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange(CancellationToken ct = default)
        => inner.NotifyOnChange(ct);
}
