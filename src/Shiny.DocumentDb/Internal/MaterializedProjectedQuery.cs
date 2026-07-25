using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Diagnostics;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The result of <c>Select(...)</c> on a client-side-projecting store: the source query's rows run through the
/// compiled selector. A projection is a terminal shape — every builder call and every set-based write on it
/// throws, because the source is where filtering, ordering, and paging belong.
/// <para>
/// One implementation for every document provider; the source is supplied as a delegate so the projection knows
/// nothing about the store it came from.
/// </para>
/// </summary>
sealed class MaterializedProjectedQuery<TSource, TResult> : IDocumentQuery<TResult>
    where TSource : class
    where TResult : class
{
    readonly Func<CancellationToken, Task<IEnumerable<TSource>>> source;
    readonly Func<TSource, TResult> selector;
    readonly OperationTracker tracker;

    internal MaterializedProjectedQuery(
        Func<CancellationToken, Task<IEnumerable<TSource>>> source,
        Func<TSource, TResult> selector,
        OperationTracker tracker)
    {
        this.source = source;
        this.selector = selector;
        this.tracker = tracker;
    }

    public JsonTypeInfo<TResult>? QueryTypeInfo => null;

    async Task<IEnumerable<TResult>> Materialize(CancellationToken ct)
        => (await this.source(ct).ConfigureAwait(false)).Select(this.selector);

    // ── Terminals ───────────────────────────────────────────────────────

    public Task<IReadOnlyList<TResult>> ToList(CancellationToken ct = default)
        => this.tracker.Track("query.to_list", typeof(TResult).Name, async () =>
        {
            var rows = await this.Materialize(ct).ConfigureAwait(false);
            return (IReadOnlyList<TResult>)rows.ToList().AsReadOnly();
        }, r => r.Count);

    public async IAsyncEnumerable<TResult> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        var rows = await this.Materialize(ct).ConfigureAwait(false);
        foreach (var row in rows)
        {
            ct.ThrowIfCancellationRequested();
            yield return row;
        }
    }

    public Task<long> Count(CancellationToken ct = default)
        => this.tracker.Track("query.count", typeof(TResult).Name, async () =>
            (await this.Materialize(ct).ConfigureAwait(false)).LongCount(), r => r);

    public Task<bool> Any(CancellationToken ct = default)
        => this.tracker.Track("query.any", typeof(TResult).Name, async () =>
            (await this.Materialize(ct).ConfigureAwait(false)).Any(), r => r ? 1 : 0);

    public Task<TValue> Max<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => this.tracker.Track("query.max", typeof(TResult).Name, async () =>
            (await this.Materialize(ct).ConfigureAwait(false)).Max(ExpressionInterpreter.Interpret(selector))!);

    public Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => this.tracker.Track("query.min", typeof(TResult).Name, async () =>
            (await this.Materialize(ct).ConfigureAwait(false)).Min(ExpressionInterpreter.Interpret(selector))!);

    public Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => this.tracker.Track("query.sum", typeof(TResult).Name, async () =>
        {
            var compiled = ExpressionInterpreter.Interpret(selector);
            var rows = await this.Materialize(ct).ConfigureAwait(false);
            return rows.Select(compiled).Aggregate(default(TValue)!, DynamicAdd);
        });

    public Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default)
        => this.tracker.Track("query.average", typeof(TResult).Name, async () =>
        {
            var compiled = ExpressionInterpreter.Interpret(selector);
            var rows = await this.Materialize(ct).ConfigureAwait(false);
            return rows.Average(x => Convert.ToDouble(compiled(x)));
        });

    // ── Not valid after a projection ────────────────────────────────────

    public IDocumentQuery<TResult> Where(Expression<Func<TResult, bool>> predicate)
        => throw NotAfterSelect("Where");

    public IDocumentQuery<TResult> IgnoreQueryFilters()
        => throw new NotSupportedException("Cannot call IgnoreQueryFilters after Select. Call it on the source query before projecting.");

    public IDocumentQuery<TResult> IgnoreQueryFilters(params string[] filterNames)
        => throw new NotSupportedException("Cannot call IgnoreQueryFilters after Select. Call it on the source query before projecting.");

    public IDocumentQuery<TResult> OrderBy(Expression<Func<TResult, object>> selector)
        => throw NotAfterSelect("OrderBy");

    public IDocumentQuery<TResult> OrderByDescending(Expression<Func<TResult, object>> selector)
        => throw NotAfterSelect("OrderByDescending");

    public IDocumentQuery<TResult> Paginate(int offset, int take)
        => throw NotAfterSelect("Paginate");

    public IGroupedDocumentQuery<TResult, TKey> GroupBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        => throw NotAfterSelect("GroupBy");

    public IGroupedDocumentQuery<TResult, object> GroupBy(string keyField)
        => throw NotAfterSelect("GroupBy");

    public IDocumentQuery<TNewResult> Select<TNewResult>(
        Expression<Func<TResult, TNewResult>> selector,
        JsonTypeInfo<TNewResult>? resultTypeInfo = null) where TNewResult : class
        => throw new NotSupportedException("Cannot apply Select twice. Project once, then shape the results in the consumer.");

    public IDocumentQuery<JsonObject> Project(string fields, JsonTypeInfo<TResult>? jsonTypeInfo = null)
        => throw new NotSupportedException("Cannot project after Select. Call Project on the source query.");

    public Task<CursorPage<TResult>> ToCursorPage(string? cursor, int take, CancellationToken ct = default)
        => throw NotAfterSelect("ToCursorPage");

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteDelete after Select. Run the set-based write on the source query.");

    public Task<int> ExecuteUpdate(Expression<Func<TResult, object>> property, object? value, CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteUpdate after Select. Run the set-based write on the source query.");

    public DocumentQueryString ToQueryString()
        => throw new NotSupportedException("ToQueryString is not supported after Select.");

    public IAsyncEnumerable<DocumentChange<TResult>> NotifyOnChange(CancellationToken ct = default)
        => throw new NotSupportedException(
            "NotifyOnChange is not supported after Select. Subscribe before projecting, " +
            "or use IObservableDocumentStore.NotifyOnChange<T>() and project in the consumer.");

    public Task<IReadOnlyList<FullTextResult<TResult>>> FullTextMatch(string searchText, int maxResults = 50, CancellationToken ct = default)
        => throw new NotSupportedException(
            "FullTextMatch is not supported after Select. Run the full-text search first, then project the results in the consumer.");

    public Task<IReadOnlyList<VectorResult<TResult>>> NearestVectors(ReadOnlyMemory<float> query, int k, CancellationToken ct = default)
        => throw new NotSupportedException(
            "NearestVectors is not supported after Select. Run the vector search first, then project the results in the consumer.");

    static NotSupportedException NotAfterSelect(string op)
        => new($"Cannot chain {op} after Select. Apply it to the source query before projecting.");

    static TValue DynamicAdd<TValue>(TValue a, TValue b) => a switch
    {
        int ai when b is int bi => (TValue)(object)(ai + bi),
        long al when b is long bl => (TValue)(object)(al + bl),
        double ad when b is double bd => (TValue)(object)(ad + bd),
        decimal am when b is decimal bm => (TValue)(object)(am + bm),
        float af when b is float bf => (TValue)(object)(af + bf),
        _ => throw new NotSupportedException($"Sum is not supported for type {typeof(TValue).Name}")
    };
}
