using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Google.Cloud.Firestore;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Firestore;

/// <summary>
/// <see cref="IDocumentQuery{T}"/> for Firestore. Pushable predicates (equality / single-field range on
/// string/bool/integral/enum) are applied server-side to shrink reads; the full predicate set, ordering, and
/// paging are always re-applied client-side via <see cref="ExpressionInterpreter"/> so results are correct
/// even when a clause could not be pushed. <see cref="ToCursorPage"/> uses native Firestore keyset cursors.
/// </summary>
public class FirestoreDocumentQuery<T> : IDocumentQuery<T> where T : class
{
    readonly FirestoreDocumentStore store;
    readonly JsonTypeInfo<T>? typeInfo;

    public JsonTypeInfo<T>? QueryTypeInfo => this.typeInfo;

    readonly List<Expression<Func<T, bool>>> predicates = new();
    readonly List<(Expression<Func<T, object>> Selector, bool Descending)> orderBys = new();
    int? skipCount;
    int? takeCount;
    bool ignoreAllFilters;
    HashSet<string>? ignoredFilterNames;

    internal FirestoreDocumentStore Store => this.store;

    internal FirestoreDocumentQuery(FirestoreDocumentStore store, JsonTypeInfo<T>? typeInfo)
    {
        this.store = store;
        this.typeInfo = typeInfo;
    }

    FirestoreDocumentQuery(FirestoreDocumentQuery<T> source)
    {
        this.store = source.store;
        this.typeInfo = source.typeInfo;
        this.predicates.AddRange(source.predicates);
        this.orderBys.AddRange(source.orderBys);
        this.skipCount = source.skipCount;
        this.takeCount = source.takeCount;
        this.ignoreAllFilters = source.ignoreAllFilters;
        this.ignoredFilterNames = source.ignoredFilterNames is null
            ? null
            : new HashSet<string>(source.ignoredFilterNames, StringComparer.Ordinal);
    }

    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate)
    {
        var clone = new FirestoreDocumentQuery<T>(this);
        clone.predicates.Add(predicate);
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters()
    {
        var clone = new FirestoreDocumentQuery<T>(this);
        clone.ignoreAllFilters = true;
        return clone;
    }

    public IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames)
    {
        ArgumentNullException.ThrowIfNull(filterNames);
        var clone = new FirestoreDocumentQuery<T>(this);
        clone.ignoredFilterNames ??= new HashSet<string>(StringComparer.Ordinal);
        foreach (var n in filterNames)
            if (!string.IsNullOrWhiteSpace(n))
                clone.ignoredFilterNames.Add(n);
        return clone;
    }

    internal IEnumerable<Expression<Func<T, bool>>> GetEffectivePredicateExpressions()
    {
        if (!this.ignoreAllFilters)
        {
            foreach (var f in this.store.Options.ResolveQueryFilters(typeof(T)))
            {
                if (f.Name != null && this.ignoredFilterNames?.Contains(f.Name) == true)
                    continue;
                yield return (Expression<Func<T, bool>>)f.Predicate;
            }
        }
        foreach (var p in this.predicates)
            yield return p;
    }

    public IDocumentQuery<T> OrderBy(Expression<Func<T, object>> selector)
    {
        var clone = new FirestoreDocumentQuery<T>(this);
        clone.orderBys.Add((selector, false));
        return clone;
    }

    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector)
    {
        var clone = new FirestoreDocumentQuery<T>(this);
        clone.orderBys.Add((selector, true));
        return clone;
    }

    public IGroupedDocumentQuery<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        => throw NotSupportedGroupBy();

    public IGroupedDocumentQuery<T, object> GroupBy(string keyField)
        => throw NotSupportedGroupBy();

    static NotSupportedException NotSupportedGroupBy()
        => new("GroupBy is not supported on Firestore — it has no server-side grouping. " +
            "Read with a filter and aggregate client-side, or use a relational or MongoDB store.");

    public IDocumentQuery<T> Paginate(int offset, int take)
    {
        var clone = new FirestoreDocumentQuery<T>(this);
        clone.skipCount = offset;
        clone.takeCount = take;
        return clone;
    }

    public IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<T, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
        => new FirestoreProjectedDocumentQuery<T, TResult>(this, selector);

    public Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
        => this.store.Tracker.Track("query.to_list", typeof(T).Name, () => this.ToListImpl(ct), r => r.Count);

    async Task<IReadOnlyList<T>> ToListImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).ToList().AsReadOnly();

    public async IAsyncEnumerable<T> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in await this.MaterializeAsync(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public Task<long> Count(CancellationToken ct = default)
        => this.store.Tracker.Track("query.count", typeof(T).Name, () => this.CountImpl(ct), r => r);

    async Task<long> CountImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Count();

    public Task<bool> Any(CancellationToken ct = default)
        => this.store.Tracker.Track("query.any", typeof(T).Name, () => this.AnyImpl(ct), r => r ? 1 : 0);

    async Task<bool> AnyImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Any();

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => this.store.Tracker.Track("query.execute_delete", typeof(T).Name, () => this.ExecuteDeleteImpl(ct), r => r);

    async Task<int> ExecuteDeleteImpl(CancellationToken ct)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var interceptors = this.store.InterceptorPipeline;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Delete, typeName);
        await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false);

        var predicate = this.BuildCombinedPredicate();
        var pushdown = this.store.BuildPushdown(this.GetEffectivePredicateExpressions(), this.typeInfo);
        var count = await this.store.DeleteWhereAsync(predicate, pushdown, this.typeInfo, ct).ConfigureAwait(false);

        await interceptors.AfterBulk(bulkCtx, count, ct).ConfigureAwait(false);
        return count;
    }

    public Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default)
        => this.store.Tracker.Track("query.execute_update", typeof(T).Name, () => this.ExecuteUpdateImpl(property, value, ct), r => r);

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null.")]
    async Task<int> ExecuteUpdateImpl(Expression<Func<T, object>> property, object? value, CancellationToken ct)
    {
        var typeName = this.store.ResolveTypeNameFor<T>();
        var jsonPath = this.typeInfo != null
            ? IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions, this.typeInfo)
            : IndexExpressionHelper.ResolveJsonPath(property, this.store.JsonOptions);

        var interceptors = this.store.InterceptorPipeline;
        var bulkCtx = interceptors.NewBulk<T>(DocumentOperation.Update, typeName, assignment: (jsonPath, value));
        await interceptors.BeforeBulk(bulkCtx, ct).ConfigureAwait(false);

        var predicate = this.BuildCombinedPredicate();
        var pushdown = this.store.BuildPushdown(this.GetEffectivePredicateExpressions(), this.typeInfo);
        var count = await this.store.UpdatePropertyWhereAsync(predicate, jsonPath, value, pushdown, this.typeInfo, ct).ConfigureAwait(false);

        await interceptors.AfterBulk(bulkCtx, count, ct).ConfigureAwait(false);
        return count;
    }

    public Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.max", typeof(T).Name, () => this.MaxImpl(selector, ct));

    async Task<TValue> MaxImpl<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Max(ExpressionInterpreter.Interpret(selector))!;

    public Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.min", typeof(T).Name, () => this.MinImpl(selector, ct));

    async Task<TValue> MinImpl<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Min(ExpressionInterpreter.Interpret(selector))!;

    public Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.sum", typeof(T).Name, () => this.SumImpl(selector, ct));

    async Task<TValue> SumImpl<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var items = await this.MaterializeAsync(ct).ConfigureAwait(false);
        object result = items.Select(compiled).Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return (TValue)result;
    }

    public Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default)
        => this.store.Tracker.Track("query.average", typeof(T).Name, () => this.AverageImpl(selector, ct));

    async Task<double> AverageImpl(Expression<Func<T, object>> selector, CancellationToken ct)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        return (await this.MaterializeAsync(ct).ConfigureAwait(false)).Average(x => Convert.ToDouble(compiled(x)));
    }

    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange(CancellationToken ct = default)
    {
        var compiled = this.GetEffectivePredicateExpressions().Select(p => ExpressionInterpreter.Interpret(p)).ToList();
        Func<T, bool>? predicate = compiled.Count == 0 ? null : item => compiled.All(p => p(item));
        return this.store.ListenChanges(predicate, this.typeInfo, ct);
    }

    // ── Native keyset (cursor) pagination via StartAfter ────────────────

    public Task<CursorPage<T>> ToCursorPage(string? cursor, int take, CancellationToken ct = default)
    {
        if (take <= 0)
            throw new ArgumentOutOfRangeException(nameof(take), "take must be greater than zero.");
        return this.store.Tracker.Track("query.to_cursor_page", typeof(T).Name, () => this.ToCursorPageImpl(cursor, take, ct), r => r.Items.Count);
    }

    async Task<CursorPage<T>> ToCursorPageImpl(string? cursor, int take, CancellationToken ct)
    {
        // Resolve the ordering fields (defaulting to document id) so the keyset order is total.
        var orderFields = new List<(string Path, bool Descending)>();
        foreach (var (selector, descending) in this.orderBys)
        {
            var path = this.typeInfo != null
                ? IndexExpressionHelper.ResolveJsonPath(selector, this.store.JsonOptions, this.typeInfo)
                : IndexExpressionHelper.ResolveJsonPath(selector, this.store.JsonOptions);
            orderFields.Add((path, descending));
        }

        Query query = this.store.GetCollection<T>();
        // Push equality clauses only — a range clause plus keyset ordering needs a matching composite index.
        var pushdown = this.store.BuildPushdown(this.GetEffectivePredicateExpressions(), this.typeInfo);
        foreach (var clause in pushdown.Where(c => c.Op == FirestoreOp.Equal))
            query = query.WhereEqualTo(clause.Path, clause.Value);

        foreach (var (path, descending) in orderFields)
            query = descending ? query.OrderByDescending(path) : query.OrderBy(path);
        query = query.OrderBy(FieldPath.DocumentId);

        if (cursor != null)
        {
            var values = DecodeCursor(cursor, orderFields.Count);
            query = query.StartAfter(values);
        }
        query = query.Limit(take);

        var snapshot = await query.GetSnapshotAsync(ct).ConfigureAwait(false);

        var items = new List<T>(snapshot.Count);
        DocumentSnapshot? last = null;
        foreach (var doc in snapshot.Documents)
        {
            var model = this.store.DeserializeSnapshot(doc, this.typeInfo);
            if (model == null)
                continue;
            items.Add(model);
            last = doc;
        }

        string? nextCursor = null;
        if (items.Count == take && last != null)
            nextCursor = EncodeCursor(last, orderFields);

        return new CursorPage<T>(items.AsReadOnly(), nextCursor);
    }

    static string EncodeCursor(DocumentSnapshot snapshot, List<(string Path, bool Descending)> orderFields)
    {
        var map = snapshot.ToDictionary();
        var arr = new JsonArray();
        foreach (var (path, _) in orderFields)
            arr.Add(FirestoreCursorValue(ExtractByPath(map, path)));
        arr.Add(snapshot.Id);
        var json = arr.ToJsonString();
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(json));
    }

    static object[] DecodeCursor(string cursor, int orderFieldCount)
    {
        var json = Encoding.UTF8.GetString(Convert.FromBase64String(cursor));
        var arr = JsonNode.Parse(json)!.AsArray();
        var values = new object[arr.Count];
        for (var i = 0; i < arr.Count; i++)
        {
            var node = arr[i];
            // Last element is always the document id (string).
            if (i == arr.Count - 1)
            {
                values[i] = node!.GetValue<string>();
                continue;
            }
            values[i] = NodeToFirestoreValue(node);
        }
        return values;
    }

    static JsonNode? FirestoreCursorValue(object? value) => value switch
    {
        null => null,
        bool b => JsonValue.Create(b),
        string s => JsonValue.Create(s),
        long l => JsonValue.Create(l),
        int i => JsonValue.Create(i),
        double d => JsonValue.Create(d),
        _ => JsonValue.Create(value.ToString())
    };

    static object NodeToFirestoreValue(JsonNode? node)
    {
        if (node is not JsonValue v)
            return node?.ToJsonString() ?? "";
        var element = v.GetValue<JsonElement>();
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString()!,
            JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => element.GetRawText()
        };
    }

    static object? ExtractByPath(IDictionary<string, object> map, string path)
    {
        object? current = map;
        foreach (var part in path.Split('.'))
        {
            if (current is IDictionary<string, object> d && d.TryGetValue(part, out var next))
                current = next;
            else
                return null;
        }
        return current;
    }

    // ── Internal ────────────────────────────────────────────────────────

    internal async Task<IEnumerable<T>> MaterializeAsync(CancellationToken ct)
    {
        var pushdown = this.store.BuildPushdown(this.GetEffectivePredicateExpressions(), this.typeInfo);
        var items = new List<T>();
        await foreach (var doc in this.store.LoadDocumentsAsync(this.typeInfo, pushdown, ct).ConfigureAwait(false))
            items.Add(doc);

        IEnumerable<T> query = items;
        foreach (var predicate in this.GetEffectivePredicateExpressions())
            query = query.Where(ExpressionInterpreter.Interpret(predicate));

        IOrderedEnumerable<T>? ordered = null;
        foreach (var (selector, descending) in this.orderBys)
        {
            var typedFunc = ExpressionInterpreter.Interpret(selector);
            if (ordered == null)
                ordered = descending ? query.OrderByDescending(typedFunc) : query.OrderBy(typedFunc);
            else
                ordered = descending ? ordered.ThenByDescending(typedFunc) : ordered.ThenBy(typedFunc);
        }
        if (ordered != null)
            query = ordered;

        if (this.skipCount.HasValue)
            query = query.Skip(this.skipCount.Value);
        if (this.takeCount.HasValue)
            query = query.Take(this.takeCount.Value);

        return query;
    }

    Func<T, bool> BuildCombinedPredicate()
    {
        var compiled = this.GetEffectivePredicateExpressions().Select(p => ExpressionInterpreter.Interpret(p)).ToList();
        if (compiled.Count == 0)
            return _ => true;
        return item => compiled.All(p => p(item));
    }

    static TVal DynamicAdd<TVal>(TVal a, TVal b)
    {
        if (a is int ai && b is int bi) return (TVal)(object)(ai + bi);
        if (a is long al && b is long bl) return (TVal)(object)(al + bl);
        if (a is double ad && b is double bd) return (TVal)(object)(ad + bd);
        if (a is decimal am && b is decimal bm) return (TVal)(object)(am + bm);
        if (a is float af && b is float bf) return (TVal)(object)(af + bf);
        throw new NotSupportedException($"Sum is not supported for type {typeof(TVal).Name}");
    }
}

internal class FirestoreProjectedDocumentQuery<TSource, TResult> : IDocumentQuery<TResult>
    where TSource : class
    where TResult : class
{
    readonly FirestoreDocumentQuery<TSource> source;
    readonly Func<TSource, TResult> compiledSelector;

    internal FirestoreProjectedDocumentQuery(FirestoreDocumentQuery<TSource> source, Expression<Func<TSource, TResult>> selector)
    {
        this.source = source;
        this.compiledSelector = ExpressionInterpreter.Interpret(selector);
    }

    async Task<IEnumerable<TResult>> MaterializeAsync(CancellationToken ct)
        => (await this.source.MaterializeAsync(ct).ConfigureAwait(false)).Select(this.compiledSelector);

    public IDocumentQuery<TResult> Where(Expression<Func<TResult, bool>> predicate)
        => throw new NotSupportedException("Cannot chain Where after Select. Apply filters before Select.");

    public IDocumentQuery<TResult> IgnoreQueryFilters()
        => throw new NotSupportedException("Cannot call IgnoreQueryFilters after Select. Call it on the source query before projecting.");

    public IDocumentQuery<TResult> IgnoreQueryFilters(params string[] filterNames)
        => throw new NotSupportedException("Cannot call IgnoreQueryFilters after Select. Call it on the source query before projecting.");

    public IDocumentQuery<TResult> OrderBy(Expression<Func<TResult, object>> selector)
        => throw new NotSupportedException("Cannot chain OrderBy after Select. Apply ordering before Select.");

    public IDocumentQuery<TResult> OrderByDescending(Expression<Func<TResult, object>> selector)
        => throw new NotSupportedException("Cannot chain OrderByDescending after Select. Apply ordering before Select.");

    public IGroupedDocumentQuery<TResult, TKey> GroupBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        => throw new NotSupportedException("Cannot chain GroupBy after Select.");

    public IDocumentQuery<TResult> Paginate(int offset, int take)
        => throw new NotSupportedException("Cannot chain Paginate after Select. Apply pagination before Select.");

    public IDocumentQuery<TNewResult> Select<TNewResult>(
        Expression<Func<TResult, TNewResult>> selector,
        JsonTypeInfo<TNewResult>? resultTypeInfo = null) where TNewResult : class
        => throw new NotSupportedException("Cannot chain Select after Select.");

    public Task<IReadOnlyList<TResult>> ToList(CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.to_list", typeof(TResult).Name, () => this.ToListImpl(ct), r => r.Count);

    async Task<IReadOnlyList<TResult>> ToListImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).ToList().AsReadOnly();

    public async IAsyncEnumerable<TResult> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in await this.MaterializeAsync(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    public Task<long> Count(CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.count", typeof(TResult).Name, () => this.CountImpl(ct), r => r);

    async Task<long> CountImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Count();

    public Task<bool> Any(CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.any", typeof(TResult).Name, () => this.AnyImpl(ct), r => r ? 1 : 0);

    async Task<bool> AnyImpl(CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Any();

    public Task<int> ExecuteDelete(CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteDelete after Select.");

    public Task<int> ExecuteUpdate(Expression<Func<TResult, object>> property, object? value, CancellationToken ct = default)
        => throw new NotSupportedException("Cannot ExecuteUpdate after Select.");

    public Task<TValue> Max<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.max", typeof(TResult).Name, () => this.MaxImpl(selector, ct));

    async Task<TValue> MaxImpl<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Max(ExpressionInterpreter.Interpret(selector))!;

    public Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.min", typeof(TResult).Name, () => this.MinImpl(selector, ct));

    async Task<TValue> MinImpl<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct)
        => (await this.MaterializeAsync(ct).ConfigureAwait(false)).Min(ExpressionInterpreter.Interpret(selector))!;

    public Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.sum", typeof(TResult).Name, () => this.SumImpl(selector, ct));

    async Task<TValue> SumImpl<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        var items = (await this.MaterializeAsync(ct).ConfigureAwait(false)).Select(compiled);
        object result = items.Aggregate(default(TValue)!, (acc, val) => DynamicAdd(acc, val));
        return (TValue)result;
    }

    public Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default)
        => this.source.Store.Tracker.Track("query.average", typeof(TResult).Name, () => this.AverageImpl(selector, ct));

    async Task<double> AverageImpl(Expression<Func<TResult, object>> selector, CancellationToken ct)
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        return (await this.MaterializeAsync(ct).ConfigureAwait(false)).Average(x => Convert.ToDouble(compiled(x)));
    }

    public IAsyncEnumerable<DocumentChange<TResult>> NotifyOnChange(CancellationToken ct = default)
        => throw new NotSupportedException("NotifyOnChange is not supported after Select.");

    static TVal DynamicAdd<TVal>(TVal a, TVal b)
    {
        if (a is int ai && b is int bi) return (TVal)(object)(ai + bi);
        if (a is long al && b is long bl) return (TVal)(object)(al + bl);
        if (a is double ad && b is double bd) return (TVal)(object)(ad + bd);
        if (a is decimal am && b is decimal bm) return (TVal)(object)(am + bm);
        throw new NotSupportedException($"Sum is not supported for type {typeof(TVal).Name}");
    }
}
