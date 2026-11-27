using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Hosting;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb;

/// <summary>
/// Loads the documents a query's results refer to, in one batched read per related type, instead of one
/// <c>Get</c> per result:
/// <code>
/// var customers = new Dictionary&lt;string, Customer&gt;();
/// var orders = await store.Query&lt;Order&gt;()
///     .Where(o => o.Status == "open")
///     .Include(o => o.CustomerId, customers)
///     .ToList();
///
/// foreach (var order in orders)
///     Console.WriteLine($"{order.Id} for {customers[order.CustomerId].Name}");
/// </code>
/// <para>
/// It works on every provider: once the results are read, the distinct keys are collected and the related documents
/// are read with an <c>IN</c> query on their id (in batches of <see cref="BatchSize"/> keys), through the same store
/// — or unit of work — the query runs in, with that type's query filters applied. A key with no matching document
/// (or one a filter hides) is simply absent from the dictionary. Chain several <c>Include</c> calls to load several
/// relations. The related documents arrive with <c>ToList</c>, <c>ToAsyncEnumerable</c> (per batch of results),
/// <c>First</c> / <c>Single</c> and their <c>OrDefault</c> forms, <c>ToCursorPage</c>, <c>NearestVectors</c> and
/// <c>FullTextMatch</c>; counts, aggregates, projections and set-based writes ignore the <c>Include</c>.
/// </para>
/// </summary>
public static class DocumentIncludeExtensions
{
    /// <summary>How many keys go into one <c>IN</c> query when loading related documents.</summary>
    public const int BatchSize = 500;

    /// <summary>
    /// After the query runs, loads the <typeparamref name="TRelated"/> documents whose id is <paramref name="foreignKey"/>
    /// of a result into <paramref name="into"/>, keyed by id.
    /// </summary>
    /// <param name="query">The query whose results hold the keys.</param>
    /// <param name="foreignKey">Selects the related document's id from a result, e.g. <c>o =&gt; o.CustomerId</c>. Null keys are skipped.</param>
    /// <param name="into">Receives the related documents, keyed by id. Existing entries are kept.</param>
    public static IDocumentQuery<T> Include<T, TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TRelated>(
        this IDocumentQuery<T> query,
        Expression<Func<T, TKey>> foreignKey,
        IDictionary<TKey, TRelated> into)
        where T : class
        where TKey : notnull
        where TRelated : class
    {
        ArgumentNullException.ThrowIfNull(foreignKey);
        var select = ExpressionInterpreter.Interpret(foreignKey);
        return AddInclude(query, into, results => results.Select(select));
    }

    /// <inheritdoc cref="Include{T, TKey, TRelated}(IDocumentQuery{T}, Expression{Func{T, TKey}}, IDictionary{TKey, TRelated})"/>
    public static IDocumentQuery<T> Include<T, TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TRelated>(
        this IDocumentQuery<T> query,
        Expression<Func<T, TKey?>> foreignKey,
        IDictionary<TKey, TRelated> into)
        where T : class
        where TKey : struct
        where TRelated : class
    {
        ArgumentNullException.ThrowIfNull(foreignKey);
        var select = ExpressionInterpreter.Interpret(foreignKey);
        return AddInclude(query, into, results => results.Select(select).Where(k => k.HasValue).Select(k => k!.Value));
    }

    /// <summary>
    /// After the query runs, loads the <typeparamref name="TRelated"/> documents whose ids are in a result's
    /// <paramref name="foreignKeys"/> collection into <paramref name="into"/>, keyed by id.
    /// </summary>
    /// <param name="query">The query whose results hold the keys.</param>
    /// <param name="foreignKeys">Selects a collection of related ids from a result, e.g. <c>o =&gt; o.ProductIds</c>.</param>
    /// <param name="into">Receives the related documents, keyed by id. Existing entries are kept.</param>
    public static IDocumentQuery<T> Include<T, TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TRelated>(
        this IDocumentQuery<T> query,
        Expression<Func<T, IEnumerable<TKey>?>> foreignKeys,
        IDictionary<TKey, TRelated> into)
        where T : class
        where TKey : notnull
        where TRelated : class
    {
        ArgumentNullException.ThrowIfNull(foreignKeys);
        var select = ExpressionInterpreter.Interpret(foreignKeys);
        return AddInclude(query, into, results => results.SelectMany(r => select(r) ?? []));
    }

    static IDocumentQuery<T> AddInclude<T, TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TRelated>(
        IDocumentQuery<T> query,
        IDictionary<TKey, TRelated> into,
        Func<IReadOnlyList<T>, IEnumerable<TKey?>> keys)
        where T : class
        where TKey : notnull
        where TRelated : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(into);

        var existing = query as IncludeQuery<T>;
        var store = (query as IStoreBoundQuery)?.Store
                    ?? throw new NotSupportedException(
                        "Include needs a query that knows its store — one created by store.Query<T>() on a Shiny.DocumentDb provider.");

        Func<IReadOnlyList<T>, CancellationToken, Task> loader = (results, ct) => LoadAsync(store, keys(results), into, ct);
        return new IncludeQuery<T>(existing?.Inner ?? query, store, [.. existing?.Loaders ?? [], loader]);
    }

    static async Task LoadAsync<TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TRelated>(
        IDocumentStore store,
        IEnumerable<TKey?> keys,
        IDictionary<TKey, TRelated> into,
        CancellationToken ct)
        where TKey : notnull
        where TRelated : class
    {
        var wanted = keys.Where(k => k is not null && !into.ContainsKey(k)).Distinct().Cast<TKey>().ToList();
        if (wanted.Count == 0)
            return;

        var idName = store.GetMappings()?.ResolveIdPropertyName(typeof(TRelated)) ?? "Id";
        var idProperty = typeof(TRelated).GetProperty(idName)
                         ?? throw new InvalidOperationException($"'{typeof(TRelated).Name}' has no '{idName}' property to match included keys against.");

        foreach (var batch in wanted.Chunk(BatchSize))
        {
            var related = await store.Query<TRelated>().WhereIn(idName, batch).ToList(ct).ConfigureAwait(false);
            foreach (var document in related)
            {
                if (idProperty.GetValue(document) is TKey id)
                    into[id] = document;
            }
        }
    }
}

/// <summary>
/// The query <c>Include</c> returns: forwards everything to the query it wraps, and after a materializing terminal runs
/// the include loaders over the results. Builder calls keep the wrapper so includes survive further composition.
/// </summary>
sealed class IncludeQuery<T>(IDocumentQuery<T> inner, IDocumentStore store, IReadOnlyList<Func<IReadOnlyList<T>, CancellationToken, Task>> loaders)
    : IDocumentQuery<T>, IStoreBoundQuery, IComputedAwareQuery where T : class
{
    const int StreamBatch = 100;

    public IDocumentQuery<T> Inner => inner;
    public IDocumentStore Store => store;
    public IReadOnlyList<Func<IReadOnlyList<T>, CancellationToken, Task>> Loaders => loaders;

    IDocumentStore? IStoreBoundQuery.Store => store;
    IReadOnlyDictionary<string, ComputedMapping>? IComputedAwareQuery.ComputedLookup => (inner as IComputedAwareQuery)?.ComputedLookup;

    IncludeQuery<T> Wrap(IDocumentQuery<T> query) => new(query, store, loaders);

    async Task<TList> RunAsync<TList>(TList results, Func<TList, IReadOnlyList<T>> documents, CancellationToken ct)
    {
        var docs = documents(results);
        if (docs.Count > 0)
        {
            foreach (var loader in loaders)
                await loader(docs, ct).ConfigureAwait(false);
        }
        return results;
    }

    // ── Builders keep the includes ──────────────────────────────────────

    public JsonTypeInfo<T>? QueryTypeInfo => inner.QueryTypeInfo;
    public IDocumentQuery<T> Where(Expression<Func<T, bool>> predicate) => this.Wrap(inner.Where(predicate));
    public IDocumentQuery<T> IgnoreQueryFilters() => this.Wrap(inner.IgnoreQueryFilters());
    public IDocumentQuery<T> IgnoreQueryFilters(params string[] filterNames) => this.Wrap(inner.IgnoreQueryFilters(filterNames));
    public IDocumentQuery<T> OrderBy(Expression<Func<T, object>> selector) => this.Wrap(inner.OrderBy(selector));
    public IDocumentQuery<T> OrderByDescending(Expression<Func<T, object>> selector) => this.Wrap(inner.OrderByDescending(selector));
    public IDocumentQuery<T> Paginate(int offset, int take) => this.Wrap(inner.Paginate(offset, take));

    // ── Terminals that return documents run the includes ────────────────

    public async Task<IReadOnlyList<T>> ToList(CancellationToken ct = default)
        => await this.RunAsync(await inner.ToList(ct).ConfigureAwait(false), x => x, ct).ConfigureAwait(false);

    public async IAsyncEnumerable<T> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        var batch = new List<T>(StreamBatch);
        await foreach (var document in inner.ToAsyncEnumerable(ct).ConfigureAwait(false))
        {
            batch.Add(document);
            if (batch.Count == StreamBatch)
            {
                foreach (var loaded in await this.RunAsync(batch, x => x, ct).ConfigureAwait(false))
                    yield return loaded;
                batch = new List<T>(StreamBatch);
            }
        }
        foreach (var loaded in await this.RunAsync(batch, x => x, ct).ConfigureAwait(false))
            yield return loaded;
    }

    public async Task<T?> FirstOrDefault(CancellationToken ct = default)
    {
        var first = await inner.FirstOrDefault(ct).ConfigureAwait(false);
        return first is null ? null : (await this.RunAsync<IReadOnlyList<T>>([first], x => x, ct).ConfigureAwait(false))[0];
    }

    public async Task<T?> SingleOrDefault(CancellationToken ct = default)
    {
        var single = await inner.SingleOrDefault(ct).ConfigureAwait(false);
        return single is null ? null : (await this.RunAsync<IReadOnlyList<T>>([single], x => x, ct).ConfigureAwait(false))[0];
    }

    public async Task<CursorPage<T>> ToCursorPage(string? cursor, int take, CancellationToken ct = default)
        => await this.RunAsync(await inner.ToCursorPage(cursor, take, ct).ConfigureAwait(false), x => x.Items, ct).ConfigureAwait(false);

    public async Task<IReadOnlyList<VectorResult<T>>> NearestVectors(ReadOnlyMemory<float> query, int k, CancellationToken ct = default)
        => await this.RunAsync(await inner.NearestVectors(query, k, ct).ConfigureAwait(false), x => x.Select(r => r.Document).ToList(), ct).ConfigureAwait(false);

    public async Task<IReadOnlyList<FullTextResult<T>>> FullTextMatch(string searchText, int maxResults = 50, CancellationToken ct = default)
        => await this.RunAsync(await inner.FullTextMatch(searchText, maxResults, ct).ConfigureAwait(false), x => x.Select(r => r.Document).ToList(), ct).ConfigureAwait(false);

    // ── Everything else ignores the includes ────────────────────────────

    public IGroupedDocumentQuery<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector) => inner.GroupBy(keySelector);
    public IGroupedDocumentQuery<T, object> GroupBy(string keyField) => inner.GroupBy(keyField);
    public IDocumentQuery<TResult> Select<TResult>(Expression<Func<T, TResult>> selector, JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
        => inner.Select(selector, resultTypeInfo);
    public IDocumentQuery<JsonObject> Project(string fields, JsonTypeInfo<T>? jsonTypeInfo = null) => inner.Project(fields, jsonTypeInfo);
    public IJoinQuery<T, TRight> Join<TRight>(Expression<Func<T, TRight, bool>> on, JoinKind kind = JoinKind.Inner, JsonTypeInfo<TRight>? rightTypeInfo = null) where TRight : class
        => inner.Join(on, kind, rightTypeInfo);
    public IJoinQuery<T, TRight> Join<TRight>(string leftAlias, string rightAlias, string on, JoinKind kind = JoinKind.Inner, JsonTypeInfo<TRight>? rightTypeInfo = null) where TRight : class
        => inner.Join<TRight>(leftAlias, rightAlias, on, kind, rightTypeInfo);
    public DocumentQueryString ToQueryString() => inner.ToQueryString();
    public Task<long> Count(CancellationToken ct = default) => inner.Count(ct);
    public Task<bool> Any(CancellationToken ct = default) => inner.Any(ct);
    public IAsyncEnumerable<string> RawJsonRows(int? maxRows, CancellationToken ct = default) => inner.RawJsonRows(maxRows, ct);
    public bool SupportsRawJson => inner.SupportsRawJson;
    public Task<CursorPage<JsonObject>> ToJsonCursorPage(string? cursor, int take, CancellationToken ct = default) => inner.ToJsonCursorPage(cursor, take, ct);
    public Task<int> ExecuteDelete(CancellationToken ct = default) => inner.ExecuteDelete(ct);
    public Task<int> ExecuteUpdate(Expression<Func<T, object>> property, object? value, CancellationToken ct = default) => inner.ExecuteUpdate(property, value, ct);
    public Task<int> ExecuteUpdate(Action<IDocumentUpdateBuilder<T>> build, CancellationToken ct = default) => inner.ExecuteUpdate(build, ct);
    public Task<TValue> Max<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default) => inner.Max(selector, ct);
    public Task<TValue> Min<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default) => inner.Min(selector, ct);
    public Task<TValue> Sum<TValue>(Expression<Func<T, TValue>> selector, CancellationToken ct = default) => inner.Sum(selector, ct);
    public Task<double> Average(Expression<Func<T, object>> selector, CancellationToken ct = default) => inner.Average(selector, ct);
    public IAsyncEnumerable<DocumentChange<T>> NotifyOnChange(CancellationToken ct = default) => inner.NotifyOnChange(ct);
}
