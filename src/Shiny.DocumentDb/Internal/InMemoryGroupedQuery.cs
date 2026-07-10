using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The typed grouped builder for the in-memory providers (LiteDB / IndexedDB). Groups a client-side
/// materialized set with a compiled key selector and evaluates the compiled projection against each
/// group (the <see cref="Sql"/> aggregates compute directly on <see cref="MaterializedDocumentGroup{TKey,T}"/>).
/// </summary>
public sealed class InMemoryGroupedQuery<T, TKey> : IGroupedDocumentQuery<T, TKey>
    where T : class
{
    readonly Func<Task<IEnumerable<T>>> materialize;
    readonly Func<T, TKey> keySelector;
    readonly List<Func<IDocumentGroup<TKey, T>, bool>> havings = [];

    /// <param name="materialize">Yields the filtered source documents to group over.</param>
    /// <param name="keySelector">Compiled group-key selector.</param>
    public InMemoryGroupedQuery(Func<Task<IEnumerable<T>>> materialize, Func<T, TKey> keySelector)
    {
        this.materialize = materialize;
        this.keySelector = keySelector;
    }

    /// <summary>Convenience overload for providers whose materialize step is synchronous (e.g. LiteDB).</summary>
    public InMemoryGroupedQuery(Func<IEnumerable<T>> materialize, Func<T, TKey> keySelector)
        : this(() => Task.FromResult(materialize()), keySelector)
    {
    }

    public IGroupedDocumentQuery<T, TKey> Having(Expression<Func<IDocumentGroup<TKey, T>, bool>> predicate)
    {
        this.havings.Add(ExpressionInterpreter.Interpret(predicate));
        return this;
    }

    public IDocumentQuery<TResult> Select<TResult>(
        Expression<Func<IDocumentGroup<TKey, T>, TResult>> selector,
        JsonTypeInfo<TResult>? resultTypeInfo = null) where TResult : class
    {
        var compiled = ExpressionInterpreter.Interpret(selector);
        return new InMemoryGroupedResult<T, TKey, TResult>(this.materialize, this.keySelector, [.. this.havings], compiled);
    }
}

/// <summary>The executable in-memory grouped query: one output row per surviving group.</summary>
internal sealed class InMemoryGroupedResult<T, TKey, TResult> : IDocumentQuery<TResult>
    where T : class
    where TResult : class
{
    readonly Func<Task<IEnumerable<T>>> materialize;
    readonly Func<T, TKey> keySelector;
    readonly IReadOnlyList<Func<IDocumentGroup<TKey, T>, bool>> havings;
    readonly Func<IDocumentGroup<TKey, T>, TResult> project;
    readonly List<(Func<TResult, object> Selector, bool Descending)> orderBys = [];
    int? skip;
    int? take;

    internal InMemoryGroupedResult(
        Func<Task<IEnumerable<T>>> materialize,
        Func<T, TKey> keySelector,
        IReadOnlyList<Func<IDocumentGroup<TKey, T>, bool>> havings,
        Func<IDocumentGroup<TKey, T>, TResult> project)
    {
        this.materialize = materialize;
        this.keySelector = keySelector;
        this.havings = havings;
        this.project = project;
    }

    async Task<IEnumerable<TResult>> Evaluate()
    {
        IEnumerable<IDocumentGroup<TKey, T>> groups = (await this.materialize().ConfigureAwait(false))
            .GroupBy(this.keySelector)
            .Select(g => (IDocumentGroup<TKey, T>)new MaterializedDocumentGroup<TKey, T>(g.Key, g.ToList()));

        foreach (var having in this.havings)
            groups = groups.Where(having);

        var rows = groups.Select(this.project);

        IOrderedEnumerable<TResult>? ordered = null;
        foreach (var (selector, descending) in this.orderBys)
        {
            if (ordered == null)
                ordered = descending ? rows.OrderByDescending(selector) : rows.OrderBy(selector);
            else
                ordered = descending ? ordered.ThenByDescending(selector) : ordered.ThenBy(selector);
        }
        if (ordered != null)
            rows = ordered;

        if (this.skip.HasValue)
            rows = rows.Skip(this.skip.Value);
        if (this.take.HasValue)
            rows = rows.Take(this.take.Value);
        return rows;
    }

    public IDocumentQuery<TResult> OrderBy(Expression<Func<TResult, object>> selector)
    {
        this.orderBys.Add((ExpressionInterpreter.Interpret(selector), false));
        return this;
    }

    public IDocumentQuery<TResult> OrderByDescending(Expression<Func<TResult, object>> selector)
    {
        this.orderBys.Add((ExpressionInterpreter.Interpret(selector), true));
        return this;
    }

    public IDocumentQuery<TResult> Paginate(int offset, int take)
    {
        this.skip = offset;
        this.take = take;
        return this;
    }

    public async Task<IReadOnlyList<TResult>> ToList(CancellationToken ct = default)
        => (await this.Evaluate().ConfigureAwait(false)).ToList();

    public async IAsyncEnumerable<TResult> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var row in await this.Evaluate().ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            yield return row;
        }
    }

    public async Task<long> Count(CancellationToken ct = default) => (await this.Evaluate().ConfigureAwait(false)).Count();
    public async Task<bool> Any(CancellationToken ct = default) => (await this.Evaluate().ConfigureAwait(false)).Any();

    const string AfterGroup = "Cannot modify a grouped query after its projection.";
    public IDocumentQuery<TResult> Where(Expression<Func<TResult, bool>> predicate) => throw new NotSupportedException(AfterGroup);
    public IDocumentQuery<TResult> IgnoreQueryFilters() => throw new NotSupportedException(AfterGroup);
    public IDocumentQuery<TResult> IgnoreQueryFilters(params string[] filterNames) => throw new NotSupportedException(AfterGroup);
    public IGroupedDocumentQuery<TResult, TNewKey> GroupBy<TNewKey>(Expression<Func<TResult, TNewKey>> keySelector) => throw new NotSupportedException(AfterGroup);
    public IDocumentQuery<TNewResult> Select<TNewResult>(Expression<Func<TResult, TNewResult>> selector, JsonTypeInfo<TNewResult>? resultTypeInfo = null) where TNewResult : class => throw new NotSupportedException(AfterGroup);
    public IDocumentQuery<JsonObject> Project(string fields, JsonTypeInfo<TResult>? jsonTypeInfo = null) => throw new NotSupportedException(AfterGroup);
    public Task<int> ExecuteDelete(CancellationToken ct = default) => throw new NotSupportedException(AfterGroup);
    public Task<int> ExecuteUpdate(Expression<Func<TResult, object>> property, object? value, CancellationToken ct = default) => throw new NotSupportedException(AfterGroup);
    public Task<TValue> Max<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default) => throw new NotSupportedException("Aggregate terminals are not supported after a grouped projection.");
    public Task<TValue> Min<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default) => throw new NotSupportedException("Aggregate terminals are not supported after a grouped projection.");
    public Task<TValue> Sum<TValue>(Expression<Func<TResult, TValue>> selector, CancellationToken ct = default) => throw new NotSupportedException("Aggregate terminals are not supported after a grouped projection.");
    public Task<double> Average(Expression<Func<TResult, object>> selector, CancellationToken ct = default) => throw new NotSupportedException("Aggregate terminals are not supported after a grouped projection.");
    public IAsyncEnumerable<DocumentChange<TResult>> NotifyOnChange(CancellationToken ct = default) => throw new NotSupportedException("NotifyOnChange is not supported after a grouped projection.");
}
