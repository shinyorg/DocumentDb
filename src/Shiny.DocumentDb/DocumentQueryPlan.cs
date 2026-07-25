using System.Linq.Expressions;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb;

/// <summary>
/// What a query asks its provider for: the effective predicates (registered query filters that were not
/// ignored, then the caller's <c>Where</c> clauses), the ordering, and the paging window. A provider pushes
/// down as much of this as its engine supports and reports the rest back through
/// <see cref="QueryExecution{T}"/>, which <see cref="DocumentQueryBase{T}"/> then applies client-side.
/// </summary>
public sealed class QueryPlan<T> where T : class
{
    Func<T, bool>? compiled;

    internal QueryPlan(
        IReadOnlyList<Expression<Func<T, bool>>> predicates,
        IReadOnlyList<(LambdaExpression Selector, bool Descending)> orderBy,
        int? skip,
        int? take)
    {
        this.Predicates = predicates;
        this.OrderBy = orderBy;
        this.Skip = skip;
        this.Take = take;
    }

    /// <summary>Query filters (minus any ignored) followed by the caller's <c>Where</c> predicates, AND-combined.</summary>
    public IReadOnlyList<Expression<Func<T, bool>>> Predicates { get; }

    /// <summary>The requested ordering, outermost key first.</summary>
    public IReadOnlyList<(LambdaExpression Selector, bool Descending)> OrderBy { get; }

    /// <summary>Documents to skip, from <c>Paginate</c>.</summary>
    public int? Skip { get; }

    /// <summary>Maximum documents to return, from <c>Paginate</c>.</summary>
    public int? Take { get; }

    public bool HasPredicates => this.Predicates.Count > 0;
    public bool HasOrdering => this.OrderBy.Count > 0;
    public bool HasPaging => this.Skip.HasValue || this.Take.HasValue;

    /// <summary>
    /// The predicates as one compiled delegate (AOT-safe interpretation, not <c>Expression.Compile</c>), cached
    /// for the life of the plan. Returns an always-true delegate when there are none.
    /// </summary>
    public Func<T, bool> CompilePredicate()
    {
        if (this.compiled != null)
            return this.compiled;
        if (this.Predicates.Count == 0)
            return this.compiled = static _ => true;
        var parts = new Func<T, bool>[this.Predicates.Count];
        for (var i = 0; i < this.Predicates.Count; i++)
            parts[i] = ExpressionInterpreter.Interpret(this.Predicates[i]);
        return this.compiled = item =>
        {
            for (var i = 0; i < parts.Length; i++)
                if (!parts[i](item))
                    return false;
            return true;
        };
    }

    /// <summary>The predicates AND-combined into a single expression, or null when there are none — the shape
    /// the store-level full-text and vector searches take as a pre-filter.</summary>
    public Expression<Func<T, bool>>? CombinedPredicate()
    {
        if (this.Predicates.Count == 0)
            return null;
        var combined = this.Predicates[0];
        for (var i = 1; i < this.Predicates.Count; i++)
        {
            var next = this.Predicates[i];
            var param = combined.Parameters[0];
            var rebound = new ParameterRebinder(next.Parameters[0], param).Visit(next.Body)!;
            combined = Expression.Lambda<Func<T, bool>>(Expression.AndAlso(combined.Body, rebound), param);
        }
        return combined;
    }

    sealed class ParameterRebinder(ParameterExpression from, ParameterExpression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node)
            => node == from ? to : base.VisitParameter(node);
    }
}

/// <summary>
/// What a provider produced for a <see cref="QueryPlan{T}"/>, and how much of the plan it already satisfied.
/// Anything it did not satisfy is applied client-side by <see cref="DocumentQueryBase{T}"/>, in the order
/// predicates → ordering → paging. Report honestly: claiming ordering you did not apply silently returns rows
/// in the wrong order, and claiming paging you did not apply drops rows.
/// </summary>
public readonly struct QueryExecution<T> where T : class
{
    QueryExecution(IEnumerable<T> rows, bool predicates, bool ordering, bool paging)
    {
        this.Rows = rows;
        this.PredicatesApplied = predicates;
        this.OrderingApplied = ordering;
        this.PagingApplied = paging;
    }

    public IEnumerable<T> Rows { get; }
    public bool PredicatesApplied { get; }
    public bool OrderingApplied { get; }
    public bool PagingApplied { get; }

    /// <summary>Rows the provider narrowed to but did not filter, order, or page — the base does all of it.
    /// Use this for a store that can only fetch by key/partition (Redis, Azure Table, DynamoDB, LiteDB…).</summary>
    public static QueryExecution<T> Candidates(IEnumerable<T> rows) => new(rows, false, false, false);

    /// <summary>Rows the provider fully filtered but did not order or page.</summary>
    public static QueryExecution<T> Filtered(IEnumerable<T> rows) => new(rows, true, false, false);

    /// <summary>Rows the provider filtered, ordered, and paged entirely server-side (MongoDB, Cosmos DB).</summary>
    public static QueryExecution<T> Complete(IEnumerable<T> rows) => new(rows, true, true, true);

    /// <summary>Full control for a provider that satisfies some of the plan — e.g. a native query that filters
    /// and orders but cannot offset.</summary>
    public static QueryExecution<T> Partial(IEnumerable<T> rows, bool predicatesApplied, bool orderingApplied, bool pagingApplied)
        => new(rows, predicatesApplied, orderingApplied, pagingApplied);
}
