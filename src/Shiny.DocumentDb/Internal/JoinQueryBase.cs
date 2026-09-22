using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>Which of a join's two documents an expression reads.</summary>
[Flags]
enum JoinSideUse
{
    None = 0,
    Left = 1,
    Right = 2,
    Both = Left | Right
}

/// <summary>
/// The provider-agnostic half of a join: builder state and immutability, the string grammar, preparing the
/// predicates both engines run, and shaping materialized pairs. A provider supplies how to fetch, stream, count and
/// render the joined pairs.
/// </summary>
abstract class JoinQueryBase<TLeft, TRight> : IJoinQuery<TLeft, TRight> where TLeft : class where TRight : class
{
    // Member tokens taken from lambdas rather than looked up by name, so the trimmer sees them.
    static readonly MemberInfo LeftMember = ((MemberExpression)((Expression<Func<JoinPair<TLeft, TRight>, TLeft>>)(p => p.Left)).Body).Member;
    static readonly MemberInfo RightMember = ((MemberExpression)((Expression<Func<JoinPair<TLeft, TRight>, TRight?>>)(p => p.Right)).Body).Member;

    readonly List<Expression> wheres = [];
    readonly List<(Expression Body, bool Descending)> orderBys = [];
    int? skip;
    int? take;
    bool ignoreAllFilters;
    HashSet<string>? ignoredFilterNames;

    protected JoinQueryBase(JoinDefinition<TLeft, TRight> definition)
        => this.Definition = definition;

    protected JoinQueryBase(JoinQueryBase<TLeft, TRight> source)
    {
        this.Definition = source.Definition;
        this.wheres.AddRange(source.wheres);
        this.orderBys.AddRange(source.orderBys);
        this.skip = source.skip;
        this.take = source.take;
        this.ignoreAllFilters = source.ignoreAllFilters;
        this.ignoredFilterNames = source.ignoredFilterNames is null
            ? null
            : new HashSet<string>(source.ignoredFilterNames, StringComparer.Ordinal);
    }

    protected JoinDefinition<TLeft, TRight> Definition { get; }

    /// <summary>Returns a copy of this query, chaining to the copy constructor.</summary>
    protected abstract JoinQueryBase<TLeft, TRight> Clone();

    /// <summary>Runs the join. <paramref name="maxRows"/> narrows the row limit for the single-row terminals.</summary>
    internal abstract Task<IReadOnlyList<JoinPair<TLeft, TRight>>> FetchAsync(int? maxRows, CancellationToken ct);

    internal abstract IAsyncEnumerable<JoinPair<TLeft, TRight>> StreamAsync(CancellationToken ct);

    internal abstract Task<long> CountAsync(CancellationToken ct);

    internal abstract Task<bool> AnyAsync(CancellationToken ct);

    internal abstract DocumentQueryString RenderQuery();

    // ── Builder ─────────────────────────────────────────────────────────

    public IJoinQuery<TLeft, TRight> Where(Expression<Func<TLeft, TRight, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        var clone = this.Clone();
        clone.wheres.Add(this.Definition.Bind(SpanContainsRewriter.Rewrite(predicate)));
        return clone;
    }

    public IJoinQuery<TLeft, TRight> Where(string filter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filter);
        var clone = this.Clone();
        clone.wheres.Add(this.Definition.ParseFilter(filter, null));
        return clone;
    }

    public IJoinQuery<TLeft, TRight> Where(FilterInterpolatedStringHandler filter)
    {
        var clone = this.Clone();
        clone.wheres.Add(this.Definition.ParseFilter(filter.Filter, filter.Arguments));
        return clone;
    }

    public IJoinQuery<TLeft, TRight> OrderBy(Expression<Func<TLeft, TRight, object>> selector)
        => this.AddOrder(this.Definition.Bind(Required(selector)), false);

    public IJoinQuery<TLeft, TRight> OrderBy(string field)
        => this.AddOrder(this.Definition.ParseSelector(RequiredText(field)), false);

    public IJoinQuery<TLeft, TRight> OrderByDescending(Expression<Func<TLeft, TRight, object>> selector)
        => this.AddOrder(this.Definition.Bind(Required(selector)), true);

    public IJoinQuery<TLeft, TRight> OrderByDescending(string field)
        => this.AddOrder(this.Definition.ParseSelector(RequiredText(field)), true);

    public IJoinQuery<TLeft, TRight> Paginate(int offset, int take)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(take);
        var clone = this.Clone();
        clone.skip = offset;
        clone.take = take;
        return clone;
    }

    public IJoinQuery<TLeft, TRight> IgnoreQueryFilters()
    {
        var clone = this.Clone();
        clone.ignoreAllFilters = true;
        return clone;
    }

    public IJoinQuery<TLeft, TRight> IgnoreQueryFilters(params string[] filterNames)
    {
        ArgumentNullException.ThrowIfNull(filterNames);
        var clone = this.Clone();
        clone.ignoredFilterNames ??= new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in filterNames)
            clone.ignoredFilterNames.Add(name);
        return clone;
    }

    public IJoinResult<TResult> Select<TResult>(Expression<Func<TLeft, TRight, TResult>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        return new JoinResult<TLeft, TRight, TResult>(this, this.Interpret<TResult>(this.Definition.Bind(selector)));
    }

    public IJoinResult<JsonObject> Project(string fields)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fields);
        var options = this.Definition.JsonOptions;
        var columns = this.Definition.ParseProjection(fields)
            .Select(c => (
                c.Key,
                ReadsRight: (this.Uses(c.Body) & JoinSideUse.Right) != 0,
                Read: this.Interpret<object?>(Expression.Convert(c.Body, typeof(object)))))
            .ToList();

        // A field of a left join's missing document projects as null, the way the column would come back from SQL.
        return new JoinResult<TLeft, TRight, JsonObject>(this, pair =>
        {
            var row = new JsonObject();
            foreach (var (key, readsRight, read) in columns)
                row[key] = readsRight && pair.Right is null ? null : ToJsonNode(read(pair), options);
            return row;
        });
    }

    JoinQueryBase<TLeft, TRight> AddOrder(Expression body, bool descending)
    {
        var clone = this.Clone();
        clone.orderBys.Add((body, descending));
        return clone;
    }

    static T Required<T>(T value) where T : class
    {
        ArgumentNullException.ThrowIfNull(value);
        return value;
    }

    static string RequiredText(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        return value;
    }

    // ── Preparation ─────────────────────────────────────────────────────

    /// <summary>
    /// The predicates a provider runs, every one over <see cref="JoinDefinition{TLeft, TRight}.Left"/> and
    /// <see cref="JoinDefinition{TLeft, TRight}.Right"/>. Single-side predicates have had the cross-cutting rewriters
    /// applied (field encryption turns a constant into its ciphertext) exactly once; a predicate that compares the two
    /// sides may not touch an encrypted property at all.
    /// </summary>
    /// <param name="LeftScope">The left type's query filters and the source query's <c>Where</c> clauses.</param>
    /// <param name="RightScope">The right type's query filters — part of the join condition, so a left join keeps its rows.</param>
    /// <param name="On">The join condition, split into its AND-ed conditions.</param>
    /// <param name="Where">The join's <c>Where</c> clauses, split into their AND-ed conditions.</param>
    protected sealed record PreparedJoin(
        IReadOnlyList<Expression> LeftScope,
        IReadOnlyList<Expression> RightScope,
        IReadOnlyList<Expression> On,
        IReadOnlyList<Expression> Where,
        IReadOnlyList<(Expression Body, bool Descending)> OrderBy);

    protected PreparedJoin Prepare()
    {
        var d = this.Definition;

        var leftScope = new List<Expression>();
        leftScope.AddRange(Scope<TLeft>(
            d.LeftSource.Filters, d.Left,
            d.LeftSource.IgnoreAllFilters || this.ignoreAllFilters,
            d.LeftSource.IgnoredFilterNames, this.ignoredFilterNames));
        foreach (var where in d.LeftSource.Wheres)
        {
            var rewritten = DocumentPredicateRewriters.Apply(where);
            leftScope.Add(JoinExpressions.Replace(rewritten.Body, rewritten.Parameters[0], d.Left));
        }

        var rightScope = Scope<TRight>(d.RightSource.Filters, d.Right, this.ignoreAllFilters, null, this.ignoredFilterNames).ToList();

        var on = new List<Expression>();
        foreach (var condition in JoinExpressions.Conjuncts(d.On))
            on.Add(this.PrepareCondition(condition, "a join condition"));

        var joinWhere = new List<Expression>();
        foreach (var where in this.wheres)
        {
            foreach (var condition in JoinExpressions.Conjuncts(where))
                joinWhere.Add(this.PrepareCondition(condition, "a filter that compares the two sides of a join"));
        }

        return new PreparedJoin(leftScope, rightScope, on, joinWhere, [.. this.orderBys]);
    }

    /// <summary>Which documents <paramref name="expression"/> reads.</summary>
    protected JoinSideUse Uses(Expression expression)
        => JoinExpressions.Uses(expression, this.Definition.Left, this.Definition.Right);

    /// <summary>The paging window, with a single-row terminal's limit applied (an existing smaller window wins).</summary>
    protected (int? Skip, int? Take) Window(int? maxRows)
    {
        var effective = this.take;
        if (maxRows is { } max)
            effective = effective is { } window && window < max ? window : max;
        return effective is null ? (null, null) : (this.skip ?? 0, effective);
    }

    static IEnumerable<Expression> Scope<T>(
        IReadOnlyList<QueryFilter> filters,
        ParameterExpression target,
        bool ignoreAll,
        IReadOnlySet<string>? ignoredNames,
        IReadOnlySet<string>? joinIgnoredNames) where T : class
    {
        if (ignoreAll)
            yield break;

        foreach (var filter in filters)
        {
            var ignored = filter.Name != null
                          && (ignoredNames?.Contains(filter.Name) == true || joinIgnoredNames?.Contains(filter.Name) == true);
            if (!ignored)
            {
                var rewritten = DocumentPredicateRewriters.Apply((Expression<Func<T, bool>>)filter.Predicate);
                yield return JoinExpressions.Replace(rewritten.Body, rewritten.Parameters[0], target);
            }
        }
    }

    Expression PrepareCondition(Expression condition, string context)
    {
        var d = this.Definition;
        switch (this.Uses(condition))
        {
            case JoinSideUse.Left:
                return DocumentPredicateRewriters.Apply(Expression.Lambda<Func<TLeft, bool>>(condition, d.Left)).Body;

            case JoinSideUse.Right:
                return DocumentPredicateRewriters.Apply(Expression.Lambda<Func<TRight, bool>>(condition, d.Right)).Body;

            case JoinSideUse.Both:
                // A comparison between the two documents cannot be answered against ciphertext, and the constant rewrite
                // only knows how to replace a literal — so an encrypted property here is refused outright.
                if (JoinExpressions.FindEncryptedMember(condition, d.Left, d.Right) is { } encrypted)
                    throw new NotSupportedException(
                        $"'{encrypted}' is encrypted at rest, so it cannot be used in {context}. " +
                        "Join on an unencrypted property, or compare the decrypted values in memory after the join.");
                return condition;

            default:
                return condition;
        }
    }

    // ── Shaping ─────────────────────────────────────────────────────────

    Func<JoinPair<TLeft, TRight>, TResult> Interpret<TResult>(Expression body)
    {
        var d = this.Definition;
        var pair = Expression.Parameter(typeof(JoinPair<TLeft, TRight>), "pair");
        var rebound = JoinExpressions.Replace(
            JoinExpressions.Replace(body, d.Left, Expression.MakeMemberAccess(pair, LeftMember)),
            d.Right,
            Expression.MakeMemberAccess(pair, RightMember));

        var evaluate = ExpressionInterpreter.Interpret(Expression.Lambda<Func<JoinPair<TLeft, TRight>, TResult>>(rebound, pair));
        return row =>
        {
            try
            {
                return evaluate(row);
            }
            catch (TargetException) when (row.Right is null)
            {
                throw new InvalidOperationException(
                    $"A left join found no '{typeof(TRight).Name}' for this row, so the right document is null. " +
                    "Guard it in the projection, e.g. c == null ? null : c.Name.");
            }
        };
    }

    static JsonNode? ToJsonNode(object? value, JsonSerializerOptions options) => value switch
    {
        null => null,
        string s => JsonValue.Create(s),
        bool b => JsonValue.Create(b),
        int i => JsonValue.Create(i),
        long l => JsonValue.Create(l),
        short sh => JsonValue.Create(sh),
        byte by => JsonValue.Create(by),
        double dbl => JsonValue.Create(dbl),
        float f => JsonValue.Create(f),
        decimal m => JsonValue.Create(m),
        Guid g => JsonValue.Create(g),
        DateTime dt => JsonValue.Create(dt),
        DateTimeOffset dto => JsonValue.Create(dto),
        char c => JsonValue.Create(c),
        _ => JsonSerializer.SerializeToNode(value, options.GetTypeInfo(value.GetType()))
    };
}

/// <summary>The terminals of a projected join: fetch the pairs through the query, shape each one.</summary>
sealed class JoinResult<TLeft, TRight, TResult>(
    JoinQueryBase<TLeft, TRight> query,
    Func<JoinPair<TLeft, TRight>, TResult> shape) : IJoinResult<TResult> where TLeft : class where TRight : class
{
    public async Task<IReadOnlyList<TResult>> ToList(CancellationToken ct = default)
    {
        var pairs = await query.FetchAsync(null, ct).ConfigureAwait(false);
        var rows = new List<TResult>(pairs.Count);
        foreach (var pair in pairs)
            rows.Add(shape(pair));
        return rows;
    }

    public async IAsyncEnumerable<TResult> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var pair in query.StreamAsync(ct).WithCancellation(ct).ConfigureAwait(false))
            yield return shape(pair);
    }

    public Task<long> Count(CancellationToken ct = default) => query.CountAsync(ct);

    public Task<bool> Any(CancellationToken ct = default) => query.AnyAsync(ct);

    public async Task<TResult?> FirstOrDefault(CancellationToken ct = default)
    {
        var pairs = await query.FetchAsync(1, ct).ConfigureAwait(false);
        return pairs.Count == 0 ? default : shape(pairs[0]);
    }

    public async Task<TResult> First(CancellationToken ct = default)
    {
        var pairs = await query.FetchAsync(1, ct).ConfigureAwait(false);
        return pairs.Count == 0
            ? throw new InvalidOperationException($"No '{typeof(TLeft).Name}' joined to a '{typeof(TRight).Name}' matched the query.")
            : shape(pairs[0]);
    }

    public DocumentQueryString ToQueryString() => query.RenderQuery();
}

/// <summary>Expression plumbing shared by both join engines.</summary>
static class JoinExpressions
{
    public static Expression Replace(Expression body, ParameterExpression from, Expression to)
        => new Substitution(from, to).Visit(body);

    /// <summary>Splits an expression into its top-level AND-ed conditions.</summary>
    public static IEnumerable<Expression> Conjuncts(Expression expression)
    {
        if (expression is BinaryExpression { NodeType: ExpressionType.AndAlso } and)
        {
            foreach (var left in Conjuncts(and.Left))
                yield return left;
            foreach (var right in Conjuncts(and.Right))
                yield return right;
        }
        else
        {
            yield return expression;
        }
    }

    public static Expression Combine(IEnumerable<Expression> conditions)
        => conditions.Aggregate(Expression.AndAlso);

    public static JoinSideUse Uses(Expression expression, ParameterExpression left, ParameterExpression right)
    {
        var visitor = new SideUseVisitor(left, right);
        visitor.Visit(expression);
        return visitor.Use;
    }

    /// <summary>The first encrypted property of either document referenced in <paramref name="expression"/>, as Type.Property.</summary>
    public static string? FindEncryptedMember(Expression expression, ParameterExpression left, ParameterExpression right)
    {
        var finder = new EncryptedMemberFinder(left, right);
        finder.Visit(expression);
        return finder.Found;
    }

    sealed class Substitution(ParameterExpression from, Expression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) => node == from ? to : node;
    }

    sealed class SideUseVisitor(ParameterExpression left, ParameterExpression right) : ExpressionVisitor
    {
        public JoinSideUse Use { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == left)
                this.Use |= JoinSideUse.Left;
            else if (node == right)
                this.Use |= JoinSideUse.Right;
            return node;
        }
    }

    sealed class EncryptedMemberFinder(ParameterExpression left, ParameterExpression right) : ExpressionVisitor
    {
        public string? Found { get; private set; }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (this.Found == null
                && node is { Member: PropertyInfo property, Expression: ParameterExpression parameter }
                && (parameter == left || parameter == right)
                && EncryptionRegistry.TryGet(parameter.Type, property.Name, out _))
            {
                this.Found = $"{parameter.Type.Name}.{property.Name}";
            }
            return base.VisitMember(node);
        }
    }
}
