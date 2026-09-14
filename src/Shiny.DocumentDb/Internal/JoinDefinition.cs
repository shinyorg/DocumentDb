using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>One joined row: the left document, and the right one — <c>null</c> for a left join's unmatched row.</summary>
sealed class JoinPair<TLeft, TRight>(TLeft left, TRight? right) where TLeft : class where TRight : class
{
    public TLeft Left { get; } = left;
    public TRight? Right { get; } = right;
}

/// <summary>
/// What a join takes from one of its document types: the metadata fields resolve through, the registered query
/// filters, and — for the left side — the <c>Where</c> clauses and <c>IgnoreQueryFilters</c> state of the query the
/// join was started from.
/// </summary>
sealed record JoinSideSource<T>(
    JsonTypeInfo<T> TypeInfo,
    IReadOnlyList<QueryFilter> Filters,
    IReadOnlyList<Expression<Func<T, bool>>> Wheres,
    bool IgnoreAllFilters,
    IReadOnlySet<string>? IgnoredFilterNames,
    IReadOnlyDictionary<string, ComputedMapping>? Computed) where T : class;

/// <summary>
/// The fixed part of a join: its two parameters, kind, condition and sides. Every lambda added later — a
/// <c>Where</c>, an ordering key, a query filter — is rebound onto <see cref="Left"/> and <see cref="Right"/>, so all of
/// them lower against one side map.
/// </summary>
sealed class JoinDefinition<TLeft, TRight> where TLeft : class where TRight : class
{
    JoinDefinition(
        ParameterExpression left,
        ParameterExpression right,
        JoinKind kind,
        JoinSideSource<TLeft> leftSource,
        JoinSideSource<TRight> rightSource,
        JsonSerializerOptions jsonOptions,
        Func<JoinDefinition<TLeft, TRight>, Expression> buildOn)
    {
        this.Left = left;
        this.Right = right;
        this.Kind = kind;
        this.LeftSource = leftSource;
        this.RightSource = rightSource;
        this.JsonOptions = jsonOptions;
        this.Binder = new JoinFieldBinder<TLeft, TRight>(left, leftSource.TypeInfo, right, rightSource.TypeInfo);
        this.On = buildOn(this);
    }

    public ParameterExpression Left { get; }
    public ParameterExpression Right { get; }
    public JoinKind Kind { get; }
    public JoinSideSource<TLeft> LeftSource { get; }
    public JoinSideSource<TRight> RightSource { get; }
    public JsonSerializerOptions JsonOptions { get; }
    public JoinFieldBinder<TLeft, TRight> Binder { get; }

    /// <summary>The join condition, over <see cref="Left"/> and <see cref="Right"/>.</summary>
    public Expression On { get; }

    /// <summary>A LINQ join: the condition's parameter names become the aliases the string overloads qualify with.</summary>
    public static JoinDefinition<TLeft, TRight> FromCondition(
        Expression<Func<TLeft, TRight, bool>> on,
        JoinKind kind,
        JoinSideSource<TLeft> left,
        JoinSideSource<TRight> right,
        JsonSerializerOptions jsonOptions)
        => new(on.Parameters[0], on.Parameters[1], kind, left, right, jsonOptions, _ => on.Body);

    /// <summary>A string join: the caller names both documents and qualifies every field in the condition.</summary>
    public static JoinDefinition<TLeft, TRight> FromString(
        string leftAlias,
        string rightAlias,
        string on,
        JoinKind kind,
        JoinSideSource<TLeft> left,
        JoinSideSource<TRight> right,
        JsonSerializerOptions jsonOptions)
    {
        ValidateAlias(leftAlias, nameof(leftAlias));
        ValidateAlias(rightAlias, nameof(rightAlias));
        if (leftAlias.Equals(rightAlias, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException($"The two sides of a join need different aliases; both are '{leftAlias}'.", nameof(rightAlias));

        return new(
            Expression.Parameter(typeof(TLeft), leftAlias),
            Expression.Parameter(typeof(TRight), rightAlias),
            kind,
            left,
            right,
            jsonOptions,
            d => d.ParseFilter(on, null));
    }

    static void ValidateAlias(string alias, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(alias, parameterName);
        var valid = (char.IsLetter(alias[0]) || alias[0] == '_') && alias.All(c => char.IsLetterOrDigit(c) || c == '_');
        if (!valid)
            throw new ArgumentException($"'{alias}' is not a valid join alias. Use letters, digits and underscores, starting with a letter.", parameterName);
    }

    /// <summary>Rebinds a two-parameter lambda's body onto this join's parameters.</summary>
    public Expression Bind(LambdaExpression lambda)
        => JoinExpressions.Replace(JoinExpressions.Replace(lambda.Body, lambda.Parameters[0], this.Left), lambda.Parameters[1], this.Right);

    public Expression ParseFilter(string filter, IReadOnlyList<object?>? args)
        => FilterExpressionParser.ParseJoin(filter, args, this.Binder);

    public Expression ParseSelector(string expression)
        => FilterExpressionParser.ParseJoinValueSelector(expression, this.Binder);

    /// <summary>Parses a string projection into output keys and value expressions over the two documents.</summary>
    public IReadOnlyList<(string Key, Expression Body)> ParseProjection(string fields)
    {
        var columns = new List<(string Key, Expression Body)>();
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in FilterExpressionParser.ParseJoinProjection(fields, this.Binder))
        {
            var (key, body) = item.ValueExpr is { } value
                ? (item.Alias!, value)
                : (item.Alias ?? this.Binder.LeafJsonName(item.FieldPath!), this.Binder.Resolve(item.FieldPath!).Body);

            if (!keys.Add(key))
                throw new ArgumentException($"Two projected fields resolve to the key '{key}'. Alias one of them with 'as'.", nameof(fields));
            columns.Add((key, body));
        }
        return columns;
    }
}

/// <summary>
/// The string grammar's field binder for a join: every path is qualified with one of the join's aliases, which picks
/// the document it resolves against. The typed grammar is otherwise unchanged — this binder's rules are the typed
/// binder's, minus the functions that need a single mapped document.
/// </summary>
sealed class JoinFieldBinder<TLeft, TRight>(
    ParameterExpression left,
    JsonTypeInfo<TLeft> leftTypeInfo,
    ParameterExpression right,
    JsonTypeInfo<TRight> rightTypeInfo) : IFieldBinder where TLeft : class where TRight : class
{
    public (Expression Body, Type LeafType) Resolve(string path)
    {
        var (isLeft, field) = this.Split(path);
        return isLeft
            ? DocumentQueryExtensions.BuildMemberAccess(left, field, leftTypeInfo)
            : DocumentQueryExtensions.BuildMemberAccess(right, field, rightTypeInfo);
    }

    /// <summary>The JSON name of a qualified path's leaf — the default key of an unaliased projected field.</summary>
    public string LeafJsonName(string path)
    {
        var (isLeft, field) = this.Split(path);
        return isLeft
            ? DocumentQueryExtensions.ResolveJsonPath(field, leftTypeInfo).LeafJsonName
            : DocumentQueryExtensions.ResolveJsonPath(field, rightTypeInfo).LeafJsonName;
    }

    // The qualifier is always stripped before the path is split, so a document that genuinely has a property named
    // like an alias is still addressable (o.o.name), and an unqualified path is an error rather than a silent bind.
    (bool IsLeft, string Field) Split(string path)
    {
        var dot = path.IndexOf('.');
        if (dot <= 0 || dot == path.Length - 1)
            throw new ArgumentException($"'{path}' must be qualified with a join alias: '{left.Name}.{path}' or '{right.Name}.{path}'.");

        var alias = path[..dot];
        if (alias.Equals(left.Name, StringComparison.OrdinalIgnoreCase))
            return (true, path[(dot + 1)..]);
        if (alias.Equals(right.Name, StringComparison.OrdinalIgnoreCase))
            return (false, path[(dot + 1)..]);

        throw new ArgumentException($"Unknown alias '{alias}' in '{path}'. This join's aliases are '{left.Name}' and '{right.Name}'.");
    }

    public bool IsUnresolved(Type leafType) => false;

    public Expression Require(Expression operand, Type leafType, Type required, string func, int position)
    {
        if (leafType != required)
            throw FilterParseError.Create($"'{func}' requires a {(required == typeof(string) ? "string" : required.Name)} argument but got '{leafType.Name}'", position);
        return operand;
    }

    public Expression NumericArg(Expression operand, Type leafType) => Expression.Convert(operand, typeof(double));

    public Expression MemberArg(Expression operand, Type leafType, Type required) => operand;

    public Type LiteralType(Type leafType, Type naturalType) => leafType;

    public Expression AdaptTo(Expression operand, Type type) => operand;

    public Expression GeometryArg(Expression operand, Type leafType, int position)
        => throw FilterParseError.Create("Geo functions are not supported in a join", position);

    public Type EnumArg(Type leafType, string func, int position)
    {
        var underlying = Nullable.GetUnderlyingType(leafType) ?? leafType;
        if (!underlying.IsEnum)
            throw FilterParseError.Create($"'{func}' requires an enum field but got '{underlying.Name}'", position);
        return underlying;
    }

    public void RequireMapping(string func, int position)
        => throw FilterParseError.Create($"'{func}' is not supported in a join", position);
}
