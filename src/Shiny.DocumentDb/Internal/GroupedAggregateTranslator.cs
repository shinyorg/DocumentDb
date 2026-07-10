using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Translates an explicit grouped projection — <c>GroupBy(key).Having(…).Select(g =&gt; …)</c> — into the
/// relational <c>SELECT &lt;json_object&gt; … GROUP BY &lt;cols&gt; [HAVING …]</c> the aggregate engine runs.
/// The group key is lowered through the shared value IR (so a JSON property, a derived scalar, or an
/// anonymous multi-column key all work); <c>g.Key</c> in the projection re-emits the same column(s);
/// the <see cref="Sql"/> group aggregates (<c>g.Count()</c>, <c>g.Sum(x =&gt; x.Total)</c>, …) become
/// SQL aggregate terms.
/// </summary>
static class GroupedAggregateTranslator
{
    public sealed record Result(
        string SelectClause,
        string GroupByClause,
        string? HavingClause,
        IReadOnlyDictionary<string, string> OutputColumns,
        Dictionary<string, object?> Parameters);

    public static Result Translate<T, TKey, TResult>(
        Expression<Func<IDocumentGroup<TKey, T>, TResult>> selector,
        Expression<Func<T, TKey>> keySelector,
        IReadOnlyList<Expression<Func<IDocumentGroup<TKey, T>, bool>>> havings,
        JsonTypeInfo<T> sourceTypeInfo,
        JsonTypeInfo<TResult> resultTypeInfo,
        IDatabaseProvider provider,
        IReadOnlyDictionary<string, ComputedMapping>? computed)
    {
        if (selector.Body is not MemberInitExpression memberInit)
            throw new NotSupportedException(
                "A grouped projection selector must be a member initialization expression " +
                "(e.g. g => new Result { Status = g.Key, Count = g.Count() }).");

        var jsonOptions = sourceTypeInfo.Options;
        var parameters = new Dictionary<string, object?>(StringComparer.Ordinal);
        var paramIndex = 0;

        // ── Group key → group-by column(s), keyed for g.Key / g.Key.Member resolution ──
        var keyBody = Strip(keySelector.Body);
        string? singleKeyColumn = null;
        Dictionary<string, string>? multiKeyColumns = null;
        var groupByColumns = new List<string>();

        if (keyBody is NewExpression newKey && newKey.Members != null)
        {
            multiKeyColumns = new Dictionary<string, string>(StringComparer.Ordinal);
            for (var i = 0; i < newKey.Arguments.Count; i++)
            {
                var col = LowerColumn(newKey.Arguments[i]);
                multiKeyColumns[newKey.Members[i].Name] = col;
                groupByColumns.Add(col);
            }
        }
        else
        {
            singleKeyColumn = LowerColumn(keyBody);
            groupByColumns.Add(singleKeyColumn);
        }

        // ── Projection → SELECT json_object(...) ──
        var pairs = new List<string>(memberInit.Bindings.Count * 2);
        var outputColumns = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var binding in memberInit.Bindings)
        {
            if (binding is not MemberAssignment assignment)
                throw new NotSupportedException(
                    $"Only property assignment bindings are supported in grouped projections. " +
                    $"'{binding.Member.Name}' uses '{binding.BindingType}'.");

            var (jsonKey, _) = JsonPropertyNameResolver.ResolveProperty(resultTypeInfo, assignment.Member.Name);
            var valueSql = TranslateTerm(Strip(assignment.Expression));
            pairs.Add($"'{jsonKey}'");
            pairs.Add(valueSql);
            outputColumns[jsonKey] = valueSql;
        }

        // ── HAVING ──
        string? havingClause = null;
        if (havings.Count > 0)
        {
            var parts = new List<string>(havings.Count);
            foreach (var having in havings)
                parts.Add(TranslateHaving(Strip(having.Body)));
            havingClause = parts.Count == 1 ? parts[0] : string.Join(" AND ", parts.Select(p => $"({p})"));
        }

        return new Result(
            provider.JsonObject(pairs),
            string.Join(", ", groupByColumns),
            havingClause,
            outputColumns,
            parameters);

        // ── locals ──

        string LowerColumn(Expression expr)
        {
            var node = ExpressionLowerer.LowerValue(Strip(expr), jsonOptions, sourceTypeInfo, computed);
            var (sql, ps) = SqlPredicateEmitter.EmitValue(node, provider, $"@g{paramIndex++}x");
            Merge(ps);
            return sql;
        }

        string TranslateTerm(Expression expr)
        {
            if (TryTranslateAggregate(expr, out var aggSql))
                return aggSql;
            if (TryResolveKeyColumn(expr, out var keyCol))
                return keyCol;

            throw new NotSupportedException(
                $"'{expr}' is not valid in a grouped projection. Use g.Key (or g.Key.Member for a " +
                "multi-column key) and the Sql group aggregates g.Count()/g.Sum()/g.Min()/g.Max()/g.Avg().");
        }

        bool TryTranslateAggregate(Expression expr, out string sql)
        {
            sql = "";
            if (expr is not MethodCallExpression mc || mc.Method.DeclaringType != typeof(Sql))
                return false;

            switch (mc.Method.Name)
            {
                case "Count":
                    sql = "COUNT(*)";
                    return true;
                case "Sum":
                case "Min":
                case "Max":
                case "Avg":
                    sql = CoalesceZero($"{SqlFunc(mc.Method.Name)}({AggregateArg(mc)})");
                    return true;
                default:
                    return false;
            }
        }

        // The member selector of a group aggregate (g.Sum(x => x.Total)) is the trailing lambda argument.
        string AggregateArg(MethodCallExpression mc)
        {
            var arg = mc.Arguments[^1];
            if (arg is UnaryExpression { NodeType: ExpressionType.Quote } q)
                arg = q.Operand;
            if (arg is not LambdaExpression lambda)
                throw new NotSupportedException($"Sql.{mc.Method.Name}() requires a member selector, e.g. g.{mc.Method.Name}(x => x.Total).");

            var chain = MemberChain(Strip(lambda.Body), lambda.Parameters[0])
                ?? throw new NotSupportedException($"Sql.{mc.Method.Name}() selector must be a simple member access, e.g. x => x.Total.");
            var path = JsonPropertyNameResolver.BuildJsonPath(jsonOptions, sourceTypeInfo, chain);
            return provider.JsonExtractNumeric("Data", path);
        }

        bool TryResolveKeyColumn(Expression expr, out string sql)
        {
            sql = "";
            if (expr is not MemberExpression me)
                return false;

            // g.Key (single-column key)
            if (me.Member.Name == "Key" && IsGroupParameter(me.Expression))
            {
                sql = singleKeyColumn
                    ?? throw new NotSupportedException(
                        "The group key is a multi-column key — project its members (g.Key.Member), not g.Key directly.");
                return true;
            }

            // g.Key.Member (multi-column key)
            if (me.Expression is MemberExpression { Member.Name: "Key" } keyAccess
                && IsGroupParameter(keyAccess.Expression))
            {
                if (multiKeyColumns == null || !multiKeyColumns.TryGetValue(me.Member.Name, out var col))
                    throw new NotSupportedException($"'{me.Member.Name}' is not a member of the group key.");
                sql = col;
                return true;
            }
            return false;
        }

        string TranslateHaving(Expression expr)
        {
            switch (expr)
            {
                case BinaryExpression { NodeType: ExpressionType.AndAlso } and:
                    return $"({TranslateHaving(Strip(and.Left))} AND {TranslateHaving(Strip(and.Right))})";
                case BinaryExpression { NodeType: ExpressionType.OrElse } or:
                    return $"({TranslateHaving(Strip(or.Left))} OR {TranslateHaving(Strip(or.Right))})";
                case UnaryExpression { NodeType: ExpressionType.Not } not:
                    return $"NOT ({TranslateHaving(Strip(not.Operand))})";
                case BinaryExpression cmp:
                    var op = ComparisonOp(cmp.NodeType);
                    return $"{HavingOperand(Strip(cmp.Left))} {op} {HavingOperand(Strip(cmp.Right))}";
                default:
                    throw new NotSupportedException(
                        $"'{expr}' is not a supported HAVING predicate. Compare a group aggregate " +
                        "(g.Sum(x => x.Total)) or the key (g.Key) to a constant.");
            }
        }

        string HavingOperand(Expression expr)
        {
            if (TryTranslateAggregate(expr, out var aggSql))
                return aggSql;
            if (TryResolveKeyColumn(expr, out var keyCol))
                return keyCol;

            // Anything else must be a constant / captured value — lower it as a bound parameter.
            var node = ExpressionLowerer.LowerValue(expr, jsonOptions, sourceTypeInfo, computed);
            var (sql, ps) = SqlPredicateEmitter.EmitValue(node, provider, $"@h{paramIndex++}x");
            Merge(ps);
            return sql;
        }

        void Merge(Dictionary<string, object?> ps)
        {
            foreach (var kv in ps)
                parameters[kv.Key] = kv.Value;
        }
    }

    static Expression Strip(Expression expr)
        => expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u
            ? Strip(u.Operand)
            : expr;

    static bool IsGroupParameter(Expression? expr) => expr is ParameterExpression;

    static List<string>? MemberChain(Expression node, ParameterExpression parameter)
    {
        var chain = new List<string>();
        var current = node;
        while (current is MemberExpression m)
        {
            chain.Insert(0, m.Member.Name);
            current = m.Expression;
        }
        return current == parameter ? chain : null;
    }

    static string SqlFunc(string name) => name == "Avg" ? "AVG" : name.ToUpperInvariant();

    static string CoalesceZero(string sqlExpr) => $"COALESCE({sqlExpr}, 0)";

    static string ComparisonOp(ExpressionType type) => type switch
    {
        ExpressionType.Equal => "=",
        ExpressionType.NotEqual => "<>",
        ExpressionType.GreaterThan => ">",
        ExpressionType.GreaterThanOrEqual => ">=",
        ExpressionType.LessThan => "<",
        ExpressionType.LessThanOrEqual => "<=",
        _ => throw new NotSupportedException($"Operator '{type}' is not supported in HAVING.")
    };
}
