using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

static class AggregateTranslator
{
    public static (string SelectClause, string? GroupByClause, Dictionary<string, object?> Parameters) Translate<T, TResult>(
        Expression<Func<T, TResult>> selector,
        JsonTypeInfo<T> sourceTypeInfo,
        JsonTypeInfo<TResult> resultTypeInfo,
        IDatabaseProvider provider)
    {
        if (selector.Body is not MemberInitExpression memberInit)
            throw new NotSupportedException(
                "Aggregate selector must be a member initialization expression (e.g. x => new Result { A = x.A, Count = Sql.Count() }).");

        var parameter = selector.Parameters[0];
        var jsonOptions = sourceTypeInfo.Options;
        var pairs = new List<string>(memberInit.Bindings.Count * 2);
        var groupByColumns = new List<string>();
        var parameters = new Dictionary<string, object?>();

        foreach (var binding in memberInit.Bindings)
        {
            if (binding is not MemberAssignment assignment)
                throw new NotSupportedException(
                    $"Only property assignment bindings are supported in aggregates. '{binding.Member.Name}' uses '{binding.BindingType}'.");

            var (resultKey, _) = JsonPropertyNameResolver.ResolveProperty(resultTypeInfo, assignment.Member.Name);
            pairs.Add($"'{resultKey}'");

            var sqlExpr = TranslateAggregateExpression(assignment.Expression, parameter, jsonOptions, sourceTypeInfo, groupByColumns, provider);
            pairs.Add(sqlExpr);
        }

        var selectClause = provider.JsonObject(pairs);
        var groupByClause = groupByColumns.Count > 0 ? string.Join(", ", groupByColumns) : null;

        return (selectClause, groupByClause, parameters);
    }

    static string TranslateAggregateExpression(
        Expression expr,
        ParameterExpression parameter,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo sourceTypeInfo,
        List<string> groupByColumns,
        IDatabaseProvider provider)
    {
        // Check for Sql.* marker method calls
        if (expr is MethodCallExpression methodCall && methodCall.Method.DeclaringType == typeof(Sql))
        {
            return TranslateSqlMarker(methodCall, parameter, jsonOptions, sourceTypeInfo, provider);
        }

        // Unwrap Convert (value-type boxing)
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            return TranslateAggregateExpression(unary.Operand, parameter, jsonOptions, sourceTypeInfo, groupByColumns, provider);

        // Simple member access chain: x.Prop → GROUP BY column
        var sourceChain = BuildMemberChainFromRoot(expr, parameter);
        if (sourceChain != null)
        {
            var sourcePath = JsonPropertyNameResolver.BuildJsonPath(jsonOptions, sourceTypeInfo, sourceChain);
            var jsonExtract = provider.JsonExtract("Data", sourcePath);
            groupByColumns.Add(jsonExtract);
            return jsonExtract;
        }

        throw new NotSupportedException(
            $"Aggregate binding expression '{expr}' must be a simple member access (for GROUP BY) or a Sql.* aggregate call.");
    }

    static string TranslateSqlMarker(
        MethodCallExpression methodCall,
        ParameterExpression parameter,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo sourceTypeInfo,
        IDatabaseProvider provider)
    {
        return methodCall.Method.Name switch
        {
            "Count" => "COUNT(*)",
            "Max" => CoalesceZero(TranslateSqlAggregateArg("MAX", methodCall.Arguments[0], parameter, jsonOptions, sourceTypeInfo, provider)),
            "Min" => CoalesceZero(TranslateSqlAggregateArg("MIN", methodCall.Arguments[0], parameter, jsonOptions, sourceTypeInfo, provider)),
            "Sum" => CoalesceZero(TranslateSqlAggregateArg("SUM", methodCall.Arguments[0], parameter, jsonOptions, sourceTypeInfo, provider)),
            "Avg" => CoalesceZero(TranslateSqlAggregateArg("AVG", methodCall.Arguments[0], parameter, jsonOptions, sourceTypeInfo, provider)),
            _ => throw new NotSupportedException($"Sql.{methodCall.Method.Name}() is not a recognized aggregate function.")
        };
    }

    static string TranslateSqlAggregateArg(
        string sqlFunc,
        Expression argExpr,
        ParameterExpression parameter,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo sourceTypeInfo,
        IDatabaseProvider provider)
    {
        // Unwrap Convert (value-type boxing)
        if (argExpr is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            argExpr = unary.Operand;

        var chain = BuildMemberChainFromRoot(argExpr, parameter);
        if (chain == null)
            throw new NotSupportedException(
                $"Sql.{sqlFunc}() argument must be a simple member access from the source parameter.");

        var path = JsonPropertyNameResolver.BuildJsonPath(jsonOptions, sourceTypeInfo, chain);
        return $"{sqlFunc}({provider.JsonExtract("Data", path)})";
    }

    static string CoalesceZero(string sqlExpr) => $"COALESCE({sqlExpr}, 0)";

    static List<string>? BuildMemberChainFromRoot(Expression node, ParameterExpression parameter)
    {
        var chain = new List<string>();
        var current = node;

        while (current is MemberExpression m)
        {
            chain.Insert(0, m.Member.Name);
            current = m.Expression;
        }

        if (current == parameter)
            return chain;

        return null;
    }

    /// <summary>
    /// Resolves a JSON path from a simple property selector expression (e.g. u => u.Age).
    /// Handles Convert wrapping for value types.
    /// </summary>
    public static string ResolveJsonPathFromSelector<T, TValue>(
        Expression<Func<T, TValue>> selector,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo<T> jsonTypeInfo)
    {
        var body = selector.Body;

        // Unwrap Convert (value-type boxing)
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;

        if (body is not MemberExpression member)
            throw new ArgumentException("Selector must be a property access.", nameof(selector));

        var chain = new List<string>();
        Expression? current = member;
        while (current is MemberExpression m)
        {
            chain.Insert(0, m.Member.Name);
            current = m.Expression;
        }

        if (current is not ParameterExpression)
            throw new ArgumentException("Selector must be a simple property chain on the parameter.", nameof(selector));

        return JsonPropertyNameResolver.BuildJsonPath(jsonOptions, jsonTypeInfo, chain);
    }
}
