using System.Collections;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

/// <summary>
/// Translates LINQ expression trees into Cosmos SQL WHERE clause fragments.
/// Property access is mapped to c.data.{jsonPropertyName}.
/// </summary>
internal static class CosmosExpressionVisitor
{
    internal static (string sql, Dictionary<string, object?> parameters) Translate<T>(
        Expression<Func<T, bool>> expression,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo<T>? typeInfo) where T : class
    {
        var parameters = new Dictionary<string, object?>();
        var sql = Visit(expression.Body, jsonOptions, typeInfo, parameters, "c.data");
        return (sql, parameters);
    }

    static string Visit(
        Expression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        Dictionary<string, object?> parameters,
        string dataPrefix)
    {
        return expr switch
        {
            BinaryExpression binary => VisitBinary(binary, jsonOptions, typeInfo, parameters, dataPrefix),
            UnaryExpression { NodeType: ExpressionType.Not } unary => VisitNot(unary, jsonOptions, typeInfo, parameters, dataPrefix),
            UnaryExpression { NodeType: ExpressionType.Convert } unary => Visit(unary.Operand, jsonOptions, typeInfo, parameters, dataPrefix),
            MethodCallExpression method => VisitMethodCall(method, jsonOptions, typeInfo, parameters, dataPrefix),
            MemberExpression member when member.Type == typeof(bool) => VisitBoolMember(member, jsonOptions, typeInfo, parameters, dataPrefix),
            ConstantExpression constant when constant.Type == typeof(bool) => (bool)constant.Value! ? "true" : "false",
            _ => throw new NotSupportedException($"Expression type '{expr.NodeType}' ({expr.GetType().Name}) is not supported in CosmosDB queries.")
        };
    }

    static string VisitBinary(
        BinaryExpression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        Dictionary<string, object?> parameters,
        string dataPrefix)
    {
        // Logical operators
        if (expr.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
        {
            var left = Visit(expr.Left, jsonOptions, typeInfo, parameters, dataPrefix);
            var right = Visit(expr.Right, jsonOptions, typeInfo, parameters, dataPrefix);
            var op = expr.NodeType == ExpressionType.AndAlso ? "AND" : "OR";
            return $"({left} {op} {right})";
        }

        // Comparison operators
        var opStr = expr.NodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => throw new NotSupportedException($"Binary operator '{expr.NodeType}' is not supported.")
        };

        // Null comparisons
        if (IsNullConstant(expr.Right))
        {
            var path = ResolvePath(expr.Left, jsonOptions, typeInfo, dataPrefix);
            return expr.NodeType == ExpressionType.Equal
                ? $"(NOT IS_DEFINED({path}) OR IS_NULL({path}))"
                : $"(IS_DEFINED({path}) AND NOT IS_NULL({path}))";
        }
        if (IsNullConstant(expr.Left))
        {
            var path = ResolvePath(expr.Right, jsonOptions, typeInfo, dataPrefix);
            return expr.NodeType == ExpressionType.Equal
                ? $"(NOT IS_DEFINED({path}) OR IS_NULL({path}))"
                : $"(IS_DEFINED({path}) AND NOT IS_NULL({path}))";
        }

        // Enum comparisons — extract the underlying integer value
        var leftPath = ResolvePath(expr.Left, jsonOptions, typeInfo, dataPrefix);
        var rightValue = EvaluateExpression(expr.Right);

        if (rightValue != null && rightValue.GetType().IsEnum)
            rightValue = Convert.ToInt32(rightValue);

        var paramName = $"@p{parameters.Count}";
        parameters[paramName] = rightValue;
        return $"{leftPath} {opStr} {paramName}";
    }

    static string VisitNot(
        UnaryExpression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        Dictionary<string, object?> parameters,
        string dataPrefix)
    {
        var operand = Visit(expr.Operand, jsonOptions, typeInfo, parameters, dataPrefix);
        return $"NOT ({operand})";
    }

    static string VisitBoolMember(
        MemberExpression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        Dictionary<string, object?> parameters,
        string dataPrefix)
    {
        var path = ResolvePath(expr, jsonOptions, typeInfo, dataPrefix);
        return $"{path} = true";
    }

    static string VisitMethodCall(
        MethodCallExpression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        Dictionary<string, object?> parameters,
        string dataPrefix)
    {
        var methodName = expr.Method.Name;
        var declaringType = expr.Method.DeclaringType;

        // String methods
        if (declaringType == typeof(string))
        {
            return methodName switch
            {
                "Contains" => VisitStringFunction("CONTAINS", expr.Object!, expr.Arguments[0], jsonOptions, typeInfo, parameters, dataPrefix),
                "StartsWith" => VisitStringFunction("STARTSWITH", expr.Object!, expr.Arguments[0], jsonOptions, typeInfo, parameters, dataPrefix),
                "EndsWith" => VisitStringFunction("ENDSWITH", expr.Object!, expr.Arguments[0], jsonOptions, typeInfo, parameters, dataPrefix),
                "ToUpper" or "ToUpperInvariant" => $"UPPER({ResolvePath(expr.Object!, jsonOptions, typeInfo, dataPrefix)})",
                "ToLower" or "ToLowerInvariant" => $"LOWER({ResolvePath(expr.Object!, jsonOptions, typeInfo, dataPrefix)})",
                _ => throw new NotSupportedException($"String method '{methodName}' is not supported in CosmosDB queries.")
            };
        }

        // Enumerable.Contains (for "in" queries)
        if (declaringType != null && methodName == "Contains" && IsEnumerableType(declaringType))
        {
            var collection = EvaluateExpression(expr.Arguments[0]);
            if (collection is IEnumerable enumerable)
            {
                var path = ResolvePath(expr.Arguments[1], jsonOptions, typeInfo, dataPrefix);
                var values = new List<string>();
                foreach (var item in enumerable)
                {
                    var pName = $"@p{parameters.Count}";
                    parameters[pName] = item;
                    values.Add(pName);
                }
                return $"{path} IN ({string.Join(", ", values)})";
            }
        }

        // Collection.Any() / .Any(predicate) / .Count()
        if (methodName == "Any" && IsEnumerableMethod(expr))
        {
            var sourcePath = ResolvePath(expr.Arguments[0], jsonOptions, typeInfo, dataPrefix);

            if (expr.Arguments.Count == 1)
            {
                // .Any() — array is non-empty
                return $"ARRAY_LENGTH({sourcePath}) > 0";
            }

            // .Any(predicate) — EXISTS subquery
            var lambda = (LambdaExpression)StripQuotes(expr.Arguments[1]);
            var elementAlias = $"elem{parameters.Count}";
            var innerSql = Visit(lambda.Body, jsonOptions, null, parameters, elementAlias);
            return $"EXISTS(SELECT VALUE 1 FROM {elementAlias} IN {sourcePath} WHERE {innerSql})";
        }

        if (methodName == "Count" && IsEnumerableMethod(expr))
        {
            var sourcePath = ResolvePath(expr.Arguments[0], jsonOptions, typeInfo, dataPrefix);

            if (expr.Arguments.Count == 1)
                return $"ARRAY_LENGTH({sourcePath})";

            // .Count(predicate) — not natively supported, fall back to subquery
            var lambda = (LambdaExpression)StripQuotes(expr.Arguments[1]);
            var elementAlias = $"elem{parameters.Count}";
            var innerSql = Visit(lambda.Body, jsonOptions, null, parameters, elementAlias);
            return $"(SELECT VALUE COUNT(1) FROM {elementAlias} IN {sourcePath} WHERE {innerSql})";
        }

        throw new NotSupportedException($"Method '{declaringType?.Name}.{methodName}' is not supported in CosmosDB queries.");
    }

    static string VisitStringFunction(
        string functionName,
        Expression instance,
        Expression argument,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        Dictionary<string, object?> parameters,
        string dataPrefix)
    {
        var path = ResolvePath(instance, jsonOptions, typeInfo, dataPrefix);
        var value = EvaluateExpression(argument);
        var paramName = $"@p{parameters.Count}";
        parameters[paramName] = value;
        return $"{functionName}({path}, {paramName})";
    }

    static string ResolvePath(
        Expression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        string dataPrefix)
    {
        var parts = new List<string>();
        var current = expr;

        while (current is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            current = convert.Operand;

        while (current is MemberExpression member)
        {
            var name = ResolveJsonPropertyName(member, jsonOptions, typeInfo);
            parts.Insert(0, name);
            current = member.Expression;
        }

        if (parts.Count == 0)
            throw new NotSupportedException($"Cannot resolve path for expression: {expr}");

        return $"{dataPrefix}.{string.Join(".", parts)}";
    }

    static string ResolveJsonPropertyName(MemberExpression member, JsonSerializerOptions jsonOptions, JsonTypeInfo? typeInfo)
    {
        var propertyName = member.Member.Name;

        // Try JsonTypeInfo first for AOT-safe resolution
        if (typeInfo != null)
        {
            foreach (var prop in typeInfo.Properties)
            {
                if (prop.AttributeProvider is System.Reflection.MemberInfo mi && mi.Name == propertyName)
                    return prop.Name;
            }
        }

        // Apply naming policy
        if (jsonOptions.PropertyNamingPolicy != null)
            return jsonOptions.PropertyNamingPolicy.ConvertName(propertyName);

        return propertyName;
    }

    static object? EvaluateExpression(Expression expr)
    {
        // Unwrap Convert
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            expr = unary.Operand;

        if (expr is ConstantExpression constant)
            return constant.Value;

        // Compile and evaluate
        var lambda = Expression.Lambda(expr);
        return lambda.Compile().DynamicInvoke();
    }

    static bool IsNullConstant(Expression expr)
    {
        if (expr is ConstantExpression { Value: null })
            return true;
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            return IsNullConstant(unary.Operand);
        return false;
    }

    static bool IsEnumerableType(Type type)
        => type == typeof(Enumerable) || type == typeof(Queryable) ||
           (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>));

    static bool IsEnumerableMethod(MethodCallExpression expr)
        => expr.Method.DeclaringType == typeof(Enumerable) || expr.Method.DeclaringType == typeof(Queryable);

    static Expression StripQuotes(Expression expr)
        => expr is UnaryExpression { NodeType: ExpressionType.Quote } unary ? unary.Operand : expr;
}
