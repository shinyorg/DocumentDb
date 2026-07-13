using System.Collections;
using System.Globalization;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.RavenDb;

/// <summary>
/// Translates a LINQ predicate into an <b>RQL</b> <c>where</c> fragment for
/// <see cref="RavenDbDocumentQuery{T}.ToQueryString"/>. RavenDB stores the document body as opaque JSON
/// (see <see cref="RavenDbDocument.DataJson"/>), so this fragment is a <i>representative</i> RQL projection
/// of the LINQ intent — the query itself evaluates client-side. Untranslatable nodes throw
/// <see cref="NotSupportedException"/> (the same contract as the other document providers' query-string surface).
/// </summary>
static class RavenExpressionVisitor
{
    public static string Translate<T>(Expression<Func<T, bool>> expression, JsonSerializerOptions jsonOptions, JsonTypeInfo<T>? typeInfo) where T : class
        => Visit(expression.Body, jsonOptions, typeInfo);

    static string Visit(Expression expr, JsonSerializerOptions jsonOptions, JsonTypeInfo? typeInfo)
    {
        return expr switch
        {
            BinaryExpression binary => VisitBinary(binary, jsonOptions, typeInfo),
            UnaryExpression { NodeType: ExpressionType.Not } unary => $"not ({Visit(unary.Operand, jsonOptions, typeInfo)})",
            UnaryExpression { NodeType: ExpressionType.Convert } unary => Visit(unary.Operand, jsonOptions, typeInfo),
            MethodCallExpression method => VisitMethodCall(method, jsonOptions, typeInfo),
            MemberExpression member when member.Type == typeof(bool) => $"{ResolveField(member, jsonOptions, typeInfo)} = true",
            ConstantExpression { Value: bool b } => b ? "true" : "false",
            _ => throw new NotSupportedException($"Expression '{expr.NodeType}' ({expr.GetType().Name}) is not supported in a RavenDB query string.")
        };
    }

    static string VisitBinary(BinaryExpression expr, JsonSerializerOptions jsonOptions, JsonTypeInfo? typeInfo)
    {
        if (expr.NodeType == ExpressionType.AndAlso)
            return $"({Visit(expr.Left, jsonOptions, typeInfo)} and {Visit(expr.Right, jsonOptions, typeInfo)})";
        if (expr.NodeType == ExpressionType.OrElse)
            return $"({Visit(expr.Left, jsonOptions, typeInfo)} or {Visit(expr.Right, jsonOptions, typeInfo)})";

        string field;
        object? value;
        bool reversed;
        if (TryResolveField(expr.Left, jsonOptions, typeInfo, out field!))
        {
            value = EvaluateExpression(expr.Right);
            reversed = false;
        }
        else if (TryResolveField(expr.Right, jsonOptions, typeInfo, out field!))
        {
            value = EvaluateExpression(expr.Left);
            reversed = true;
        }
        else
        {
            throw new NotSupportedException($"Cannot resolve a field for binary expression: {expr}");
        }

        var literal = FormatValue(value);
        var op = reversed ? Flip(expr.NodeType) : expr.NodeType;
        return op switch
        {
            ExpressionType.Equal => $"{field} = {literal}",
            ExpressionType.NotEqual => $"{field} != {literal}",
            ExpressionType.GreaterThan => $"{field} > {literal}",
            ExpressionType.GreaterThanOrEqual => $"{field} >= {literal}",
            ExpressionType.LessThan => $"{field} < {literal}",
            ExpressionType.LessThanOrEqual => $"{field} <= {literal}",
            _ => throw new NotSupportedException($"Binary operator '{expr.NodeType}' is not supported in a RavenDB query string.")
        };
    }

    static string VisitMethodCall(MethodCallExpression expr, JsonSerializerOptions jsonOptions, JsonTypeInfo? typeInfo)
    {
        var methodName = expr.Method.Name;

        if (expr.Method.DeclaringType == typeof(string) && expr.Object != null)
        {
            var field = ResolveField(expr.Object, jsonOptions, typeInfo);
            var arg = EvaluateExpression(expr.Arguments[0])?.ToString() ?? string.Empty;
            return methodName switch
            {
                "StartsWith" => $"startsWith({field}, {Quote(arg)})",
                "EndsWith" => $"endsWith({field}, {Quote(arg)})",
                "Contains" => $"search({field}, {Quote("*" + arg + "*")})",
                _ => throw new NotSupportedException($"String method '{methodName}' is not supported in a RavenDB query string.")
            };
        }

        // Enumerable.Contains(source, item) / list.Contains(item) → "field in (...)"
        if (methodName == "Contains")
        {
            Expression? source = null;
            Expression? itemExpr = null;
            if (expr.Object != null && expr.Arguments.Count == 1)
            {
                source = expr.Object;
                itemExpr = expr.Arguments[0];
            }
            else if (expr.Arguments.Count == 2)
            {
                source = expr.Arguments[0];
                itemExpr = expr.Arguments[1];
            }

            if (source != null && itemExpr != null && TryResolveField(itemExpr, jsonOptions, typeInfo, out var field) && EvaluateExpression(source) is IEnumerable enumerable and not string)
            {
                var parts = new List<string>();
                foreach (var item in enumerable)
                    parts.Add(FormatValue(item));
                return $"{field} in ({string.Join(", ", parts)})";
            }
        }

        throw new NotSupportedException($"Method '{expr.Method.DeclaringType?.Name}.{methodName}' is not supported in a RavenDB query string.");
    }

    static bool TryResolveField(Expression expr, JsonSerializerOptions jsonOptions, JsonTypeInfo? typeInfo, out string? field)
    {
        var current = expr;
        while (current is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            current = convert.Operand;

        if (current is not MemberExpression || IsCaptured(current))
        {
            field = null;
            return false;
        }

        field = ResolveField(current, jsonOptions, typeInfo);
        return true;
    }

    static string ResolveField(Expression expr, JsonSerializerOptions jsonOptions, JsonTypeInfo? typeInfo)
    {
        var current = expr;
        while (current is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            current = convert.Operand;

        var parts = new List<string>();
        while (current is MemberExpression member)
        {
            parts.Insert(0, ResolveJsonPropertyName(member, jsonOptions, typeInfo));
            current = member.Expression;
        }

        if (parts.Count == 0)
            throw new NotSupportedException($"Cannot resolve field for expression: {expr}");
        return string.Join(".", parts);
    }

    static string ResolveJsonPropertyName(MemberExpression member, JsonSerializerOptions jsonOptions, JsonTypeInfo? typeInfo)
    {
        var propertyName = member.Member.Name;
        if (typeInfo != null)
        {
            foreach (var prop in typeInfo.Properties)
            {
                if (prop.AttributeProvider is System.Reflection.MemberInfo mi && mi.Name == propertyName)
                    return prop.Name;
            }
        }
        return jsonOptions.PropertyNamingPolicy?.ConvertName(propertyName) ?? propertyName;
    }

    static object? EvaluateExpression(Expression expr)
    {
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            expr = unary.Operand;
        if (expr is ConstantExpression constant)
            return constant.Value;
        return ExpressionInterpreter.EvaluateClosed(expr);
    }

    static bool IsCaptured(Expression expr)
    {
        var current = expr;
        while (current is MemberExpression m)
            current = m.Expression;
        return current is ConstantExpression;
    }

    static string FormatValue(object? value) => value switch
    {
        null => "null",
        bool b => b ? "true" : "false",
        string s => Quote(s),
        Guid g => Quote(g.ToString()),
        DateTime dt => Quote(dt.ToString("o", CultureInfo.InvariantCulture)),
        DateTimeOffset dto => Quote(dto.ToString("o", CultureInfo.InvariantCulture)),
        Enum e => Convert.ToInt64(e).ToString(CultureInfo.InvariantCulture),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => Quote(value.ToString() ?? string.Empty)
    };

    static string Quote(string value)
    {
        var sb = new StringBuilder(value.Length + 2);
        sb.Append('\'');
        foreach (var c in value)
        {
            if (c == '\'')
                sb.Append("''");
            else
                sb.Append(c);
        }
        sb.Append('\'');
        return sb.ToString();
    }

    static ExpressionType Flip(ExpressionType op) => op switch
    {
        ExpressionType.GreaterThan => ExpressionType.LessThan,
        ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
        ExpressionType.LessThan => ExpressionType.GreaterThan,
        ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
        _ => op
    };
}
