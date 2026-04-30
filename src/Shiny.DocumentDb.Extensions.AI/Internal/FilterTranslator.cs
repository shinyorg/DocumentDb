using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

/// <summary>
/// Translates the structured-filter JSON the LLM produces into an
/// <see cref="Expression{TDelegate}"/> of <c>Func&lt;T,bool&gt;</c> that the existing
/// <see cref="IDocumentQuery{T}.Where"/> pipeline can compile to SQL.
/// </summary>
/// <remarks>
/// Builds expression trees programmatically — never calls <c>Compile()</c> — so the
/// existing JSON-extract translator handles AOT-safe SQL generation.
/// </remarks>
static class FilterTranslator
{
    static readonly MethodInfo StringContains = typeof(string).GetMethod("Contains", new[] { typeof(string) })!;
    static readonly MethodInfo StringStartsWith = typeof(string).GetMethod("StartsWith", new[] { typeof(string) })!;

    public static Expression<Func<T, bool>>? Translate<T>(
        JsonElement? filter,
        IReadOnlyList<DocumentField> allowedFields) where T : class
    {
        if (filter is null || filter.Value.ValueKind == JsonValueKind.Null || filter.Value.ValueKind == JsonValueKind.Undefined)
            return null;

        var parameter = Expression.Parameter(typeof(T), "x");
        var fieldMap = BuildFieldMap(allowedFields);
        var body = Build(filter.Value, parameter, fieldMap);
        return Expression.Lambda<Func<T, bool>>(body, parameter);
    }

    static Dictionary<string, PropertyInfo> BuildFieldMap(IReadOnlyList<DocumentField> fields)
    {
        var map = new Dictionary<string, PropertyInfo>(fields.Count, StringComparer.Ordinal);
        foreach (var f in fields)
        {
            if (f.ClrProperty != null)
                map[f.JsonName] = f.ClrProperty;
        }
        return map;
    }

    static Expression Build(JsonElement node, ParameterExpression parameter, Dictionary<string, PropertyInfo> fields)
    {
        if (node.ValueKind != JsonValueKind.Object)
            throw new InvalidOperationException("Filter node must be a JSON object.");

        // Combinators take precedence and are mutually exclusive with leaf comparisons.
        if (node.TryGetProperty("and", out var andNode))
            return Combine(andNode, parameter, fields, isAnd: true);
        if (node.TryGetProperty("or", out var orNode))
            return Combine(orNode, parameter, fields, isAnd: false);
        if (node.TryGetProperty("not", out var notNode))
            return Expression.Not(Build(notNode, parameter, fields));

        // Leaf comparison
        if (!node.TryGetProperty("field", out var fieldNode))
            throw new InvalidOperationException("Leaf filter requires a 'field' property.");
        if (!node.TryGetProperty("op", out var opNode))
            throw new InvalidOperationException("Leaf filter requires an 'op' property.");

        var fieldName = fieldNode.GetString() ?? throw new InvalidOperationException("'field' must be a string.");
        var op = opNode.GetString() ?? throw new InvalidOperationException("'op' must be a string.");

        if (!fields.TryGetValue(fieldName, out var prop))
            throw new InvalidOperationException(
                $"Field '{fieldName}' is not allowed by the tool configuration.");

        node.TryGetProperty("value", out var valueNode);
        return BuildLeaf(parameter, prop, op, valueNode);
    }

    static Expression Combine(JsonElement array, ParameterExpression parameter, Dictionary<string, PropertyInfo> fields, bool isAnd)
    {
        if (array.ValueKind != JsonValueKind.Array || array.GetArrayLength() == 0)
            throw new InvalidOperationException($"'{(isAnd ? "and" : "or")}' must be a non-empty array.");

        Expression? acc = null;
        foreach (var item in array.EnumerateArray())
        {
            var expr = Build(item, parameter, fields);
            acc = acc is null ? expr : (isAnd ? Expression.AndAlso(acc, expr) : Expression.OrElse(acc, expr));
        }
        return acc!;
    }

    static Expression BuildLeaf(ParameterExpression parameter, PropertyInfo prop, string op, JsonElement value)
    {
        var member = Expression.Property(parameter, prop);

        if (op == "in")
        {
            if (value.ValueKind != JsonValueKind.Array)
                throw new InvalidOperationException("'in' requires an array value.");

            Expression? acc = null;
            foreach (var item in value.EnumerateArray())
            {
                var literal = ConvertValue(item, prop.PropertyType);
                var eq = Expression.Equal(member, Expression.Constant(literal, prop.PropertyType));
                acc = acc is null ? eq : Expression.OrElse(acc, eq);
            }
            return acc ?? Expression.Constant(false);
        }

        if (op is "contains" or "startsWith")
        {
            if (prop.PropertyType != typeof(string))
                throw new InvalidOperationException($"Operator '{op}' requires a string field.");
            var literal = value.GetString() ?? throw new InvalidOperationException($"'{op}' requires a string value.");
            var method = op == "contains" ? StringContains : StringStartsWith;
            return Expression.Call(member, method, Expression.Constant(literal, typeof(string)));
        }

        var converted = ConvertValue(value, prop.PropertyType);
        var constant = Expression.Constant(converted, prop.PropertyType);

        return op switch
        {
            "eq"  => Expression.Equal(member, constant),
            "ne"  => Expression.NotEqual(member, constant),
            "gt"  => Expression.GreaterThan(member, constant),
            "gte" => Expression.GreaterThanOrEqual(member, constant),
            "lt"  => Expression.LessThan(member, constant),
            "lte" => Expression.LessThanOrEqual(member, constant),
            _     => throw new InvalidOperationException($"Unknown operator '{op}'.")
        };
    }

    static object? ConvertValue(JsonElement value, Type targetType)
    {
        if (value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return null;

        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (underlying == typeof(string))    return value.ValueKind == JsonValueKind.String ? value.GetString() : value.ToString();
        if (underlying == typeof(bool))      return value.GetBoolean();
        if (underlying == typeof(Guid))      return value.ValueKind == JsonValueKind.String ? Guid.Parse(value.GetString()!) : throw new InvalidOperationException("Guid value must be a string.");
        if (underlying == typeof(DateTime))  return value.GetDateTime();
        if (underlying == typeof(DateTimeOffset)) return value.GetDateTimeOffset();
        if (underlying == typeof(int))       return value.GetInt32();
        if (underlying == typeof(long))      return value.GetInt64();
        if (underlying == typeof(short))     return value.GetInt16();
        if (underlying == typeof(byte))      return value.GetByte();
        if (underlying == typeof(uint))      return value.GetUInt32();
        if (underlying == typeof(ulong))     return value.GetUInt64();
        if (underlying == typeof(double))    return value.GetDouble();
        if (underlying == typeof(float))     return (float)value.GetDouble();
        if (underlying == typeof(decimal))   return value.GetDecimal();

        // Fallback: stringify and let the SQL provider coerce. Predictable for opaque types.
        return value.ToString();
    }
}
