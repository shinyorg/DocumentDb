using System.Collections;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Shiny.DocumentDb.MongoDb;

/// <summary>
/// Translates LINQ expression trees into MongoDB FilterDefinition&lt;BsonDocument&gt; trees.
/// Property access is mapped to fields under the "data." prefix so it lines up with the envelope schema.
/// </summary>
internal static class MongoExpressionVisitor
{
    internal static FilterDefinition<BsonDocument> Translate<T>(
        Expression<Func<T, bool>> expression,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo<T>? typeInfo) where T : class
        => Visit(expression.Body, jsonOptions, typeInfo, MongoFields.Data);

    static FilterDefinition<BsonDocument> Visit(
        Expression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        string fieldPrefix)
    {
        return expr switch
        {
            BinaryExpression binary => VisitBinary(binary, jsonOptions, typeInfo, fieldPrefix),
            UnaryExpression { NodeType: ExpressionType.Not } unary
                => Builders<BsonDocument>.Filter.Not(Visit(unary.Operand, jsonOptions, typeInfo, fieldPrefix)),
            UnaryExpression { NodeType: ExpressionType.Convert } unary
                => Visit(unary.Operand, jsonOptions, typeInfo, fieldPrefix),
            MethodCallExpression method => VisitMethodCall(method, jsonOptions, typeInfo, fieldPrefix),
            MemberExpression member when member.Type == typeof(bool)
                => Builders<BsonDocument>.Filter.Eq(ResolveField(member, jsonOptions, typeInfo, fieldPrefix), true),
            ConstantExpression { Value: bool b }
                => b ? Builders<BsonDocument>.Filter.Empty : Builders<BsonDocument>.Filter.Where(_ => false),
            _ => throw new NotSupportedException($"Expression type '{expr.NodeType}' ({expr.GetType().Name}) is not supported in MongoDB queries.")
        };
    }

    static FilterDefinition<BsonDocument> VisitBinary(
        BinaryExpression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        string fieldPrefix)
    {
        var fb = Builders<BsonDocument>.Filter;

        if (expr.NodeType == ExpressionType.AndAlso)
            return fb.And(
                Visit(expr.Left, jsonOptions, typeInfo, fieldPrefix),
                Visit(expr.Right, jsonOptions, typeInfo, fieldPrefix));

        if (expr.NodeType == ExpressionType.OrElse)
            return fb.Or(
                Visit(expr.Left, jsonOptions, typeInfo, fieldPrefix),
                Visit(expr.Right, jsonOptions, typeInfo, fieldPrefix));

        // Comparisons. Field is always the side that resolves to a member access.
        string field;
        object? value;
        bool reversed;
        if (TryResolveField(expr.Left, jsonOptions, typeInfo, fieldPrefix, out field!))
        {
            value = EvaluateExpression(expr.Right);
            reversed = false;
        }
        else if (TryResolveField(expr.Right, jsonOptions, typeInfo, fieldPrefix, out field!))
        {
            value = EvaluateExpression(expr.Left);
            reversed = true;
        }
        else
        {
            throw new NotSupportedException($"Cannot resolve field for binary expression: {expr}");
        }

        if (value != null && value.GetType().IsEnum)
            value = Convert.ToInt32(value);

        var bsonValue = ToBsonValue(value);

        return expr.NodeType switch
        {
            ExpressionType.Equal => fb.Eq(field, bsonValue),
            ExpressionType.NotEqual => fb.Ne(field, bsonValue),
            ExpressionType.GreaterThan => reversed ? fb.Lt(field, bsonValue) : fb.Gt(field, bsonValue),
            ExpressionType.GreaterThanOrEqual => reversed ? fb.Lte(field, bsonValue) : fb.Gte(field, bsonValue),
            ExpressionType.LessThan => reversed ? fb.Gt(field, bsonValue) : fb.Lt(field, bsonValue),
            ExpressionType.LessThanOrEqual => reversed ? fb.Gte(field, bsonValue) : fb.Lte(field, bsonValue),
            _ => throw new NotSupportedException($"Binary operator '{expr.NodeType}' is not supported.")
        };
    }

    static FilterDefinition<BsonDocument> VisitMethodCall(
        MethodCallExpression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        string fieldPrefix)
    {
        var fb = Builders<BsonDocument>.Filter;
        var methodName = expr.Method.Name;
        var declaringType = expr.Method.DeclaringType;

        if (declaringType == typeof(string))
        {
            var field = ResolveField(expr.Object!, jsonOptions, typeInfo, fieldPrefix);
            var argValue = EvaluateExpression(expr.Arguments[0])?.ToString() ?? string.Empty;
            var escaped = System.Text.RegularExpressions.Regex.Escape(argValue);
            return methodName switch
            {
                "Contains" => fb.Regex(field, new BsonRegularExpression(escaped)),
                "StartsWith" => fb.Regex(field, new BsonRegularExpression("^" + escaped)),
                "EndsWith" => fb.Regex(field, new BsonRegularExpression(escaped + "$")),
                _ => throw new NotSupportedException($"String method '{methodName}' is not supported in MongoDB queries.")
            };
        }

        // Enumerable.Contains(source, item) for "in" queries
        if (declaringType != null && methodName == "Contains" && IsEnumerableMethod(expr))
        {
            var collection = EvaluateExpression(expr.Arguments[0]);
            if (collection is IEnumerable enumerable)
            {
                var field = ResolveField(expr.Arguments[1], jsonOptions, typeInfo, fieldPrefix);
                var values = new BsonArray();
                foreach (var item in enumerable)
                    values.Add(ToBsonValue(item));
                return fb.In(field, values.Cast<BsonValue>());
            }
        }

        // List<T>.Contains(item) for "in" queries on the in-memory list
        if (declaringType != null && methodName == "Contains" && expr.Object != null)
        {
            var collection = EvaluateExpression(expr.Object);
            if (collection is IEnumerable enumerable)
            {
                var field = ResolveField(expr.Arguments[0], jsonOptions, typeInfo, fieldPrefix);
                var values = new BsonArray();
                foreach (var item in enumerable)
                    values.Add(ToBsonValue(item));
                return fb.In(field, values.Cast<BsonValue>());
            }
        }

        // Collection.Any() — at least one element
        if (methodName == "Any" && IsEnumerableMethod(expr))
        {
            var field = ResolveField(expr.Arguments[0], jsonOptions, typeInfo, fieldPrefix);

            if (expr.Arguments.Count == 1)
                return fb.SizeGt(field, 0);

            // .Any(predicate): build $elemMatch
            var lambda = (LambdaExpression)StripQuotes(expr.Arguments[1]);
            var inner = Visit(lambda.Body, jsonOptions, null, string.Empty);
            return fb.ElemMatch<BsonDocument>(field, inner);
        }

        if (methodName == "Count" && IsEnumerableMethod(expr))
        {
            var field = ResolveField(expr.Arguments[0], jsonOptions, typeInfo, fieldPrefix);
            // Standalone Count() in a predicate isn't natively comparable; surface to the binary visitor instead.
            // We can't return a numeric here — only filters. So only support count compared via outer binary.
            throw new NotSupportedException("Count() is only supported in projections, not predicates.");
        }

        throw new NotSupportedException($"Method '{declaringType?.Name}.{methodName}' is not supported in MongoDB queries.");
    }

    static bool TryResolveField(
        Expression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        string fieldPrefix,
        out string? field)
    {
        var current = expr;
        while (current is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            current = convert.Operand;

        if (current is not MemberExpression)
        {
            field = null;
            return false;
        }

        field = ResolveField(current, jsonOptions, typeInfo, fieldPrefix);
        return true;
    }

    static string ResolveField(
        Expression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        string fieldPrefix)
    {
        var current = expr;
        while (current is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            current = convert.Operand;

        var parts = new List<string>();
        while (current is MemberExpression member)
        {
            var name = ResolveJsonPropertyName(member, jsonOptions, typeInfo);
            parts.Insert(0, name);
            current = member.Expression;
        }

        if (parts.Count == 0)
            throw new NotSupportedException($"Cannot resolve field for expression: {expr}");

        return string.IsNullOrEmpty(fieldPrefix)
            ? string.Join(".", parts)
            : $"{fieldPrefix}.{string.Join(".", parts)}";
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

        if (jsonOptions.PropertyNamingPolicy != null)
            return jsonOptions.PropertyNamingPolicy.ConvertName(propertyName);

        return propertyName;
    }

    static object? EvaluateExpression(Expression expr)
    {
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            expr = unary.Operand;

        if (expr is ConstantExpression constant)
            return constant.Value;

        var lambda = Expression.Lambda(expr);
        return lambda.Compile().DynamicInvoke();
    }

    static BsonValue ToBsonValue(object? value) => value switch
    {
        null => BsonNull.Value,
        bool b => BsonBoolean.Create(b),
        int i => new BsonInt32(i),
        long l => new BsonInt64(l),
        double d => new BsonDouble(d),
        float f => new BsonDouble(f),
        decimal m => new BsonDecimal128(m),
        DateTime dt => new BsonDateTime(dt),
        DateTimeOffset dto => new BsonDateTime(dto.UtcDateTime),
        Guid g => new BsonString(g.ToString()),
        string s => new BsonString(s),
        _ => BsonValue.Create(value)
    };

    static bool IsEnumerableMethod(MethodCallExpression expr)
        => expr.Method.DeclaringType == typeof(Enumerable) || expr.Method.DeclaringType == typeof(Queryable);

    static Expression StripQuotes(Expression expr)
        => expr is UnaryExpression { NodeType: ExpressionType.Quote } unary ? unary.Operand : expr;
}
