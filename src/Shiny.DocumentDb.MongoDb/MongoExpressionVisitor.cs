using Shiny.DocumentDb.Internal.Query;
using System.Collections;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using MongoDB.Bson;
using MongoDB.Driver;
using Shiny.DocumentDb.Internal;

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

        // Scalar functions / computed operands (ToLower, string Length, Math.*, date parts, …) → $expr
        // aggregation. Checked before size comparison so string.Length routes here, not to collection $size.
        if (IsScalarExpr(expr.Left) || IsScalarExpr(expr.Right))
            return BuildExprComparison(expr, jsonOptions, typeInfo, fieldPrefix);

        // Collection size: x.Items.Count / x.Items.Length compared to a constant → $size operators.
        if (TryResolveSizeComparison(expr, jsonOptions, typeInfo, fieldPrefix, out var sizeFilter))
            return sizeFilter!;

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

        // DocumentFunctions spatial — MongoDB find filters natively support $geoIntersects / $geoWithin
        // (and $centerSphere for a point distance). The finer predicates have no filter operator; use the
        // dedicated store.Geo* methods for those.
        if (declaringType == typeof(DocumentFunctions))
        {
            Expression fieldExpr = expr.Arguments[0];
            while (fieldExpr is UnaryExpression { NodeType: ExpressionType.Convert } c) fieldExpr = c.Operand;
            var field = ResolveField(fieldExpr, jsonOptions, typeInfo, fieldPrefix);
            var query = EvaluateExpression(expr.Arguments[1]) switch
            {
                Geometry g => g,
                GeoPoint p => (Geometry)p,
                _ => throw new NotSupportedException("A DocumentFunctions spatial query must be a constant Geometry.")
            };
            var geoJson = BsonDocument.Parse(Shiny.DocumentDb.Internal.Spatial.SpatialJson.ToGeoJson(query));

            switch (methodName)
            {
                case nameof(DocumentFunctions.Intersects):
                    return new BsonDocument(field, new BsonDocument("$geoIntersects", new BsonDocument("$geometry", geoJson)));
                case nameof(DocumentFunctions.Within):
                    return new BsonDocument(field, new BsonDocument("$geoWithin", new BsonDocument("$geometry", geoJson)));
                case nameof(DocumentFunctions.WithinDistance) when query is GeoPointGeometry pt:
                    var meters = Convert.ToDouble(EvaluateExpression(expr.Arguments[2]));
                    return new BsonDocument(field, new BsonDocument("$geoWithin",
                        new BsonDocument("$centerSphere", new BsonArray
                        {
                            new BsonArray { pt.Point.Longitude, pt.Point.Latitude },
                            meters / 6_378_137.0
                        })));
                default:
                    throw new NotSupportedException(
                        $"DocumentFunctions.{methodName} in a Where is not translatable on MongoDB — use store.Geo{methodName}(...).");
            }
        }

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

        // Flag-enum test: enumValue.HasFlag(flag) → { field: { $bitsAllSet: mask } }.
        if (methodName == "HasFlag" && declaringType == typeof(Enum) && expr.Object != null)
        {
            var field = ResolveField(expr.Object, jsonOptions, typeInfo, fieldPrefix);
            var mask = Convert.ToInt64(EvaluateExpression(expr.Arguments[0]));
            return fb.BitsAllSet(field, mask);
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

        return ExpressionInterpreter.EvaluateClosed(expr);
    }

    // ── $expr aggregation for scalar functions ──────────────────────────────

    static Expression Strip(Expression e)
    {
        while (e is UnaryExpression { NodeType: ExpressionType.Convert } u)
            e = u.Operand;
        return e;
    }

    /// <summary>True when the operand is a computed value (scalar function / string length / date part /
    /// concat) that can't be a plain Mongo field and must go through <c>$expr</c>.</summary>
    static bool IsScalarExpr(Expression expr)
    {
        expr = Strip(expr);
        switch (expr)
        {
            case MethodCallExpression m when m.Method.DeclaringType == typeof(string)
                && m.Method.Name is "ToLower" or "ToLowerInvariant" or "ToUpper" or "ToUpperInvariant"
                    or "Trim" or "TrimStart" or "TrimEnd" or "Substring" or "Replace" or "IndexOf":
                return true;
            case MethodCallExpression m when m.Method.DeclaringType == typeof(Math):
                return true;
            case MemberExpression { Member.Name: "Length" } me when (me.Expression?.Type) == typeof(string):
                return true;
            case MemberExpression me when IsDateComponent(me):
                return true;
            case BinaryExpression { NodeType: ExpressionType.Add } b when b.Type == typeof(string):
                return true;
            default:
                return false;
        }
    }

    static bool IsDateComponent(MemberExpression me)
    {
        var t = me.Expression == null ? null : Nullable.GetUnderlyingType(me.Expression.Type) ?? me.Expression.Type;
        return (t == typeof(DateTime) || t == typeof(DateTimeOffset))
            && me.Member.Name is "Year" or "Month" or "Day" or "Hour" or "Minute" or "Second";
    }

    static FilterDefinition<BsonDocument> BuildExprComparison(
        BinaryExpression expr, JsonSerializerOptions jsonOptions, JsonTypeInfo? typeInfo, string fieldPrefix)
    {
        var op = expr.NodeType switch
        {
            ExpressionType.Equal => "$eq",
            ExpressionType.NotEqual => "$ne",
            ExpressionType.GreaterThan => "$gt",
            ExpressionType.GreaterThanOrEqual => "$gte",
            ExpressionType.LessThan => "$lt",
            ExpressionType.LessThanOrEqual => "$lte",
            _ => throw new NotSupportedException($"Operator '{expr.NodeType}' is not supported in a Mongo $expr.")
        };
        var left = BuildAggExpr(expr.Left, jsonOptions, typeInfo, fieldPrefix);
        var right = BuildAggExpr(expr.Right, jsonOptions, typeInfo, fieldPrefix);
        return new BsonDocument("$expr", new BsonDocument(op, new BsonArray { left, right }));
    }

    static BsonValue BuildAggExpr(Expression expr, JsonSerializerOptions jsonOptions, JsonTypeInfo? typeInfo, string fieldPrefix)
    {
        expr = Strip(expr);

        switch (expr)
        {
            case MemberExpression { Member.Name: "Length" } sl when (sl.Expression?.Type) == typeof(string):
                return new BsonDocument("$strLenCP", BuildAggExpr(sl.Expression!, jsonOptions, typeInfo, fieldPrefix));

            case MemberExpression dc when IsDateComponent(dc):
            {
                var inner = new BsonDocument("$dateFromString", new BsonDocument("dateString", BuildAggExpr(dc.Expression!, jsonOptions, typeInfo, fieldPrefix)));
                var dop = dc.Member.Name switch
                {
                    "Year" => "$year", "Month" => "$month", "Day" => "$dayOfMonth",
                    "Hour" => "$hour", "Minute" => "$minute", _ => "$second"
                };
                return new BsonDocument(dop, inner);
            }

            // Plain field reference.
            case MemberExpression when !IsCaptured(expr):
                return new BsonString("$" + ResolveField(expr, jsonOptions, typeInfo, fieldPrefix));

            case MethodCallExpression m when m.Method.DeclaringType == typeof(string):
                return BuildStringAgg(m, jsonOptions, typeInfo, fieldPrefix);

            case MethodCallExpression m when m.Method.DeclaringType == typeof(Math):
                return BuildMathAgg(m, jsonOptions, typeInfo, fieldPrefix);

            case BinaryExpression { NodeType: ExpressionType.Add } b when b.Type == typeof(string):
                return new BsonDocument("$concat", new BsonArray
                {
                    BuildAggExpr(b.Left, jsonOptions, typeInfo, fieldPrefix),
                    BuildAggExpr(b.Right, jsonOptions, typeInfo, fieldPrefix)
                });

            default:
            {
                // Constant / captured value → literal (wrapped so a leading '$' isn't read as a field path).
                var value = EvaluateExpression(expr);
                if (value != null && value.GetType().IsEnum)
                    value = Convert.ToInt64(value);
                return new BsonDocument("$literal", ToBsonValue(value));
            }
        }
    }

    static BsonValue BuildStringAgg(MethodCallExpression m, JsonSerializerOptions jsonOptions, JsonTypeInfo? typeInfo, string fieldPrefix)
    {
        var target = BuildAggExpr(m.Object!, jsonOptions, typeInfo, fieldPrefix);
        BsonValue Arg(int i) => BuildAggExpr(m.Arguments[i], jsonOptions, typeInfo, fieldPrefix);
        return m.Method.Name switch
        {
            "ToLower" or "ToLowerInvariant" => new BsonDocument("$toLower", target),
            "ToUpper" or "ToUpperInvariant" => new BsonDocument("$toUpper", target),
            "Trim" => new BsonDocument("$trim", new BsonDocument("input", target)),
            "TrimStart" => new BsonDocument("$ltrim", new BsonDocument("input", target)),
            "TrimEnd" => new BsonDocument("$rtrim", new BsonDocument("input", target)),
            "Replace" => new BsonDocument("$replaceAll", new BsonDocument { { "input", target }, { "find", Arg(0) }, { "replacement", Arg(1) } }),
            "IndexOf" => new BsonDocument("$indexOfCP", new BsonArray { target, Arg(0) }),
            "Substring" => new BsonDocument("$substrCP", new BsonArray
            {
                target,
                Arg(0),
                m.Arguments.Count == 2 ? Arg(1) : new BsonDocument("$subtract", new BsonArray { new BsonDocument("$strLenCP", target), Arg(0) })
            }),
            _ => throw new NotSupportedException($"String method '{m.Method.Name}' is not supported in a Mongo $expr.")
        };
    }

    static BsonValue BuildMathAgg(MethodCallExpression m, JsonSerializerOptions jsonOptions, JsonTypeInfo? typeInfo, string fieldPrefix)
    {
        var a0 = BuildAggExpr(m.Arguments[0], jsonOptions, typeInfo, fieldPrefix);
        return m.Method.Name switch
        {
            "Abs" => new BsonDocument("$abs", a0),
            "Ceiling" => new BsonDocument("$ceil", a0),
            "Floor" => new BsonDocument("$floor", a0),
            "Round" => new BsonDocument("$round", a0),
            "Sqrt" => new BsonDocument("$sqrt", a0),
            "Pow" => new BsonDocument("$pow", new BsonArray { a0, BuildAggExpr(m.Arguments[1], jsonOptions, typeInfo, fieldPrefix) }),
            _ => throw new NotSupportedException($"Math.{m.Method.Name} is not supported in a Mongo $expr.")
        };
    }

    static bool IsCaptured(Expression expr)
    {
        // A member chain bottoming out in a constant (closure/display class) is a captured value, not a field.
        var current = expr;
        while (current is MemberExpression m)
            current = m.Expression;
        return current is ConstantExpression;
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

    static bool TryResolveSizeComparison(
        BinaryExpression expr,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo? typeInfo,
        string fieldPrefix,
        out FilterDefinition<BsonDocument>? filter)
    {
        filter = null;

        Expression collection;
        Expression valueSide;
        bool reversed;
        if (TryGetSizeAccess(expr.Left, out var leftCollection))
        {
            collection = leftCollection;
            valueSide = expr.Right;
            reversed = false;
        }
        else if (TryGetSizeAccess(expr.Right, out var rightCollection))
        {
            collection = rightCollection;
            valueSide = expr.Left;
            reversed = true;
        }
        else
        {
            return false;
        }

        var rawValue = EvaluateExpression(valueSide);
        if (rawValue is not (int or long or short or byte))
            throw new NotSupportedException("Collection size comparisons require an integer constant.");

        var n = Convert.ToInt32(rawValue);
        var field = ResolveField(collection, jsonOptions, typeInfo, fieldPrefix);
        var op = reversed ? Flip(expr.NodeType) : expr.NodeType;
        var fb = Builders<BsonDocument>.Filter;

        filter = op switch
        {
            ExpressionType.Equal => fb.Size(field, n),
            ExpressionType.NotEqual => fb.Not(fb.Size(field, n)),
            ExpressionType.GreaterThan => fb.SizeGt(field, n),
            ExpressionType.GreaterThanOrEqual => fb.SizeGt(field, n - 1),
            ExpressionType.LessThan => fb.SizeLt(field, n),
            ExpressionType.LessThanOrEqual => fb.SizeLt(field, n + 1),
            _ => throw new NotSupportedException($"Operator '{op}' is not supported for collection size comparisons.")
        };
        return true;
    }

    static bool TryGetSizeAccess(Expression expr, out Expression collection)
    {
        collection = null!;

        var current = expr;
        while (current is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            current = convert.Operand;

        if (current is MemberExpression member)
        {
            var kind = CollectionMember.Classify(member, out var coll);
            if (kind == CollectionSizeKind.ArrayLength)
            {
                collection = coll;
                return true;
            }

            if (kind == CollectionSizeKind.Unsupported)
                throw new NotSupportedException(
                    $"'{member.Member.DeclaringType?.Name}.{member.Member.Name}' is not supported in MongoDB queries. " +
                    "Use '.Count()' or '.Any()' for collection length.");
        }

        return false;
    }

    static ExpressionType Flip(ExpressionType op) => op switch
    {
        ExpressionType.GreaterThan => ExpressionType.LessThan,
        ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
        ExpressionType.LessThan => ExpressionType.GreaterThan,
        ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
        _ => op
    };

    static bool IsEnumerableMethod(MethodCallExpression expr)
        => expr.Method.DeclaringType == typeof(Enumerable) || expr.Method.DeclaringType == typeof(Queryable);

    static Expression StripQuotes(Expression expr)
        => expr is UnaryExpression { NodeType: ExpressionType.Quote } unary ? unary.Operand : expr;
}
