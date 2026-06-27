using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.OData;

/// <summary>
/// Converts a provider-neutral <see cref="ODataFilterNode"/> tree into an
/// <see cref="Expression{TDelegate}"/> of <c>Func&lt;T,bool&gt;</c> that can be fed to
/// <c>IDocumentQuery&lt;T&gt;.Where</c>, which every provider lowers to its native query language.
/// <para>
/// Member resolution walks the supplied <see cref="JsonTypeInfo{T}"/> (source-generated) so the engine
/// stays AOT/trim-clean — it never serializes untyped objects, never calls <c>Compile()</c>, and binds
/// string/date functions through cached <see cref="MethodInfo"/> on known framework types (the same
/// pattern the core <c>FilterExpressionParser</c> uses).
/// </para>
/// </summary>
public static class FilterExpressionBuilder
{
    static readonly MethodInfo StringContains = typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!;
    static readonly MethodInfo StringStartsWith = typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string)])!;
    static readonly MethodInfo StringEndsWith = typeof(string).GetMethod(nameof(string.EndsWith), [typeof(string)])!;
    static readonly MethodInfo StringToLower = typeof(string).GetMethod(nameof(string.ToLower), Type.EmptyTypes)!;
    static readonly MethodInfo StringToUpper = typeof(string).GetMethod(nameof(string.ToUpper), Type.EmptyTypes)!;
    static readonly MethodInfo StringTrim = typeof(string).GetMethod(nameof(string.Trim), Type.EmptyTypes)!;
    static readonly MethodInfo StringIndexOf = typeof(string).GetMethod(nameof(string.IndexOf), [typeof(string)])!;
    static readonly MethodInfo StringConcat = typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string)])!;
    static readonly PropertyInfo StringLength = typeof(string).GetProperty(nameof(string.Length))!;

    public static Expression<Func<T, bool>> Build<T>(ODataFilterNode node, JsonTypeInfo<T> typeInfo)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(typeInfo);

        var parameter = Expression.Parameter(typeof(T), "x");
        var body = BuildBool(node, parameter, typeInfo);
        return Expression.Lambda<Func<T, bool>>(body, parameter);
    }

    static Expression BuildBool(ODataFilterNode node, ParameterExpression parameter, JsonTypeInfo typeInfo)
    {
        switch (node.Kind)
        {
            case ODataNodeKind.Binary:
                return BuildBinary(node, parameter, typeInfo);

            case ODataNodeKind.Unary when node.UnaryOperator == ODataUnaryOperator.Not:
                return Expression.Not(BuildBool(node.Operand!, parameter, typeInfo));

            case ODataNodeKind.Function:
                return BuildBooleanFunction(node, parameter, typeInfo);

            case ODataNodeKind.Property:
                return BuildValue(node, parameter, typeInfo, typeof(bool));

            default:
                throw new ODataNotSupportedException($"Unsupported boolean filter node '{node.Kind}'.");
        }
    }

    static Expression BuildBinary(ODataFilterNode node, ParameterExpression parameter, JsonTypeInfo typeInfo)
    {
        switch (node.BinaryOperator)
        {
            case ODataBinaryOperator.And:
                return Expression.AndAlso(
                    BuildBool(node.Left!, parameter, typeInfo),
                    BuildBool(node.Right!, parameter, typeInfo));

            case ODataBinaryOperator.Or:
                return Expression.OrElse(
                    BuildBool(node.Left!, parameter, typeInfo),
                    BuildBool(node.Right!, parameter, typeInfo));
        }

        var (left, right) = BuildComparisonOperands(node.Left!, node.Right!, parameter, typeInfo);

        return node.BinaryOperator switch
        {
            ODataBinaryOperator.Equal => Expression.Equal(left, right),
            ODataBinaryOperator.NotEqual => Expression.NotEqual(left, right),
            ODataBinaryOperator.GreaterThan => Expression.GreaterThan(left, right),
            ODataBinaryOperator.GreaterThanOrEqual => Expression.GreaterThanOrEqual(left, right),
            ODataBinaryOperator.LessThan => Expression.LessThan(left, right),
            ODataBinaryOperator.LessThanOrEqual => Expression.LessThanOrEqual(left, right),
            _ => throw new ODataNotSupportedException($"Unsupported comparison operator '{node.BinaryOperator}'.")
        };
    }

    static (Expression Left, Expression Right) BuildComparisonOperands(
        ODataFilterNode leftNode, ODataFilterNode rightNode, ParameterExpression parameter, JsonTypeInfo typeInfo)
    {
        var targetType = InferType(leftNode, parameter, typeInfo)
                         ?? InferType(rightNode, parameter, typeInfo);

        var left = BuildValue(leftNode, parameter, typeInfo, targetType);
        var right = BuildValue(rightNode, parameter, typeInfo, left.Type);

        if (left.Type != right.Type)
        {
            if (IsNullConstant(rightNode))
                (left, right) = MakeNullComparable(left);
            else if (IsNullConstant(leftNode))
                (right, left) = MakeNullComparable(right);
            else
                right = Expression.Convert(right, left.Type);
        }
        return (left, right);
    }

    static (Expression Member, Expression NullConstant) MakeNullComparable(Expression member)
    {
        var type = member.Type;
        if (type.IsValueType && Nullable.GetUnderlyingType(type) == null)
        {
            // Box the value-type member to object so `== null` is comparable without constructing a
            // Nullable<> type at runtime (AOT-clean). This mirrors the core FilterExpressionParser.
            var boxed = Expression.Convert(member, typeof(object));
            return (boxed, Expression.Constant(null, typeof(object)));
        }
        return (member, Expression.Constant(null, type));
    }

    static bool IsNullConstant(ODataFilterNode node)
        => node.Kind == ODataNodeKind.Constant && node.Value is null;

    static Type? InferType(ODataFilterNode node, ParameterExpression parameter, JsonTypeInfo typeInfo)
        => node.Kind switch
        {
            ODataNodeKind.Property => BuildMember(node.PropertyPath!, parameter, typeInfo).Type,
            ODataNodeKind.Function => InferFunctionType(node),
            _ => null
        };

    static Type? InferFunctionType(ODataFilterNode node) => node.FunctionName?.ToLowerInvariant() switch
    {
        "tolower" or "toupper" or "trim" or "concat" => typeof(string),
        "length" or "indexof" or "year" or "month" or "day" or "hour" or "minute" or "second" => typeof(int),
        "contains" or "startswith" or "endswith" => typeof(bool),
        _ => null
    };

    static Expression BuildValue(ODataFilterNode node, ParameterExpression parameter, JsonTypeInfo typeInfo, Type? targetType)
    {
        switch (node.Kind)
        {
            case ODataNodeKind.Property:
                return BuildMember(node.PropertyPath!, parameter, typeInfo);

            case ODataNodeKind.Constant:
                return BuildConstant(node.Value, targetType);

            case ODataNodeKind.Function:
                return BuildScalarFunction(node, parameter, typeInfo);

            case ODataNodeKind.Binary:
                var (l, r) = BuildComparisonOperands(node.Left!, node.Right!, parameter, typeInfo);
                return node.BinaryOperator switch
                {
                    ODataBinaryOperator.Add => Expression.Add(l, r),
                    ODataBinaryOperator.Subtract => Expression.Subtract(l, r),
                    ODataBinaryOperator.Multiply => Expression.Multiply(l, r),
                    ODataBinaryOperator.Divide => Expression.Divide(l, r),
                    ODataBinaryOperator.Modulo => Expression.Modulo(l, r),
                    _ => throw new ODataNotSupportedException($"Unsupported arithmetic operator '{node.BinaryOperator}'.")
                };

            case ODataNodeKind.Unary when node.UnaryOperator == ODataUnaryOperator.Negate:
                return Expression.Negate(BuildValue(node.Operand!, parameter, typeInfo, targetType));

            default:
                throw new ODataNotSupportedException($"Unsupported value node '{node.Kind}'.");
        }
    }

    static Expression BuildConstant(object? value, Type? targetType)
    {
        if (value is null)
            return Expression.Constant(null, targetType ?? typeof(object));

        if (targetType is null)
            return Expression.Constant(value, value.GetType());

        var nonNullable = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (nonNullable.IsInstanceOfType(value))
            return Expression.Constant(value, targetType);

        var converted = ConvertLiteral(value, nonNullable);
        return Expression.Constant(converted, nonNullable);
    }

    static object ConvertLiteral(object value, Type targetType)
    {
        if (targetType.IsEnum)
        {
            return value is string s
                ? Enum.Parse(targetType, s, ignoreCase: true)
                : Enum.ToObject(targetType, value);
        }
        if (targetType == typeof(Guid))
            return value is Guid g ? g : Guid.Parse(value.ToString()!);
        if (targetType == typeof(DateTime))
            return value is DateTime dt ? dt : DateTime.Parse(value.ToString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        if (targetType == typeof(DateTimeOffset))
            return value is DateTimeOffset dto ? dto : DateTimeOffset.Parse(value.ToString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

        return Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
    }

    static Expression BuildScalarFunction(ODataFilterNode node, ParameterExpression parameter, JsonTypeInfo typeInfo)
    {
        var name = node.FunctionName!.ToLowerInvariant();
        var args = node.Arguments;

        switch (name)
        {
            case "tolower":
                return Expression.Call(Str(args[0], parameter, typeInfo), StringToLower);
            case "toupper":
                return Expression.Call(Str(args[0], parameter, typeInfo), StringToUpper);
            case "trim":
                return Expression.Call(Str(args[0], parameter, typeInfo), StringTrim);
            case "length":
                return Expression.Property(Str(args[0], parameter, typeInfo), StringLength);
            case "indexof":
                return Expression.Call(Str(args[0], parameter, typeInfo), StringIndexOf, Str(args[1], parameter, typeInfo));
            case "concat":
                return Expression.Call(StringConcat, Str(args[0], parameter, typeInfo), Str(args[1], parameter, typeInfo));
            case "year" or "month" or "day" or "hour" or "minute" or "second":
                var dateExpr = BuildValue(args[0], parameter, typeInfo, typeof(DateTime));
                return Expression.Property(dateExpr, DatePart(dateExpr.Type, name));
            default:
                throw new ODataNotSupportedException($"Unsupported scalar function '{node.FunctionName}'.");
        }
    }

    // Cached date-part properties on the known framework date types — avoids reflective GetProperty
    // on an open Type (keeps the AOT analyzer clean).
    static PropertyInfo DatePart(Type dateType, string name)
    {
        var underlying = Nullable.GetUnderlyingType(dateType) ?? dateType;
        if (underlying == typeof(DateTime))
            return DateTimePart(name);
        if (underlying == typeof(DateTimeOffset))
            return DateTimeOffsetPart(name);
        throw new ODataNotSupportedException($"Date part '{name}' is not available on '{underlying.Name}'.");
    }

    static PropertyInfo DateTimePart(string name) => name switch
    {
        "year" => typeof(DateTime).GetProperty(nameof(DateTime.Year))!,
        "month" => typeof(DateTime).GetProperty(nameof(DateTime.Month))!,
        "day" => typeof(DateTime).GetProperty(nameof(DateTime.Day))!,
        "hour" => typeof(DateTime).GetProperty(nameof(DateTime.Hour))!,
        "minute" => typeof(DateTime).GetProperty(nameof(DateTime.Minute))!,
        "second" => typeof(DateTime).GetProperty(nameof(DateTime.Second))!,
        _ => throw new ODataNotSupportedException($"Unsupported date part '{name}'.")
    };

    static PropertyInfo DateTimeOffsetPart(string name) => name switch
    {
        "year" => typeof(DateTimeOffset).GetProperty(nameof(DateTimeOffset.Year))!,
        "month" => typeof(DateTimeOffset).GetProperty(nameof(DateTimeOffset.Month))!,
        "day" => typeof(DateTimeOffset).GetProperty(nameof(DateTimeOffset.Day))!,
        "hour" => typeof(DateTimeOffset).GetProperty(nameof(DateTimeOffset.Hour))!,
        "minute" => typeof(DateTimeOffset).GetProperty(nameof(DateTimeOffset.Minute))!,
        "second" => typeof(DateTimeOffset).GetProperty(nameof(DateTimeOffset.Second))!,
        _ => throw new ODataNotSupportedException($"Unsupported date part '{name}'.")
    };

    static Expression BuildBooleanFunction(ODataFilterNode node, ParameterExpression parameter, JsonTypeInfo typeInfo)
    {
        var name = node.FunctionName!.ToLowerInvariant();
        var args = node.Arguments;

        return name switch
        {
            "contains" => Expression.Call(Str(args[0], parameter, typeInfo), StringContains, Str(args[1], parameter, typeInfo)),
            "startswith" => Expression.Call(Str(args[0], parameter, typeInfo), StringStartsWith, Str(args[1], parameter, typeInfo)),
            "endswith" => Expression.Call(Str(args[0], parameter, typeInfo), StringEndsWith, Str(args[1], parameter, typeInfo)),
            _ => throw new ODataNotSupportedException($"Unsupported boolean function '{node.FunctionName}'.")
        };
    }

    static Expression Str(ODataFilterNode node, ParameterExpression parameter, JsonTypeInfo typeInfo)
    {
        var expr = BuildValue(node, parameter, typeInfo, typeof(string));
        return expr.Type == typeof(string) ? expr : Expression.Convert(expr, typeof(string));
    }

    static Expression BuildMember(string propertyPath, ParameterExpression parameter, JsonTypeInfo typeInfo)
    {
        Expression body = parameter;
        var currentTypeInfo = typeInfo;

        var segments = propertyPath.Split('/', '.');
        for (var i = 0; i < segments.Length; i++)
        {
            var name = segments[i].Trim();
            if (name.Length == 0)
                throw new ArgumentException("Property path contains an empty segment.", nameof(propertyPath));

            var propertyInfo = ResolvePropertyInfo(currentTypeInfo, name)
                ?? ResolveComputedPropertyInfo(currentTypeInfo, name)
                ?? throw new ArgumentException(
                    $"Property '{name}' not found on type '{currentTypeInfo.Type.Name}'.", nameof(propertyPath));

            body = Expression.Property(body, propertyInfo);

            if (i < segments.Length - 1)
                currentTypeInfo = typeInfo.Options.GetTypeInfo(propertyInfo.PropertyType);
        }
        return body;
    }

    // Computed properties are [JsonIgnore]'d and therefore absent from JsonTypeInfo.Properties; resolve
    // them reflectively so an OData $filter can reference them. The downstream computed-aware Where
    // pipeline performs the actual translation (inline expression / materialized column).
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "Property resolved by name on a user-constructed model type that is not subject to trimming.")]
    static PropertyInfo? ResolveComputedPropertyInfo(JsonTypeInfo typeInfo, string name)
        => typeInfo.Type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);

    static PropertyInfo? ResolvePropertyInfo(JsonTypeInfo typeInfo, string name)
    {
        foreach (var prop in typeInfo.Properties)
        {
            if (prop.AttributeProvider is PropertyInfo pi &&
                (pi.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                 prop.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
            {
                return pi;
            }
        }
        return null;
    }
}
