using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

sealed class JsonExpressionVisitor : ExpressionVisitor
{
    readonly StringBuilder sql = new();
    readonly Dictionary<string, object?> parameters = new();
    readonly JsonSerializerOptions jsonOptions;
    readonly JsonTypeInfo rootTypeInfo;
    readonly IDatabaseProvider provider;
    int paramIndex;

    // json_each context for Any() lambdas
    ParameterExpression? jsonEachParameter;
    JsonTypeInfo? jsonEachElementTypeInfo;
    string? jsonEachPath;
    bool jsonEachIsPrimitive;

    JsonExpressionVisitor(JsonSerializerOptions jsonOptions, JsonTypeInfo rootTypeInfo, IDatabaseProvider provider)
    {
        this.jsonOptions = jsonOptions;
        this.rootTypeInfo = rootTypeInfo;
        this.provider = provider;
    }

    public static (string WhereClause, Dictionary<string, object?> Parameters) Translate<T>(
        Expression<Func<T, bool>> predicate,
        JsonTypeInfo<T> jsonTypeInfo,
        IDatabaseProvider provider)
    {
        var visitor = new JsonExpressionVisitor(jsonTypeInfo.Options, jsonTypeInfo, provider);
        visitor.Visit(predicate.Body);
        return (visitor.sql.ToString(), visitor.parameters);
    }

    string AddParameter(object? value)
    {
        var name = $"@p{this.paramIndex++}";
        this.parameters[name] = NormalizeValue(value);
        return name;
    }

    static object? NormalizeValue(object? value) => value switch
    {
        DateTime dt => FormatDateTime(dt),
        DateTimeOffset dto => FormatDateTimeOffset(dto),
        _ => value
    };

    // Matches System.Text.Json default DateTime formatting
    static string FormatDateTime(DateTime dt)
    {
        var fractional = dt.Ticks % TimeSpan.TicksPerSecond;
        var baseFmt = fractional != 0
            ? dt.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFF")
            : dt.ToString("yyyy-MM-ddTHH:mm:ss");

        return dt.Kind switch
        {
            DateTimeKind.Utc => baseFmt + "Z",
            DateTimeKind.Local => baseFmt + dt.ToString("zzz"),
            _ => baseFmt
        };
    }

    static string FormatDateTimeOffset(DateTimeOffset dto)
    {
        var fractional = dto.Ticks % TimeSpan.TicksPerSecond;
        var baseFmt = fractional != 0
            ? dto.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFF")
            : dto.ToString("yyyy-MM-ddTHH:mm:ss");

        return baseFmt + dto.ToString("zzz");
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        // Handle null comparisons: x == null, x != null
        if (IsNullConstant(node.Right))
        {
            var isNull = node.NodeType == ExpressionType.Equal;
            var target = StripConvert(node.Left);
            var rootChain = BuildMemberChainFromRoot(target);
            if (rootChain != null)
            {
                var jsonPath = JsonPropertyNameResolver.BuildJsonPath(this.jsonOptions, this.rootTypeInfo, rootChain);
                this.sql.Append('(');
                this.sql.Append(this.provider.JsonNullCheck("Data", jsonPath, isNull));
                this.sql.Append(')');
            }
            else
            {
                this.sql.Append('(');
                this.Visit(node.Left);
                this.sql.Append(isNull ? " IS NULL" : " IS NOT NULL");
                this.sql.Append(')');
            }
            return node;
        }

        if (IsNullConstant(node.Left))
        {
            var isNull = node.NodeType == ExpressionType.Equal;
            var target = StripConvert(node.Right);
            var rootChain = BuildMemberChainFromRoot(target);
            if (rootChain != null)
            {
                var jsonPath = JsonPropertyNameResolver.BuildJsonPath(this.jsonOptions, this.rootTypeInfo, rootChain);
                this.sql.Append('(');
                this.sql.Append(this.provider.JsonNullCheck("Data", jsonPath, isNull));
                this.sql.Append(')');
            }
            else
            {
                this.sql.Append('(');
                this.Visit(node.Right);
                this.sql.Append(isNull ? " IS NULL" : " IS NOT NULL");
                this.sql.Append(')');
            }
            return node;
        }

        this.sql.Append('(');
        this.Visit(node.Left);

        this.sql.Append(node.NodeType switch
        {
            ExpressionType.Equal => " = ",
            ExpressionType.NotEqual => " <> ",
            ExpressionType.GreaterThan => " > ",
            ExpressionType.GreaterThanOrEqual => " >= ",
            ExpressionType.LessThan => " < ",
            ExpressionType.LessThanOrEqual => " <= ",
            ExpressionType.AndAlso => " AND ",
            ExpressionType.OrElse => " OR ",
            _ => throw new NotSupportedException($"Binary operator '{node.NodeType}' is not supported.")
        });

        this.Visit(node.Right);
        this.sql.Append(')');
        return node;
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        if (node.NodeType == ExpressionType.Not)
        {
            this.sql.Append("NOT (");
            this.Visit(node.Operand);
            this.sql.Append(')');
            return node;
        }

        if (node.NodeType == ExpressionType.Convert)
        {
            this.Visit(node.Operand);
            return node;
        }

        throw new NotSupportedException($"Unary operator '{node.NodeType}' is not supported.");
    }

    protected override Expression VisitParameter(ParameterExpression node)
    {
        // Inside json_each lambda for a primitive collection: the parameter itself is the element
        if (this.jsonEachParameter != null && node == this.jsonEachParameter && this.jsonEachIsPrimitive)
        {
            this.sql.Append(this.provider.JsonEachPrimitiveValue);
            return node;
        }

        return base.VisitParameter(node);
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        // Check if this is a captured variable (closure access)
        if (TryExtractCapturedValue(node, out var capturedValue))
        {
            var paramName = this.AddParameter(capturedValue);
            this.sql.Append(paramName);
            return node;
        }

        // Inside json_each lambda — accessing element properties
        if (this.jsonEachParameter != null && IsParameterAccess(node, this.jsonEachParameter))
        {
            if (this.jsonEachIsPrimitive)
            {
                // Primitive collection: use provider-specific value accessor
                this.sql.Append(this.provider.JsonEachPrimitiveValue);
                return node;
            }

            // Object collection: json_extract(value, '$.prop')
            var chain = BuildMemberChain(node, this.jsonEachParameter);
            var jsonPath = this.jsonEachElementTypeInfo != null
                ? JsonPropertyNameResolver.BuildJsonPath(this.jsonOptions, this.jsonEachElementTypeInfo, chain)
                : string.Join('.', chain);

            this.sql.Append(this.provider.JsonExtractElementTyped(jsonPath, node.Type));
            return node;
        }

        // Root document property access
        var rootChain = BuildMemberChainFromRoot(node);
        if (rootChain != null)
        {
            var jsonPath2 = JsonPropertyNameResolver.BuildJsonPath(this.jsonOptions, this.rootTypeInfo, rootChain);
            var memberType = node.Type;
            this.sql.Append(this.provider.JsonExtractTyped("Data", jsonPath2, memberType));
            return node;
        }

        throw new NotSupportedException($"Member expression '{node}' is not supported.");
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        var paramName = this.AddParameter(node.Value);
        this.sql.Append(paramName);
        return node;
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // String methods: Contains, StartsWith, EndsWith
        if (node.Object != null && node.Method.DeclaringType == typeof(string))
        {
            return this.VisitStringMethod(node);
        }

        // Enumerable.Any(collection, predicate)
        if (node.Method.Name == "Any"
            && node.Method.DeclaringType == typeof(Enumerable)
            && node.Arguments.Count == 2)
        {
            return this.VisitAnyWithPredicate(node);
        }

        // Enumerable.Any(collection) — no predicate
        if (node.Method.Name == "Any"
            && node.Method.DeclaringType == typeof(Enumerable)
            && node.Arguments.Count == 1)
        {
            return this.VisitAnyNoPredicate(node);
        }

        // Enumerable.Count(collection) — no predicate
        if (node.Method.Name == "Count"
            && node.Method.DeclaringType == typeof(Enumerable)
            && node.Arguments.Count == 1)
        {
            return this.VisitCountNoPredicate(node);
        }

        // Enumerable.Count(collection, predicate)
        if (node.Method.Name == "Count"
            && node.Method.DeclaringType == typeof(Enumerable)
            && node.Arguments.Count == 2)
        {
            return this.VisitCountWithPredicate(node);
        }

        throw new NotSupportedException($"Method '{node.Method.Name}' on '{node.Method.DeclaringType?.Name}' is not supported.");
    }

    Expression VisitStringMethod(MethodCallExpression node)
    {
        var argValue = ExtractValue(node.Arguments[0]);
        var paramName = this.AddParameter(argValue);

        this.sql.Append('(');
        this.Visit(node.Object);

        switch (node.Method.Name)
        {
            case "Contains":
                this.sql.Append($" LIKE {this.provider.ConcatStrings("'%'", paramName, "'%'")}");
                break;
            case "StartsWith":
                this.sql.Append($" LIKE {this.provider.ConcatStrings(paramName, "'%'")}");
                break;
            case "EndsWith":
                this.sql.Append($" LIKE {this.provider.ConcatStrings("'%'", paramName)}");
                break;
            default:
                throw new NotSupportedException($"String method '{node.Method.Name}' is not supported.");
        }

        this.sql.Append(')');
        return node;
    }

    Expression VisitAnyNoPredicate(MethodCallExpression node)
    {
        var collectionJsonPath = ResolveCollectionJsonPath(node.Arguments[0]);
        this.sql.Append($"{this.provider.JsonArrayLength("Data", collectionJsonPath)} > 0");
        return node;
    }

    Expression VisitCountNoPredicate(MethodCallExpression node)
    {
        var collectionJsonPath = ResolveCollectionJsonPath(node.Arguments[0]);
        this.sql.Append(this.provider.JsonArrayLength("Data", collectionJsonPath));
        return node;
    }

    Expression VisitCountWithPredicate(MethodCallExpression node)
    {
        var collectionExpr = node.Arguments[0];
        var lambda = (LambdaExpression)node.Arguments[1];
        var collectionJsonPath = ResolveCollectionJsonPath(collectionExpr);

        var collectionType = collectionExpr.Type;
        var elementType = GetCollectionElementType(collectionType);
        var isPrimitive = IsPrimitiveType(elementType);

        // Save/set json_each context
        var savedParam = this.jsonEachParameter;
        var savedTypeInfo = this.jsonEachElementTypeInfo;
        var savedPath = this.jsonEachPath;
        var savedIsPrimitive = this.jsonEachIsPrimitive;

        this.jsonEachParameter = lambda.Parameters[0];
        this.jsonEachPath = collectionJsonPath;
        this.jsonEachIsPrimitive = isPrimitive;
        this.jsonEachElementTypeInfo = isPrimitive ? null : this.jsonOptions.GetTypeInfo(elementType);

        this.sql.Append($"(SELECT COUNT(*) FROM {this.provider.JsonEachFrom("Data", collectionJsonPath)} WHERE ");
        this.Visit(lambda.Body);
        this.sql.Append(')');

        // Restore outer state
        this.jsonEachParameter = savedParam;
        this.jsonEachElementTypeInfo = savedTypeInfo;
        this.jsonEachPath = savedPath;
        this.jsonEachIsPrimitive = savedIsPrimitive;

        return node;
    }

    Expression VisitAnyWithPredicate(MethodCallExpression node)
    {
        var collectionExpr = node.Arguments[0];
        var lambda = (LambdaExpression)node.Arguments[1];
        var collectionJsonPath = ResolveCollectionJsonPath(collectionExpr);

        var collectionType = collectionExpr.Type;
        var elementType = GetCollectionElementType(collectionType);
        var isPrimitive = IsPrimitiveType(elementType);

        // Save/set json_each context
        var savedParam = this.jsonEachParameter;
        var savedTypeInfo = this.jsonEachElementTypeInfo;
        var savedPath = this.jsonEachPath;
        var savedIsPrimitive = this.jsonEachIsPrimitive;

        this.jsonEachParameter = lambda.Parameters[0];
        this.jsonEachPath = collectionJsonPath;
        this.jsonEachIsPrimitive = isPrimitive;
        this.jsonEachElementTypeInfo = isPrimitive ? null : this.jsonOptions.GetTypeInfo(elementType);

        this.sql.Append($"(SELECT COUNT(*) FROM {this.provider.JsonEachFrom("Data", collectionJsonPath)} WHERE ");
        this.Visit(lambda.Body);
        this.sql.Append(") > 0");

        // Restore outer state
        this.jsonEachParameter = savedParam;
        this.jsonEachElementTypeInfo = savedTypeInfo;
        this.jsonEachPath = savedPath;
        this.jsonEachIsPrimitive = savedIsPrimitive;

        return node;
    }

    // ── Helpers ───────────────────────────────────────────────────────

    string ResolveCollectionJsonPath(Expression collectionExpr)
    {
        var chain = BuildMemberChainFromRoot(collectionExpr);
        if (chain == null)
            throw new NotSupportedException($"Collection expression '{collectionExpr}' is not supported.");
        return JsonPropertyNameResolver.BuildJsonPath(this.jsonOptions, this.rootTypeInfo, chain);
    }

    static bool IsNullConstant(Expression expr)
    {
        if (expr is ConstantExpression { Value: null })
            return true;

        // Captured null: member access that evaluates to null
        if (expr is MemberExpression member && TryExtractCapturedValue(member, out var val) && val is null)
            return true;

        return false;
    }

    static Expression StripConvert(Expression expr)
        => expr is UnaryExpression { NodeType: ExpressionType.Convert } u ? u.Operand : expr;

    [UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "FieldInfo.GetValue on compiler-generated display classes is always preserved when referenced by expression trees.")]
    static bool TryExtractCapturedValue(MemberExpression node, out object? value)
    {
        value = null;

        // Walk the chain of member accesses down to a ConstantExpression (the closure object)
        var chain = new List<MemberInfo>();
        Expression? current = node;

        while (current is MemberExpression memberExpr)
        {
            chain.Add(memberExpr.Member);
            current = memberExpr.Expression;
        }

        if (current is not ConstantExpression constant)
            return false;

        // If the constant is the lambda parameter root, this isn't a captured variable
        if (constant.Type.GetCustomAttribute<System.Runtime.CompilerServices.CompilerGeneratedAttribute>() == null
            && chain.Count == 1
            && chain[0].DeclaringType != constant.Type)
            return false;

        var obj = constant.Value;
        for (var i = chain.Count - 1; i >= 0; i--)
        {
            var member = chain[i];
            if (member is FieldInfo fi)
                obj = fi.GetValue(obj);
            else if (member is PropertyInfo pi)
                obj = pi.GetValue(obj);
            else
                return false;
        }

        value = obj;
        return true;
    }

    static object? ExtractValue(Expression expr)
    {
        if (expr is ConstantExpression c)
            return c.Value;

        if (expr is MemberExpression m && TryExtractCapturedValue(m, out var val))
            return val;

        throw new NotSupportedException($"Cannot extract value from expression '{expr}'.");
    }

    static bool IsParameterAccess(Expression node, ParameterExpression param)
    {
        var current = node;
        while (current is MemberExpression m)
            current = m.Expression;
        return current == param;
    }

    static List<string> BuildMemberChain(Expression node, ParameterExpression stopAt)
    {
        var chain = new List<string>();
        var current = node;
        while (current is MemberExpression m)
        {
            chain.Insert(0, m.Member.Name);
            current = m.Expression;
            if (current == stopAt)
                break;
        }
        return chain;
    }

    List<string>? BuildMemberChainFromRoot(Expression node)
    {
        var chain = new List<string>();
        var current = node;
        while (current is MemberExpression m)
        {
            chain.Insert(0, m.Member.Name);
            current = m.Expression;
        }

        // Must terminate at the root lambda parameter
        if (current is ParameterExpression)
            return chain;

        return null;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2070",
        Justification = "Collection types come from expression trees referencing known model types; interfaces are always preserved.")]
    static Type GetCollectionElementType(Type collectionType)
    {
        if (collectionType.IsGenericType)
        {
            var genArgs = collectionType.GetGenericArguments();
            if (genArgs.Length == 1)
                return genArgs[0];
        }

        foreach (var iface in collectionType.GetInterfaces())
        {
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                return iface.GetGenericArguments()[0];
        }

        return typeof(object);
    }

    static bool IsPrimitiveType(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;
        return t.IsPrimitive
            || t == typeof(string)
            || t == typeof(decimal)
            || t == typeof(DateTime)
            || t == typeof(DateTimeOffset)
            || t == typeof(Guid);
    }
}
