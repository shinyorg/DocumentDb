using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

static class ProjectionTranslator
{
    public static (string Projection, Dictionary<string, object?> Parameters) Translate<T, TResult>(
        Expression<Func<T, TResult>> selector,
        JsonTypeInfo<T> sourceTypeInfo,
        JsonTypeInfo<TResult> resultTypeInfo,
        IDatabaseProvider provider)
    {
        if (selector.Body is not MemberInitExpression memberInit)
            throw new NotSupportedException(
                "Projection selector must be a member initialization expression (e.g. x => new Result { A = x.A }).");

        var parameter = selector.Parameters[0];
        var ctx = new TranslationContext(sourceTypeInfo.Options, sourceTypeInfo, provider);
        var pairs = new List<string>(memberInit.Bindings.Count * 2);

        foreach (var binding in memberInit.Bindings)
        {
            if (binding is not MemberAssignment assignment)
                throw new NotSupportedException(
                    $"Only property assignment bindings are supported in projections. '{binding.Member.Name}' uses '{binding.BindingType}'.");

            var (resultKey, _) = JsonPropertyNameResolver.ResolveProperty(resultTypeInfo, assignment.Member.Name);

            pairs.Add($"'{resultKey}'");
            pairs.Add(TranslateValueExpression(assignment.Expression, parameter, ctx));
        }

        return (provider.JsonObject(pairs), ctx.Parameters);
    }

    static string TranslateValueExpression(Expression expr, ParameterExpression parameter, TranslationContext ctx)
    {
        // Method call: Count(), Any(), etc.
        if (expr is MethodCallExpression methodCall)
            return TranslateMethodCall(methodCall, parameter, ctx);

        // Simple member access chain: x.Prop or x.Nav.Prop
        var sourceChain = BuildMemberChainFromRoot(expr, parameter);
        if (sourceChain != null)
        {
            var sourcePath = JsonPropertyNameResolver.BuildJsonPath(ctx.JsonOptions, ctx.SourceTypeInfo, sourceChain);
            return ctx.Provider.JsonExtract("Data", sourcePath);
        }

        throw new NotSupportedException(
            $"Projection binding expression '{expr}' must be a simple member access or a supported method call (Count, Any, Sum, Min, Max, Average).");
    }

    static string TranslateMethodCall(MethodCallExpression node, ParameterExpression parameter, TranslationContext ctx)
    {
        if (node.Method.DeclaringType != typeof(Enumerable))
            throw new NotSupportedException(
                $"Method '{node.Method.Name}' on '{node.Method.DeclaringType?.Name}' is not supported in projections.");

        var collectionChain = BuildMemberChainFromRoot(node.Arguments[0], parameter);
        if (collectionChain == null)
            throw new NotSupportedException(
                $"Collection expression '{node.Arguments[0]}' must be a member access from the source parameter.");

        var collectionPath = JsonPropertyNameResolver.BuildJsonPath(ctx.JsonOptions, ctx.SourceTypeInfo, collectionChain);

        return (node.Method.Name, node.Arguments.Count) switch
        {
            ("Count", 1) => ctx.Provider.JsonArrayLength("Data", collectionPath),
            ("Count", 2) => TranslateCountWithPredicate(node, collectionPath, ctx),
            ("Any", 1) => $"CASE WHEN {ctx.Provider.JsonArrayLength("Data", collectionPath)} > 0 THEN {ctx.Provider.JsonTrue()} ELSE {ctx.Provider.JsonFalse()} END",
            ("Any", 2) => TranslateAnyWithPredicate(node, collectionPath, ctx),
            ("Sum", 1) => TranslateSimpleAggregate("SUM", node, collectionPath, ctx),
            ("Sum", 2) => TranslateAggregateWithSelector("SUM", node, collectionPath, ctx),
            ("Min", 1) => TranslateSimpleAggregate("MIN", node, collectionPath, ctx),
            ("Min", 2) => TranslateAggregateWithSelector("MIN", node, collectionPath, ctx),
            ("Max", 1) => TranslateSimpleAggregate("MAX", node, collectionPath, ctx),
            ("Max", 2) => TranslateAggregateWithSelector("MAX", node, collectionPath, ctx),
            ("Average", 1) => TranslateSimpleAggregate("AVG", node, collectionPath, ctx),
            ("Average", 2) => TranslateAggregateWithSelector("AVG", node, collectionPath, ctx),
            _ => throw new NotSupportedException(
                $"Method '{node.Method.Name}' with {node.Arguments.Count} arguments is not supported in projections.")
        };
    }

    static string TranslateSimpleAggregate(string sqlFunc, MethodCallExpression node, string collectionPath, TranslationContext ctx)
    {
        var valueExpr = sqlFunc is "SUM" or "AVG" or "MAX" or "MIN"
            ? ctx.Provider.JsonEachPrimitiveNumericValue
            : ctx.Provider.JsonEachPrimitiveValue;
        var sql = $"(SELECT {sqlFunc}({valueExpr}) FROM {ctx.Provider.JsonEachFrom("Data", collectionPath)})";
        if (sqlFunc is "SUM" or "MAX" or "MIN" && IsIntegerType(node.Type))
            sql = ctx.Provider.CastIntegerAggregate(sql);
        return sql;
    }

    static string TranslateAggregateWithSelector(string sqlFunc, MethodCallExpression node, string collectionPath, TranslationContext ctx)
    {
        var lambda = (LambdaExpression)node.Arguments[1];
        var collectionType = node.Arguments[0].Type;
        var elementType = GetCollectionElementType(collectionType);
        var isPrimitive = IsPrimitiveType(elementType);

        if (isPrimitive)
        {
            var valueExpr = sqlFunc is "SUM" or "AVG" or "MAX" or "MIN"
                ? ctx.Provider.JsonEachPrimitiveNumericValue
                : ctx.Provider.JsonEachPrimitiveValue;
            var sql = $"(SELECT {sqlFunc}({valueExpr}) FROM {ctx.Provider.JsonEachFrom("Data", collectionPath)})";
            if (sqlFunc is "SUM" or "MAX" or "MIN" && IsIntegerType(node.Type))
                sql = ctx.Provider.CastIntegerAggregate(sql);
            return sql;
        }

        var selectorBody = lambda.Body;
        if (selectorBody is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            selectorBody = unary.Operand;

        var memberChain = BuildMemberChain(selectorBody, lambda.Parameters[0]);
        var elementTypeInfo = ctx.JsonOptions.GetTypeInfo(elementType);
        var propPath = JsonPropertyNameResolver.BuildJsonPath(ctx.JsonOptions, elementTypeInfo, memberChain);

        var extractExpr = sqlFunc is "SUM" or "AVG" or "MAX" or "MIN"
            ? ctx.Provider.JsonExtractElementNumeric(propPath)
            : ctx.Provider.JsonExtractElement(propPath);
        var result = $"(SELECT {sqlFunc}({extractExpr}) FROM {ctx.Provider.JsonEachFrom("Data", collectionPath)})";
        if (sqlFunc is "SUM" or "MAX" or "MIN" && IsIntegerType(node.Type))
            result = ctx.Provider.CastIntegerAggregate(result);
        return result;
    }

    static string TranslateCountWithPredicate(MethodCallExpression node, string collectionPath, TranslationContext ctx)
    {
        var lambda = (LambdaExpression)node.Arguments[1];
        var collectionType = node.Arguments[0].Type;
        var elementType = GetCollectionElementType(collectionType);
        var isPrimitive = IsPrimitiveType(elementType);
        var elementTypeInfo = isPrimitive ? null : ctx.JsonOptions.GetTypeInfo(elementType);

        var innerSql = TranslateInnerPredicate(lambda.Body, lambda.Parameters[0], isPrimitive, elementTypeInfo, ctx);
        return $"(SELECT COUNT(*) FROM {ctx.Provider.JsonEachFrom("Data", collectionPath)} WHERE {innerSql})";
    }

    static string TranslateAnyWithPredicate(MethodCallExpression node, string collectionPath, TranslationContext ctx)
    {
        var lambda = (LambdaExpression)node.Arguments[1];
        var collectionType = node.Arguments[0].Type;
        var elementType = GetCollectionElementType(collectionType);
        var isPrimitive = IsPrimitiveType(elementType);
        var elementTypeInfo = isPrimitive ? null : ctx.JsonOptions.GetTypeInfo(elementType);

        var innerSql = TranslateInnerPredicate(lambda.Body, lambda.Parameters[0], isPrimitive, elementTypeInfo, ctx);
        return $"CASE WHEN EXISTS (SELECT 1 FROM {ctx.Provider.JsonEachFrom("Data", collectionPath)} WHERE {innerSql}) THEN {ctx.Provider.JsonTrue()} ELSE {ctx.Provider.JsonFalse()} END";
    }

    // ── Inner predicate translation (inside json_each context) ──────

    static string TranslateInnerPredicate(
        Expression expr,
        ParameterExpression elementParam,
        bool isPrimitive,
        JsonTypeInfo? elementTypeInfo,
        TranslationContext ctx)
    {
        return expr switch
        {
            BinaryExpression binary => TranslateInnerBinary(binary, elementParam, isPrimitive, elementTypeInfo, ctx),
            UnaryExpression { NodeType: ExpressionType.Not } unary =>
                $"NOT ({TranslateInnerPredicate(unary.Operand, elementParam, isPrimitive, elementTypeInfo, ctx)})",
            UnaryExpression { NodeType: ExpressionType.Convert } unary =>
                TranslateInnerPredicate(unary.Operand, elementParam, isPrimitive, elementTypeInfo, ctx),
            MethodCallExpression method => TranslateInnerMethodCall(method, elementParam, isPrimitive, elementTypeInfo, ctx),
            MemberExpression member => TranslateInnerMemberAccess(member, elementParam, isPrimitive, elementTypeInfo, ctx),
            ConstantExpression constant => ctx.AddParameter(constant.Value),
            ParameterExpression p when p == elementParam && isPrimitive => ctx.Provider.JsonEachPrimitiveValue,
            _ => throw new NotSupportedException($"Expression '{expr}' is not supported in inner predicates.")
        };
    }

    static string TranslateInnerBinary(
        BinaryExpression node,
        ParameterExpression elementParam,
        bool isPrimitive,
        JsonTypeInfo? elementTypeInfo,
        TranslationContext ctx)
    {
        // Handle null comparisons
        if (IsNullExpression(node.Right))
        {
            var left = TranslateInnerPredicate(node.Left, elementParam, isPrimitive, elementTypeInfo, ctx);
            var op = node.NodeType == ExpressionType.Equal ? "IS NULL" : "IS NOT NULL";
            return $"({left} {op})";
        }

        if (IsNullExpression(node.Left))
        {
            var right = TranslateInnerPredicate(node.Right, elementParam, isPrimitive, elementTypeInfo, ctx);
            var op = node.NodeType == ExpressionType.Equal ? "IS NULL" : "IS NOT NULL";
            return $"({right} {op})";
        }

        var leftSql = TranslateInnerPredicate(node.Left, elementParam, isPrimitive, elementTypeInfo, ctx);
        var sqlOp = node.NodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            _ => throw new NotSupportedException($"Binary operator '{node.NodeType}' is not supported in inner predicates.")
        };
        var rightSql = TranslateInnerPredicate(node.Right, elementParam, isPrimitive, elementTypeInfo, ctx);

        return $"({leftSql} {sqlOp} {rightSql})";
    }

    static string TranslateInnerMemberAccess(
        MemberExpression node,
        ParameterExpression elementParam,
        bool isPrimitive,
        JsonTypeInfo? elementTypeInfo,
        TranslationContext ctx)
    {
        // Captured variable
        if (TryExtractCapturedValue(node, out var capturedValue))
            return ctx.AddParameter(capturedValue);

        // Element parameter access
        if (IsParameterAccess(node, elementParam))
        {
            if (isPrimitive)
                return ctx.Provider.JsonEachPrimitiveValue;

            var chain = BuildMemberChain(node, elementParam);
            var jsonPath = elementTypeInfo != null
                ? JsonPropertyNameResolver.BuildJsonPath(ctx.JsonOptions, elementTypeInfo, chain)
                : string.Join('.', chain);

            return ctx.Provider.JsonExtractElement(jsonPath);
        }

        throw new NotSupportedException($"Member expression '{node}' is not supported in inner predicates.");
    }

    static string TranslateInnerMethodCall(
        MethodCallExpression node,
        ParameterExpression elementParam,
        bool isPrimitive,
        JsonTypeInfo? elementTypeInfo,
        TranslationContext ctx)
    {
        // String methods: Contains, StartsWith, EndsWith
        if (node.Object != null && node.Method.DeclaringType == typeof(string))
        {
            var argValue = ExtractValue(node.Arguments[0]);
            var paramName = ctx.AddParameter(argValue);

            var objSql = TranslateInnerPredicate(node.Object, elementParam, isPrimitive, elementTypeInfo, ctx);

            return node.Method.Name switch
            {
                "Contains" => $"({objSql} LIKE '%' || {paramName} || '%')",
                "StartsWith" => $"({objSql} LIKE {paramName} || '%')",
                "EndsWith" => $"({objSql} LIKE '%' || {paramName})",
                _ => throw new NotSupportedException($"String method '{node.Method.Name}' is not supported in inner predicates.")
            };
        }

        throw new NotSupportedException(
            $"Method '{node.Method.Name}' on '{node.Method.DeclaringType?.Name}' is not supported in inner predicates.");
    }

    // ── Helpers ──────────────────────────────────────────────────────

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

    static bool IsParameterAccess(Expression node, ParameterExpression param)
    {
        var current = node;
        while (current is MemberExpression m)
            current = m.Expression;
        return current == param;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "FieldInfo.GetValue on compiler-generated display classes is always preserved when referenced by expression trees.")]
    static bool TryExtractCapturedValue(MemberExpression node, out object? value)
    {
        value = null;
        var chain = new List<MemberInfo>();
        Expression? current = node;

        while (current is MemberExpression memberExpr)
        {
            chain.Add(memberExpr.Member);
            current = memberExpr.Expression;
        }

        if (current is not ConstantExpression constant)
            return false;

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

    static bool IsNullExpression(Expression expr)
    {
        if (expr is ConstantExpression { Value: null })
            return true;

        if (expr is MemberExpression member && TryExtractCapturedValue(member, out var val) && val is null)
            return true;

        return false;
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
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(System.Collections.Generic.IEnumerable<>))
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

    static bool IsIntegerType(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;
        return t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte);
    }

    // ── Context ─────────────────────────────────────────────────────

    sealed class TranslationContext
    {
        int paramIndex;

        public JsonSerializerOptions JsonOptions { get; }
        public JsonTypeInfo SourceTypeInfo { get; }
        public IDatabaseProvider Provider { get; }
        public Dictionary<string, object?> Parameters { get; } = new();

        public TranslationContext(JsonSerializerOptions jsonOptions, JsonTypeInfo sourceTypeInfo, IDatabaseProvider provider)
        {
            this.JsonOptions = jsonOptions;
            this.SourceTypeInfo = sourceTypeInfo;
            this.Provider = provider;
        }

        public string AddParameter(object? value)
        {
            var name = $"@pp{this.paramIndex++}";
            this.Parameters[name] = value;
            return name;
        }
    }
}
