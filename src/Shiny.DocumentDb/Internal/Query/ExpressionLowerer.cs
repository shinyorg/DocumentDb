using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// Lowers a <c>Where</c> predicate expression tree into the backend-agnostic <see cref="QueryNode"/> IR.
/// This is the shared recognition layer: it resolves JSON paths (root and <c>json_each</c> element),
/// extracts captured values compile-free, and classifies method calls / operators — without emitting any
/// SQL. Emitters consume the resulting <see cref="PredicateNode"/>.
/// </summary>
static class ExpressionLowerer
{
    public static PredicateNode Lower(
        Expression body,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo rootTypeInfo,
        FunctionTranslationRegistry? registry = null,
        IReadOnlyDictionary<string, ComputedMapping>? computed = null)
        => new Lowerer(jsonOptions, rootTypeInfo, registry, computed).LowerPredicate(body, ElementScope.Root);

    /// <summary>Lowers a scalar value expression (e.g. a projected field or function) into a <see cref="ValueNode"/>.</summary>
    public static ValueNode LowerValue(
        Expression body,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo rootTypeInfo,
        IReadOnlyDictionary<string, ComputedMapping>? computed = null)
        => new Lowerer(jsonOptions, rootTypeInfo, null, computed).LowerValue(body, ElementScope.Root);

    /// <summary>Tracks the active <c>json_each</c> element binding while lowering an <c>Any</c>/<c>Count</c> body.</summary>
    readonly record struct ElementScope(ParameterExpression? Param, JsonTypeInfo? ElementTypeInfo, bool IsPrimitive)
    {
        public static readonly ElementScope Root = new(null, null, false);
        public bool InElement => this.Param != null;
    }

    sealed class Lowerer(JsonSerializerOptions jsonOptions, JsonTypeInfo rootTypeInfo, FunctionTranslationRegistry? registry, IReadOnlyDictionary<string, ComputedMapping>? computed = null)
    {
        readonly HashSet<string> expandingComputed = new(StringComparer.Ordinal);

        public PredicateNode LowerPredicate(Expression expr, ElementScope scope)
        {
            switch (expr)
            {
                case BinaryExpression { NodeType: ExpressionType.AndAlso } b:
                    return new LogicalNode(LogicalOp.And, this.LowerPredicate(b.Left, scope), this.LowerPredicate(b.Right, scope));

                case BinaryExpression { NodeType: ExpressionType.OrElse } b:
                    return new LogicalNode(LogicalOp.Or, this.LowerPredicate(b.Left, scope), this.LowerPredicate(b.Right, scope));

                case BinaryExpression b when IsComparison(b.NodeType):
                    return this.LowerComparison(b, scope);

                case UnaryExpression { NodeType: ExpressionType.Not } u:
                    return new NotNode(this.LowerPredicate(u.Operand, scope));

                case UnaryExpression { NodeType: ExpressionType.Convert } u:
                    return this.LowerPredicate(u.Operand, scope);

                case MethodCallExpression m:
                    return this.LowerPredicateMethod(m, scope);

                // Bare bool member / constant used directly as a predicate.
                case MemberExpression or ConstantExpression:
                    return new BoolValueNode(this.LowerValue(expr, scope));

                default:
                    throw new NotSupportedException($"Expression '{expr}' is not supported in a query predicate.");
            }
        }

        PredicateNode LowerComparison(BinaryExpression node, ElementScope scope)
        {
            // null comparisons collapse to a null check on the non-null side.
            if (ClosureValueExtractor.IsNullConstant(node.Right))
                return this.LowerNullCheck(node.Left, node.NodeType == ExpressionType.Equal, scope);
            if (ClosureValueExtractor.IsNullConstant(node.Left))
                return this.LowerNullCheck(node.Right, node.NodeType == ExpressionType.Equal, scope);

            var left = this.LowerValue(node.Left, scope);
            var right = this.LowerValue(node.Right, scope);
            return new CompareNode(ToCompareOp(node.NodeType), left, right);
        }

        PredicateNode LowerNullCheck(Expression target, bool isNull, ElementScope scope)
        {
            var value = this.LowerValue(ClosureValueExtractor.StripConvert(target), scope);
            return value is RootFieldNode root
                ? new NullCheckRootNode(root.JsonPath, isNull)
                : new NullCheckExprNode(value, isNull);
        }

        PredicateNode LowerPredicateMethod(MethodCallExpression node, ElementScope scope)
        {
            // Flag-enum test: enumValue.HasFlag(flag).
            if (node.Object != null && node.Method.Name == "HasFlag" && node.Method.DeclaringType == typeof(Enum))
                return new HasFlagNode(this.LowerValue(node.Object, scope), this.LowerValue(node.Arguments[0], scope));

            // string.IsNullOrEmpty(s) → (s IS NULL OR s = '').
            if (node.Method.DeclaringType == typeof(string) && node.Method.Name == "IsNullOrEmpty")
            {
                var s = this.LowerValue(node.Arguments[0], scope);
                return new LogicalNode(LogicalOp.Or, new NullCheckExprNode(s, true), new CompareNode(CompareOp.Equal, s, new ConstantNode("")));
            }

            // String LIKE methods.
            if (node.Object != null && node.Method.DeclaringType == typeof(string)
                && node.Method.Name is "Contains" or "StartsWith" or "EndsWith")
            {
                var kind = node.Method.Name switch
                {
                    "Contains" => LikeKind.Contains,
                    "StartsWith" => LikeKind.StartsWith,
                    _ => LikeKind.EndsWith
                };
                var target = this.LowerValue(node.Object, scope);
                var arg = ExtractValue(node.Arguments[0]);
                return new LikeNode(target, arg, kind);
            }

            // Enumerable.Contains(source, item) / collection.Contains(item) → IN (...).
            if (node.Method.Name == "Contains" && TryGetInOperands(node, out var inCollection, out var inItem))
                return this.LowerIn(inCollection, inItem, scope);

            // Enumerable.Any(collection[, predicate]).
            if (node.Method.Name == "Any" && node.Method.DeclaringType == typeof(Enumerable))
            {
                var collectionPath = this.ResolveCollectionJsonPath(node.Arguments[0], scope);
                if (node.Arguments.Count == 1)
                    return new AnyNode(collectionPath, null);

                var (lambda, childScope) = this.OpenElementScope(node.Arguments[0], node.Arguments[1]);
                return new AnyNode(collectionPath, this.LowerPredicate(lambda.Body, childScope));
            }

            throw new NotSupportedException($"Method '{node.Method.Name}' on '{node.Method.DeclaringType?.Name}' is not supported.");
        }

        PredicateNode LowerIn(Expression collectionExpr, Expression itemExpr, ElementScope scope)
        {
            if (ExtractValue(collectionExpr) is not System.Collections.IEnumerable enumerable || enumerable is string)
                throw new NotSupportedException($"'Contains' requires a constant collection of values, found '{collectionExpr}'.");

            var items = new List<object?>();
            foreach (var item in enumerable)
                items.Add(item);

            return new InNode(this.LowerValue(itemExpr, scope), items);
        }

        public ValueNode LowerValue(Expression expr, ElementScope scope)
        {
            expr = ClosureValueExtractor.StripConvert(expr);

            switch (expr)
            {
                case ConstantExpression c:
                    return new ConstantNode(c.Value);

                case MemberExpression member:
                    return this.LowerMember(member, scope);

                case ParameterExpression p when scope.InElement && p == scope.Param && scope.IsPrimitive:
                    return new ElementValueNode();

                case MethodCallExpression m:
                    return this.LowerValueMethod(m, scope);

                case BinaryExpression { NodeType: ExpressionType.And } b:
                    return new BitAndNode(this.LowerValue(b.Left, scope), this.LowerValue(b.Right, scope));

                case BinaryExpression { NodeType: ExpressionType.Add } b when b.Type == typeof(string):
                    return new ScalarFnNode(ScalarFn.Concat, [this.LowerValue(b.Left, scope), this.LowerValue(b.Right, scope)], typeof(string));

                case BinaryExpression b when TryArithOp(b.NodeType, out var arith):
                    return new ArithmeticNode(arith, this.LowerValue(b.Left, scope), this.LowerValue(b.Right, scope), b.Type);

                default:
                    throw new NotSupportedException($"Expression '{expr}' is not supported as a query value.");
            }
        }

        ValueNode LowerMember(MemberExpression node, ElementScope scope)
        {
            // Captured variable (closure access) — bind as a parameter.
            if (ClosureValueExtractor.TryExtractCapturedValue(node, out var captured))
                return new ConstantNode(captured);

            // string.Length → LENGTH(...).
            if (node.Member.Name == "Length" && node.Expression?.Type == typeof(string))
                return new ScalarFnNode(ScalarFn.Length, [this.LowerValue(node.Expression, scope)], typeof(int));

            // DateTime / DateTimeOffset component access → date-part functions.
            if (node.Expression != null)
            {
                var exprType = Nullable.GetUnderlyingType(node.Expression.Type) ?? node.Expression.Type;
                if (exprType == typeof(DateTime) || exprType == typeof(DateTimeOffset))
                {
                    ScalarFn? part = node.Member.Name switch
                    {
                        "Year" => ScalarFn.Year,
                        "Month" => ScalarFn.Month,
                        "Day" => ScalarFn.Day,
                        "Hour" => ScalarFn.Hour,
                        "Minute" => ScalarFn.Minute,
                        "Second" => ScalarFn.Second,
                        _ => null
                    };
                    if (part != null)
                        return new ScalarFnNode(part.Value, [this.LowerValue(node.Expression, scope)], typeof(int));
                }
            }

            // Element property inside a json_each body.
            if (scope.InElement && IsParameterAccess(node, scope.Param!))
            {
                if (scope.IsPrimitive)
                    return new ElementValueNode();

                var chain = BuildMemberChain(node, scope.Param!);
                var jsonPath = scope.ElementTypeInfo != null
                    ? JsonPropertyNameResolver.BuildJsonPath(jsonOptions, scope.ElementTypeInfo, chain)
                    : string.Join('.', chain);
                return new ElementFieldNode(jsonPath, node.Type);
            }

            // Collection size: x.Items.Count / x.Items.Length → array length.
            var sizeKind = CollectionMember.Classify(node, out var sizeCollection);
            if (sizeKind == CollectionSizeKind.ArrayLength)
            {
                var sizeChain = BuildMemberChainFromRoot(sizeCollection);
                if (sizeChain != null)
                    return new ArrayLengthNode(JsonPropertyNameResolver.BuildJsonPath(jsonOptions, rootTypeInfo, sizeChain));
            }
            else if (sizeKind == CollectionSizeKind.Unsupported)
            {
                throw new NotSupportedException(
                    $"'{node.Member.DeclaringType?.Name}.{node.Member.Name}' is not supported in queries. " +
                    "Use '.Count()' or '.Any()' for collection length.");
            }

            // Root document property — or a computed property, whose definition is lowered inline.
            var rootChain = BuildMemberChainFromRoot(node);
            if (rootChain != null)
            {
                if (computed != null && rootChain.Count == 1 && computed.TryGetValue(rootChain[0], out var mapping))
                    return this.LowerComputed(rootChain[0], mapping, scope);

                return new RootFieldNode(JsonPropertyNameResolver.BuildJsonPath(jsonOptions, rootTypeInfo, rootChain), node.Type);
            }

            throw new NotSupportedException($"Member expression '{node}' is not supported.");
        }

        ValueNode LowerComputed(string name, ComputedMapping mapping, ElementScope scope)
        {
            // Materialized → reference the real column; otherwise inline the definition (alias mode).
            if (mapping.MaterializedColumn != null)
                return new ComputedColumnNode(mapping.MaterializedColumn, mapping.ResultType);

            if (!this.expandingComputed.Add(name))
                throw new NotSupportedException($"Computed property '{name}' on '{mapping.DocumentType.Name}' references itself.");
            try
            {
                return this.LowerValue(mapping.Definition.Body, scope);
            }
            finally
            {
                this.expandingComputed.Remove(name);
            }
        }

        ValueNode LowerValueMethod(MethodCallExpression node, ElementScope scope)
        {
            // Library functions with no BCL equivalent (DocumentFunctions.*).
            if (node.Method.DeclaringType == typeof(DocumentFunctions))
            {
                var fn = node.Method.Name switch
                {
                    nameof(DocumentFunctions.Soundex) => ScalarFn.Soundex,
                    _ => throw new NotSupportedException($"DocumentFunctions.{node.Method.Name} is not translatable.")
                };
                return new ScalarFnNode(fn, [.. node.Arguments.Select(a => this.LowerValue(a, scope))], node.Method.ReturnType);
            }

            // String value-producing methods.
            if (node.Object != null && node.Method.DeclaringType == typeof(string))
            {
                var target = this.LowerValue(node.Object, scope);
                switch (node.Method.Name)
                {
                    case "ToLower" or "ToLowerInvariant":
                        return new ScalarFnNode(ScalarFn.Lower, [target], typeof(string));
                    case "ToUpper" or "ToUpperInvariant":
                        return new ScalarFnNode(ScalarFn.Upper, [target], typeof(string));
                    case "Trim" when node.Arguments.Count == 0:
                        return new ScalarFnNode(ScalarFn.Trim, [target], typeof(string));
                    case "TrimStart" when node.Arguments.Count == 0:
                        return new ScalarFnNode(ScalarFn.TrimStart, [target], typeof(string));
                    case "TrimEnd" when node.Arguments.Count == 0:
                        return new ScalarFnNode(ScalarFn.TrimEnd, [target], typeof(string));
                    case "Substring":
                        return new ScalarFnNode(ScalarFn.Substring, this.WithArgs(target, node.Arguments, scope), typeof(string));
                    case "Replace" when node.Arguments.Count == 2:
                        return new ScalarFnNode(ScalarFn.Replace, this.WithArgs(target, node.Arguments, scope), typeof(string));
                    case "IndexOf" when node.Arguments.Count == 1:
                        return new ScalarFnNode(ScalarFn.IndexOf, this.WithArgs(target, node.Arguments, scope), typeof(int));
                    default:
                        throw new NotSupportedException($"String method '{node.Method.Name}' is not supported in a query value.");
                }
            }

            // Math.* functions.
            if (node.Method.DeclaringType == typeof(Math))
            {
                var fn = node.Method.Name switch
                {
                    "Abs" => ScalarFn.Abs,
                    "Ceiling" => ScalarFn.Ceiling,
                    "Floor" => ScalarFn.Floor,
                    "Round" => ScalarFn.Round,
                    "Sqrt" => ScalarFn.Sqrt,
                    "Pow" => ScalarFn.Pow,
                    "Sign" => ScalarFn.Sign,
                    _ => throw new NotSupportedException($"Math.{node.Method.Name} is not supported in a query.")
                };
                return new ScalarFnNode(fn, [.. node.Arguments.Select(a => this.LowerValue(a, scope))], node.Method.ReturnType);
            }

            if (node.Method.Name == "Count" && node.Method.DeclaringType == typeof(Enumerable))
            {
                var collectionPath = this.ResolveCollectionJsonPath(node.Arguments[0], scope);
                if (node.Arguments.Count == 1)
                    return new ArrayLengthNode(collectionPath);

                var (lambda, childScope) = this.OpenElementScope(node.Arguments[0], node.Arguments[1]);
                return new CountSubqueryNode(collectionPath, this.LowerPredicate(lambda.Body, childScope));
            }

            // User-registered custom translation (MapFunctionTranslation).
            if (registry != null && registry.TryGet(node.Method, out var sqlName))
                return new CustomFnNode(sqlName, [.. node.Arguments.Select(a => this.LowerValue(a, scope))]);

            throw new NotSupportedException($"Method '{node.Method.Name}' on '{node.Method.DeclaringType?.Name}' is not supported as a query value.");
        }

        ValueNode[] WithArgs(ValueNode target, IReadOnlyList<Expression> args, ElementScope scope)
        {
            var result = new ValueNode[args.Count + 1];
            result[0] = target;
            for (var i = 0; i < args.Count; i++)
                result[i + 1] = this.LowerValue(args[i], scope);
            return result;
        }

        (LambdaExpression Lambda, ElementScope Scope) OpenElementScope(Expression collectionExpr, Expression lambdaArg)
        {
            var lambda = (LambdaExpression)StripQuotes(lambdaArg);
            var elementType = GetCollectionElementType(collectionExpr.Type);
            var isPrimitive = IsPrimitiveType(elementType);
            var elementTypeInfo = isPrimitive ? null : jsonOptions.GetTypeInfo(elementType);
            return (lambda, new ElementScope(lambda.Parameters[0], elementTypeInfo, isPrimitive));
        }

        string ResolveCollectionJsonPath(Expression collectionExpr, ElementScope scope)
        {
            // Collections referenced inside an element body are not supported (no nested-root context),
            // matching the original visitor; resolve against the root document.
            var chain = BuildMemberChainFromRoot(collectionExpr);
            if (chain == null)
                throw new NotSupportedException($"Collection expression '{collectionExpr}' is not supported.");
            return JsonPropertyNameResolver.BuildJsonPath(jsonOptions, rootTypeInfo, chain);
        }
    }

    // ── Static helpers (ported from JsonExpressionVisitor) ──────────────────

    static bool TryArithOp(ExpressionType t, out ArithOp op)
    {
        switch (t)
        {
            case ExpressionType.Add or ExpressionType.AddChecked: op = ArithOp.Add; return true;
            case ExpressionType.Subtract or ExpressionType.SubtractChecked: op = ArithOp.Subtract; return true;
            case ExpressionType.Multiply or ExpressionType.MultiplyChecked: op = ArithOp.Multiply; return true;
            case ExpressionType.Divide: op = ArithOp.Divide; return true;
            default: op = default; return false;
        }
    }

    static bool IsComparison(ExpressionType t) => t is ExpressionType.Equal or ExpressionType.NotEqual
        or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
        or ExpressionType.LessThan or ExpressionType.LessThanOrEqual;

    static CompareOp ToCompareOp(ExpressionType t) => t switch
    {
        ExpressionType.Equal => CompareOp.Equal,
        ExpressionType.NotEqual => CompareOp.NotEqual,
        ExpressionType.GreaterThan => CompareOp.GreaterThan,
        ExpressionType.GreaterThanOrEqual => CompareOp.GreaterThanOrEqual,
        ExpressionType.LessThan => CompareOp.LessThan,
        ExpressionType.LessThanOrEqual => CompareOp.LessThanOrEqual,
        _ => throw new NotSupportedException($"Binary operator '{t}' is not supported.")
    };

    static bool TryGetInOperands(MethodCallExpression node, out Expression collection, out Expression item)
    {
        if (node.Object == null && node.Arguments.Count == 2)
        {
            collection = node.Arguments[0];
            item = node.Arguments[1];
            return true;
        }

        if (node.Object != null && node.Arguments.Count == 1)
        {
            collection = node.Object;
            item = node.Arguments[0];
            return true;
        }

        collection = null!;
        item = null!;
        return false;
    }

    static object? ExtractValue(Expression expr)
    {
        if (expr is ConstantExpression c)
            return c.Value;
        if (expr is MemberExpression m && ClosureValueExtractor.TryExtractCapturedValue(m, out var val))
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

    static List<string>? BuildMemberChainFromRoot(Expression node)
    {
        var chain = new List<string>();
        var current = node;
        while (current is MemberExpression m)
        {
            chain.Insert(0, m.Member.Name);
            current = m.Expression;
        }
        return current is ParameterExpression ? chain : null;
    }

    static Expression StripQuotes(Expression expr)
        => expr is UnaryExpression { NodeType: ExpressionType.Quote } u ? u.Operand : expr;

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
