using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// Evaluates a LINQ expression tree against an input value by walking the tree with reflection — never
/// calling <see cref="Expression{TDelegate}.Compile()"/>. This keeps the in-memory providers (LiteDB,
/// IndexedDB) and client-side filter paths free of <c>RequiresDynamicCode</c> (IL3050): there is no
/// runtime IL generation, only reflection invocation of members already resolved in the expression tree.
/// </summary>
static class ExpressionInterpreter
{
    /// <summary>Builds a compile-free delegate equivalent to <paramref name="expr"/>.Compile().</summary>
    public static Func<T, TResult> Interpret<T, TResult>(Expression<Func<T, TResult>> expr)
    {
        var param = expr.Parameters[0];
        var body = expr.Body;
        return arg =>
        {
            var result = Evaluate(body, param, arg);
            return result is null ? default! : (TResult)ChangeType(result, typeof(TResult));
        };
    }

    /// <summary>Evaluates a closed (parameter-free) expression — e.g. a captured value or computed
    /// constant — without compiling. Replaces <c>Expression.Lambda(expr).Compile().DynamicInvoke()</c>.</summary>
    public static object? EvaluateClosed(Expression expr) => Evaluate(expr, null, null);

    static object? Evaluate(Expression node, ParameterExpression? param, object? arg) => node switch
    {
        ParameterExpression p when p == param => arg,
        ConstantExpression c => c.Value,
        MemberExpression m => EvaluateMember(m, param, arg),
        UnaryExpression u => EvaluateUnary(u, param, arg),
        BinaryExpression b => EvaluateBinary(b, param, arg),
        MethodCallExpression mc => EvaluateCall(mc, param, arg),
        ConditionalExpression cond => (bool)Evaluate(cond.Test, param, arg)!
            ? Evaluate(cond.IfTrue, param, arg)
            : Evaluate(cond.IfFalse, param, arg),
        NewExpression n => EvaluateNew(n, param, arg),
        MemberInitExpression mi => EvaluateMemberInit(mi, param, arg),
        _ => throw new NotSupportedException($"Expression '{node.NodeType}' is not supported by the in-memory interpreter.")
    };

    [UnconditionalSuppressMessage("Trimming", "IL2072", Justification = "Members come from expression trees over preserved model types.")]
    static object? EvaluateMemberInit(MemberInitExpression mi, ParameterExpression? param, object? arg)
    {
        var obj = EvaluateNew(mi.NewExpression, param, arg);
        foreach (var binding in mi.Bindings)
        {
            if (binding is not MemberAssignment ma)
                throw new NotSupportedException($"Member binding '{binding.BindingType}' is not supported by the in-memory interpreter.");

            var value = Evaluate(ma.Expression, param, arg);
            switch (ma.Member)
            {
                case PropertyInfo pi: pi.SetValue(obj, Coerce(value, pi.PropertyType)); break;
                case FieldInfo fi: fi.SetValue(obj, Coerce(value, fi.FieldType)); break;
                default: throw new NotSupportedException($"Member '{ma.Member.Name}' is not assignable.");
            }
        }
        return obj;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Members come from expression trees over preserved model types.")]
    static object? EvaluateMember(MemberExpression m, ParameterExpression? param, object? arg)
    {
        var target = m.Expression == null ? null : Evaluate(m.Expression, param, arg);
        return m.Member switch
        {
            PropertyInfo pi => pi.GetValue(target),
            FieldInfo fi => fi.GetValue(target),
            _ => throw new NotSupportedException($"Member '{m.Member.Name}' is not supported.")
        };
    }

    static object? EvaluateUnary(UnaryExpression u, ParameterExpression? param, object? arg)
    {
        if (u.NodeType == ExpressionType.Quote)
            return ((LambdaExpression)u.Operand);

        var operand = Evaluate(u.Operand, param, arg);
        return u.NodeType switch
        {
            ExpressionType.Convert or ExpressionType.ConvertChecked => operand is null ? null : ChangeType(operand, Nullable.GetUnderlyingType(u.Type) ?? u.Type),
            ExpressionType.Not => operand is bool b ? !b : ~Convert.ToInt64(operand),
            ExpressionType.Negate or ExpressionType.NegateChecked => -Convert.ToDouble(operand),
            ExpressionType.TypeAs => u.Type.IsInstanceOfType(operand) ? operand : null,
            _ => throw new NotSupportedException($"Unary '{u.NodeType}' is not supported.")
        };
    }

    static object? EvaluateBinary(BinaryExpression b, ParameterExpression? param, object? arg)
    {
        // Short-circuit logical operators.
        if (b.NodeType == ExpressionType.AndAlso)
            return (bool)Evaluate(b.Left, param, arg)! && (bool)Evaluate(b.Right, param, arg)!;
        if (b.NodeType == ExpressionType.OrElse)
            return (bool)Evaluate(b.Left, param, arg)! || (bool)Evaluate(b.Right, param, arg)!;

        var left = Evaluate(b.Left, param, arg);
        var right = Evaluate(b.Right, param, arg);

        switch (b.NodeType)
        {
            case ExpressionType.Equal: return AreEqual(left, right);
            case ExpressionType.NotEqual: return !AreEqual(left, right);
            case ExpressionType.Coalesce: return left ?? right;
        }

        if (b.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
            throw new NotSupportedException();

        // Arithmetic / bitwise / relational on numerics.
        if (b.NodeType is ExpressionType.And or ExpressionType.Or or ExpressionType.ExclusiveOr)
        {
            var l = Convert.ToInt64(left);
            var r = Convert.ToInt64(right);
            return b.NodeType switch
            {
                ExpressionType.And => l & r,
                ExpressionType.Or => l | r,
                _ => l ^ r
            };
        }

        if (left is string || right is string)
        {
            if (b.NodeType == ExpressionType.Add)
                return string.Concat(left, right);
        }

        var dl = Convert.ToDouble(left);
        var dr = Convert.ToDouble(right);
        return b.NodeType switch
        {
            ExpressionType.GreaterThan => dl > dr,
            ExpressionType.GreaterThanOrEqual => dl >= dr,
            ExpressionType.LessThan => dl < dr,
            ExpressionType.LessThanOrEqual => dl <= dr,
            ExpressionType.Add => dl + dr,
            ExpressionType.Subtract => dl - dr,
            ExpressionType.Multiply => dl * dr,
            ExpressionType.Divide => dl / dr,
            ExpressionType.Modulo => dl % dr,
            _ => throw new NotSupportedException($"Binary '{b.NodeType}' is not supported.")
        };
    }

    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Methods come from expression trees over preserved model types.")]
    static object? EvaluateCall(MethodCallExpression mc, ParameterExpression? param, object? arg)
    {
        // Grouped aggregate markers (g.Count()/g.Sum(x => x.Total)/…) — compute over the materialized
        // group's members instead of invoking the throwing marker method.
        if (mc.Method.DeclaringType == typeof(Sql) && Evaluate(mc.Arguments[0], param, arg) is IMaterializedGroup<object> group)
            return EvaluateGroupAggregate(mc, group.Members, param, arg);

        var target = mc.Object == null ? null : Evaluate(mc.Object, param, arg);

        // Enumerable methods with a lambda (predicate/selector) — iterate and interpret the lambda per
        // element instead of building a delegate, which would need Expression.Compile() or MakeGenericMethod.
        if (mc.Method.DeclaringType == typeof(Enumerable) && mc.Arguments.Count == 2
            && Unquote(mc.Arguments[1]) is LambdaExpression lambda)
        {
            var source = (Evaluate(mc.Arguments[0], param, arg) as IEnumerable ?? Array.Empty<object?>()).Cast<object?>();
            var p = lambda.Parameters[0];
            bool Pred(object? e) => (bool)Evaluate(lambda.Body, p, e)!;
            return mc.Method.Name switch
            {
                "Any" => source.Any(Pred),
                "All" => source.All(Pred),
                "Count" => source.Count(Pred),
                "First" => source.First(Pred),
                "FirstOrDefault" => source.FirstOrDefault(Pred),
                "Single" => source.Single(Pred),
                "SingleOrDefault" => source.SingleOrDefault(Pred),
                "Where" => source.Where(Pred).ToList(),
                "Select" => source.Select(e => Evaluate(lambda.Body, p, e)).ToList(),
                _ => throw new NotSupportedException($"Enumerable.{mc.Method.Name} with a lambda is not supported by the in-memory interpreter.")
            };
        }

        var args = new object?[mc.Arguments.Count];
        for (var i = 0; i < mc.Arguments.Count; i++)
            args[i] = Evaluate(mc.Arguments[i], param, arg);

        return mc.Method.Invoke(target, args);
    }

    static object? EvaluateGroupAggregate(MethodCallExpression mc, IReadOnlyList<object> members, ParameterExpression? param, object? arg)
    {
        if (mc.Method.Name == "Count")
            return members.Count;

        var lambda = (LambdaExpression)Unquote(mc.Arguments[^1]);
        var lp = lambda.Parameters[0];
        var values = members.Select(m => Evaluate(lambda.Body, lp, m)).Where(v => v != null).ToList();

        switch (mc.Method.Name)
        {
            case "Avg":
                return values.Count == 0 ? 0d : values.Average(v => Convert.ToDouble(v));
            case "Sum":
                if (values.Count == 0) return 0m;
                return values.Any(v => v is double or float)
                    ? values.Sum(v => Convert.ToDouble(v))
                    : values.Aggregate(0m, (a, v) => a + Convert.ToDecimal(v));
            case "Min":
                // Comparable-based (not numeric-coerced) so Min/Max work over dates and strings too, matching
                // the relational MIN/MAX over a typed column.
                return values.Count == 0 ? null : values.Min();
            case "Max":
                return values.Count == 0 ? null : values.Max();
            default:
                throw new NotSupportedException($"Sql.{mc.Method.Name} is not supported by the in-memory interpreter.");
        }
    }

    static Expression Unquote(Expression e) => e is UnaryExpression { NodeType: ExpressionType.Quote } u ? u.Operand : e;

    [UnconditionalSuppressMessage("Trimming", "IL2072", Justification = "Constructor and member types come from expression trees over preserved model types.")]
    static object? EvaluateNew(NewExpression n, ParameterExpression? param, object? arg)
    {
        var args = new object?[n.Arguments.Count];
        for (var i = 0; i < n.Arguments.Count; i++)
            args[i] = Evaluate(n.Arguments[i], param, arg);
        return n.Constructor!.Invoke(args);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2067", Justification = "Target types come from expression trees over preserved model types.")]
    static object? Coerce(object? value, Type target)
    {
        if (value == null || target.IsInstanceOfType(value))
            return value;
        var underlying = Nullable.GetUnderlyingType(target) ?? target;
        if (underlying.IsEnum)
            return Enum.ToObject(underlying, value);
        return value is IConvertible ? Convert.ChangeType(value, underlying) : value;
    }

    static bool AreEqual(object? a, object? b)
    {
        if (a is null || b is null)
            return a is null && b is null;
        if (a.GetType() != b.GetType() && a is IConvertible && b is IConvertible)
            return Equals(Convert.ToDouble(a), Convert.ToDouble(b));
        return Equals(a, b);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2067", Justification = "Target types come from expression trees over preserved model types.")]
    static object ChangeType(object value, Type target)
    {
        if (target == typeof(object) || target.IsInstanceOfType(value))
            return value;
        if (target.IsEnum)
            return Enum.ToObject(target, value);
        return value is IConvertible ? Convert.ChangeType(value, target) : value;
    }
}
