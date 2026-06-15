using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// Compile-free extraction of captured (closure) values and null detection from expression trees.
/// Walks compiler-generated display classes with reflection instead of calling
/// <see cref="Expression{TDelegate}.Compile()"/>, so the recognition path stays AOT/trim-safe.
/// </summary>
static class ClosureValueExtractor
{
    public static Expression StripConvert(Expression expr)
    {
        // Peel every Convert layer — enum/bitwise operations nest them (e.g. an enum `&` yields
        // Convert(Convert(field, Int32) & mask, EnumType)).
        while (expr is UnaryExpression { NodeType: ExpressionType.Convert } u)
            expr = u.Operand;
        return expr;
    }

    public static bool IsNullConstant(Expression expr)
    {
        if (expr is ConstantExpression { Value: null })
            return true;

        // Captured null: a member access that evaluates to null.
        if (expr is MemberExpression member && TryExtractCapturedValue(member, out var val) && val is null)
            return true;

        return false;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "FieldInfo.GetValue on compiler-generated display classes is always preserved when referenced by expression trees.")]
    public static bool TryExtractCapturedValue(MemberExpression node, out object? value)
    {
        value = null;

        // Walk the chain of member accesses down to a ConstantExpression (the closure object).
        var chain = new List<MemberInfo>();
        Expression? current = node;

        while (current is MemberExpression memberExpr)
        {
            chain.Add(memberExpr.Member);
            current = memberExpr.Expression;
        }

        if (current is not ConstantExpression constant)
            return false;

        // If the constant is the lambda parameter root, this isn't a captured variable.
        if (constant.Type.GetCustomAttribute<CompilerGeneratedAttribute>() == null
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
}
