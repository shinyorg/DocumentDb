using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Rewrites predicates over encrypted properties before they reach a provider:
/// <c>x.Email == "a@b.com"</c> becomes <c>x.Email == "enc:1:k1:…"</c>, which is what is actually stored.
/// Anything that cannot be answered against ciphertext is rejected here, with an explanation, rather than
/// silently matching nothing.
/// </summary>
/// <remarks>
/// Registered through <see cref="DocumentPredicateRewriters"/>, so it covers the typed LINQ <c>Where</c>, the
/// string grammar, the interpolated form, OData and the AI tools — they all lower to the same predicate list.
/// </remarks>
sealed class EncryptedPredicateRewriter<T> : ExpressionVisitor where T : class
{
    protected override Expression VisitBinary(BinaryExpression node)
    {
        var left = TryGetEncryptedMember(node.Left);
        var right = TryGetEncryptedMember(node.Right);

        if (left == null && right == null)
            return base.VisitBinary(node);

        if (left != null && right != null)
            throw Unsupported(left, "comparing two encrypted properties");

        var property = left ?? right!;
        var valueSide = left != null ? node.Right : node.Left;

        if (node.NodeType is not (ExpressionType.Equal or ExpressionType.NotEqual))
            throw Unsupported(property,
                $"the '{node.NodeType}' comparison — ciphertext does not preserve order, so ranges cannot be evaluated");

        // Comparisons against null still work: a null property is stored as JSON null, not as an envelope.
        var value = ExpressionInterpreter.EvaluateClosed(valueSide);
        if (value == null)
            return base.VisitBinary(node);

        if (property.Mode == EncryptionMode.Randomized)
            throw Unsupported(property,
                "an equality comparison — randomized encryption writes different ciphertext every time, so no stored value " +
                "would ever match. Map the property with EncryptionMode.Deterministic if it has to be queryable");

        if (value is not string text)
            throw Unsupported(property, $"a comparison against a {value.GetType().Name}; deterministic encryption is string-only");

        var ciphertext = Expression.Constant(
            property.Encryptor.Encrypt(Encoding.UTF8.GetBytes(text), EncryptionMode.Deterministic),
            typeof(string));

        return left != null
            ? node.Update(node.Left, node.Conversion, ciphertext)
            : node.Update(ciphertext, node.Conversion, node.Right);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var target = TryGetEncryptedMember(node.Object);
        if (target == null)
        {
            foreach (var argument in node.Arguments)
            {
                target = TryGetEncryptedMember(argument);
                if (target != null)
                    break;
            }
        }

        if (target != null)
            throw Unsupported(target,
                $"'{node.Method.Name}' — only equality against a deterministic property can be answered against ciphertext");

        return base.VisitMethodCall(node);
    }

    static EncryptedProperty? TryGetEncryptedMember(Expression? expression)
    {
        if (expression == null)
            return null;

        var node = expression;
        if (node is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            node = unary.Operand;

        return node is MemberExpression { Member: PropertyInfo info, Expression: ParameterExpression parameter }
            && parameter.Type == typeof(T)
            && EncryptionRegistry.TryGet(typeof(T), info.Name, out var property)
                ? property
                : null;
    }

    static NotSupportedException Unsupported(EncryptedProperty property, string what)
        => new($"'{property.DeclaringType.Name}.{property.ClrName}' is encrypted at rest, so the store cannot evaluate {what}. " +
               "Read the documents you need and filter in memory, or keep an unencrypted derived property to query on.");
}
