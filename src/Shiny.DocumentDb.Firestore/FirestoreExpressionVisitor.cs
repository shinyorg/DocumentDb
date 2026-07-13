using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Firestore;

internal enum FirestoreOp { Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual }

/// <summary>A single pushable filter clause: <c>path OP value</c> against a Firestore native field.</summary>
internal readonly record struct FirestoreClause(string Path, FirestoreOp Op, object Value);

/// <summary>
/// Best-effort translation of LINQ predicates into Firestore <c>Where</c> clauses. Only the safe conjuncts are
/// extracted — equality and range comparisons on a single string/bool/integral/enum field whose stored
/// representation is unambiguous. Everything else (OR, method calls, floating/date/Guid comparisons, nested
/// disjunctions) is left for the client-side re-apply, so pushdown never changes results — it only shrinks reads.
/// </summary>
internal static class FirestoreExpressionVisitor
{
    public static List<FirestoreClause> Extract<T>(
        IEnumerable<Expression<Func<T, bool>>> predicates,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo<T>? typeInfo) where T : class
    {
        var clauses = new List<FirestoreClause>();
        string? rangeField = null;

        foreach (var predicate in predicates)
            Walk(predicate.Body, predicate.Parameters[0], jsonOptions, typeInfo, clauses, ref rangeField);

        return clauses;
    }

    static void Walk<T>(
        Expression body,
        ParameterExpression parameter,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo<T>? typeInfo,
        List<FirestoreClause> clauses,
        ref string? rangeField) where T : class
    {
        if (body is BinaryExpression { NodeType: ExpressionType.AndAlso } and)
        {
            Walk(and.Left, parameter, jsonOptions, typeInfo, clauses, ref rangeField);
            Walk(and.Right, parameter, jsonOptions, typeInfo, clauses, ref rangeField);
            return;
        }

        if (body is not BinaryExpression cmp)
            return;

        var op = cmp.NodeType switch
        {
            ExpressionType.Equal => (FirestoreOp?)FirestoreOp.Equal,
            ExpressionType.GreaterThan => FirestoreOp.GreaterThan,
            ExpressionType.GreaterThanOrEqual => FirestoreOp.GreaterThanOrEqual,
            ExpressionType.LessThan => FirestoreOp.LessThan,
            ExpressionType.LessThanOrEqual => FirestoreOp.LessThanOrEqual,
            _ => null
        };
        if (op == null)
            return;

        // Identify which side is the member chain rooted at the lambda parameter and which is the value.
        string? path;
        Expression valueExpr;
        var effectiveOp = op.Value;
        if (TryResolvePath(cmp.Left, parameter, jsonOptions, typeInfo, out path))
        {
            valueExpr = cmp.Right;
        }
        else if (TryResolvePath(cmp.Right, parameter, jsonOptions, typeInfo, out path))
        {
            valueExpr = cmp.Left;
            // Flip the operator when the member is on the right (value OP member → member FLIP value).
            effectiveOp = effectiveOp switch
            {
                FirestoreOp.GreaterThan => FirestoreOp.LessThan,
                FirestoreOp.GreaterThanOrEqual => FirestoreOp.LessThanOrEqual,
                FirestoreOp.LessThan => FirestoreOp.GreaterThan,
                FirestoreOp.LessThanOrEqual => FirestoreOp.GreaterThanOrEqual,
                _ => effectiveOp
            };
        }
        else
        {
            return;
        }

        var raw = ExpressionInterpreter.EvaluateClosed(valueExpr);
        if (!TryNormalizeValue(raw, out var value))
            return;

        if (effectiveOp != FirestoreOp.Equal)
        {
            // Firestore permits range/inequality filters on only one field per query — push the first, drop the rest.
            if (rangeField != null && rangeField != path)
                return;
            rangeField = path;
        }

        clauses.Add(new FirestoreClause(path!, effectiveOp, value!));
    }

    static bool TryResolvePath<T>(
        Expression side,
        ParameterExpression parameter,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo<T>? typeInfo,
        out string? path) where T : class
    {
        path = null;

        var expr = side;
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            expr = unary.Operand;

        var chain = new List<string>();
        Expression? current = expr;
        while (current is MemberExpression m)
        {
            chain.Insert(0, m.Member.Name);
            current = m.Expression;
        }

        if (current != parameter || chain.Count == 0)
            return false;

        path = typeInfo != null
            ? JsonPropertyNameResolver.BuildJsonPath(jsonOptions, typeInfo, chain)
            : string.Join('.', chain.Select(c => jsonOptions.PropertyNamingPolicy?.ConvertName(c) ?? c));
        return true;
    }

    // Only push values whose Firestore representation exactly matches the serialized JSON body.
    static bool TryNormalizeValue(object? raw, out object? value)
    {
        value = null;
        switch (raw)
        {
            case null:
                return false; // equality-to-null is unreliable to push; client-side handles it.
            case string:
            case bool:
                value = raw;
                return true;
            case Enum e:
                value = Convert.ToInt64(e);
                return true;
            case byte or sbyte or short or ushort or int or uint or long:
                value = Convert.ToInt64(raw);
                return true;
            default:
                return false; // floating/decimal/Guid/DateTime → not pushed (client-side re-apply).
        }
    }
}
