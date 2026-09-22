using System.Linq.Expressions;
using System.Reflection;
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
/// representation is unambiguous, plus comparisons on the <see cref="DocumentMetadata"/> timestamps, which target the
/// <c>_meta</c> envelope fields. Everything else (OR, method calls, floating/date/Guid comparisons, nested
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
        bool envelope;
        Expression valueExpr;
        var effectiveOp = op.Value;
        if (TryResolveSide(cmp.Left, parameter, jsonOptions, typeInfo, out path, out envelope))
        {
            valueExpr = cmp.Right;
        }
        else if (TryResolveSide(cmp.Right, parameter, jsonOptions, typeInfo, out path, out envelope))
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
        var normalized = envelope ? TryNormalizeTimestamp(raw, out var value) : TryNormalizeValue(raw, out value);
        if (!normalized)
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

    static bool TryResolveSide<T>(
        Expression side,
        ParameterExpression parameter,
        JsonSerializerOptions jsonOptions,
        JsonTypeInfo<T>? typeInfo,
        out string? path,
        out bool envelope) where T : class
    {
        path = EnvelopePath(side, parameter);
        envelope = path != null;
        return envelope || TryResolvePath(side, parameter, jsonOptions, typeInfo, out path);
    }

    /// <summary>
    /// The <c>_meta</c> field a <see cref="DocumentMetadata"/> member (<c>x.Metadata.CreatedAt</c> / <c>UpdatedAt</c>)
    /// reads from, or null when <paramref name="side"/> is anything else. The metadata never lives in the body, so it
    /// must never resolve to a body path.
    /// </summary>
    public static string? EnvelopePath(Expression side, ParameterExpression parameter)
    {
        if (side is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            side = unary.Operand;

        if (side is not MemberExpression { Expression: MemberExpression owner } leaf
            || owner.Type != typeof(DocumentMetadata)
            || owner.Expression != parameter)
            return null;

        return leaf.Member.Name switch
        {
            nameof(DocumentMetadata.CreatedAt) => FirestoreDocument.MetaCreatedAtPath,
            nameof(DocumentMetadata.UpdatedAt) => FirestoreDocument.MetaUpdatedAtPath,
            _ => null
        };
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
        var stored = true;
        while (current is MemberExpression m)
        {
            stored &= IsStoredMember(m.Member);
            chain.Insert(0, m.Member.Name);
            current = m.Expression;
        }

        if (current != parameter || chain.Count == 0 || !stored)
            return false;

        path = typeInfo != null
            ? JsonPropertyNameResolver.BuildJsonPath(jsonOptions, typeInfo, chain)
            : string.Join('.', chain.Select(c => jsonOptions.PropertyNamingPolicy?.ConvertName(c) ?? c));
        return true;
    }

    // A field path only exists for members of the stored object graph. A member of a value — string.Length,
    // DateTimeOffset.Year, List.Count, Nullable.Value — or of the envelope-backed DocumentMetadata is a computation the
    // body doesn't hold, so pushing it as "name.Length" would match nothing; those stay client-side.
    static bool IsStoredMember(MemberInfo member)
    {
        var owner = member.DeclaringType;
        return owner != null
            && !owner.IsPrimitive
            && owner != typeof(string)
            && owner != typeof(decimal)
            && owner != typeof(DateTime)
            && owner != typeof(DateTimeOffset)
            && owner != typeof(DateOnly)
            && owner != typeof(TimeOnly)
            && owner != typeof(TimeSpan)
            && owner != typeof(Guid)
            && owner != typeof(DocumentMetadata)
            && !(owner.IsGenericType && owner.GetGenericTypeDefinition() == typeof(Nullable<>))
            && !typeof(System.Collections.IEnumerable).IsAssignableFrom(owner);
    }

    // Envelope timestamps are stored as fixed-width UTC ISO-8601 text, so the comparand is rendered the same way and
    // the string comparison Firestore runs is an instant comparison.
    static bool TryNormalizeTimestamp(object? raw, out object? value)
    {
        value = MetadataSupport.FromValue(raw) is { } instant
            ? FirestoreDocument.FormatTimestamp(instant.UtcDateTime)
            : null;
        return value != null;
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
