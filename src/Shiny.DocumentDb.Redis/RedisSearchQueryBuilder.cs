using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Shiny.DocumentDb.Redis;

/// <summary>
/// Translates a LINQ predicate into a RediSearch query string for <c>FT.SEARCH</c> pushdown. Only the
/// AND-conjuncts that target declared TAG/NUMERIC index fields with a safely-encodable constant are emitted,
/// so the produced query is always a <b>superset</b> of the real predicate — the caller re-applies the full
/// predicate client-side. Anything untranslatable is silently dropped (a broader candidate set, never a
/// wrong one); when nothing is translatable the builder returns <c>null</c> and the caller falls back to a
/// full key scan.
/// </summary>
internal static class RedisSearchQueryBuilder
{
    public static string? BuildPushdown<T>(IReadOnlyList<Expression<Func<T, bool>>> predicates, IReadOnlyList<RedisIndexField> fields) where T : class
    {
        if (fields.Count == 0)
            return null;
        var byClrPath = fields
            .Where(f => f.Kind is RedisFieldKind.Tag or RedisFieldKind.Numeric)
            .GroupBy(f => f.ClrPath)
            .ToDictionary(g => g.Key, g => g.First());
        if (byClrPath.Count == 0)
            return null;

        var clauses = new List<string>();
        foreach (var predicate in predicates)
            Walk(predicate.Body, predicate.Parameters[0], byClrPath, clauses);

        return clauses.Count == 0 ? null : string.Join(' ', clauses);
    }

    static void Walk(Expression body, ParameterExpression param, IReadOnlyDictionary<string, RedisIndexField> byClrPath, List<string> acc)
    {
        if (body is BinaryExpression { NodeType: ExpressionType.AndAlso } and)
        {
            Walk(and.Left, param, byClrPath, acc);
            Walk(and.Right, param, byClrPath, acc);
            return;
        }

        if (body is BinaryExpression cmp && TryOp(cmp.NodeType, out var op))
        {
            if (TryClause(cmp.Left, cmp.Right, op, param, byClrPath, out var clause) ||
                TryClause(cmp.Right, cmp.Left, Flip(op), param, byClrPath, out clause))
                acc.Add(clause!);
        }
    }

    static bool TryClause(Expression memberSide, Expression valueSide, ExpressionType op, ParameterExpression param, IReadOnlyDictionary<string, RedisIndexField> byClrPath, out string? clause)
    {
        clause = null;
        var path = MemberPathOrNull(memberSide, param);
        if (path == null || !byClrPath.TryGetValue(path, out var field))
            return false;
        if (!TryEval(valueSide, out var value) || value == null)
            return false;

        if (field.Kind == RedisFieldKind.Tag)
        {
            // Only string/Guid tags are safe to encode; enums/bools serialize differently in the stored JSON.
            if (value is not string && value is not Guid)
                return false;
            var tag = EscapeTag(value.ToString()!);
            clause = op switch
            {
                ExpressionType.Equal => $"@{field.Alias}:{{{tag}}}",
                ExpressionType.NotEqual => $"-@{field.Alias}:{{{tag}}}",
                _ => null
            };
            return clause != null;
        }

        // Numeric field — only push numeric constants.
        if (!TryNumeric(value, out var n))
            return false;
        var v = n.ToString(CultureInfo.InvariantCulture);
        clause = op switch
        {
            ExpressionType.Equal => $"@{field.Alias}:[{v} {v}]",
            ExpressionType.GreaterThan => $"@{field.Alias}:[({v} +inf]",
            ExpressionType.GreaterThanOrEqual => $"@{field.Alias}:[{v} +inf]",
            ExpressionType.LessThan => $"@{field.Alias}:[-inf ({v}]",
            ExpressionType.LessThanOrEqual => $"@{field.Alias}:[-inf {v}]",
            _ => null
        };
        return clause != null;
    }

    static bool TryNumeric(object value, out double result)
    {
        switch (value)
        {
            case int i: result = i; return true;
            case long l: result = l; return true;
            case short s: result = s; return true;
            case byte b: result = b; return true;
            case double d: result = d; return true;
            case float f: result = f; return true;
            case decimal m: result = (double)m; return true;
            default: result = 0; return false;
        }
    }

    public static string[] MemberPath<T>(Expression<Func<T, object>> selector)
    {
        var body = selector.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } u)
            body = u.Operand;
        var segs = new List<string>();
        while (body is MemberExpression m)
        {
            segs.Add(m.Member.Name);
            body = m.Expression!;
        }
        segs.Reverse();
        return segs.ToArray();
    }

    static string? MemberPathOrNull(Expression e, ParameterExpression param)
    {
        if (e is UnaryExpression { NodeType: ExpressionType.Convert } u)
            e = u.Operand;
        var segs = new List<string>();
        while (e is MemberExpression m)
        {
            segs.Add(m.Member.Name);
            e = m.Expression!;
        }
        if (e != param || segs.Count == 0)
            return null;
        segs.Reverse();
        return string.Join('.', segs);
    }

    static bool TryEval(Expression e, out object? value)
    {
        value = null;
        try
        {
            if (e is UnaryExpression { NodeType: ExpressionType.Convert } u)
                e = u.Operand;
            switch (e)
            {
                case ConstantExpression c:
                    value = c.Value;
                    return true;
                case MemberExpression m:
                    if (!TryEval(m.Expression!, out var target) && m.Expression != null)
                        return false;
                    value = m.Member switch
                    {
                        FieldInfo f => f.GetValue(target),
                        PropertyInfo p => p.GetValue(target),
                        _ => null
                    };
                    return true;
                default:
                    return false;
            }
        }
        catch
        {
            return false;
        }
    }

    static bool TryOp(ExpressionType t, out ExpressionType op)
    {
        op = t;
        return t is ExpressionType.Equal or ExpressionType.NotEqual
            or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
            or ExpressionType.LessThan or ExpressionType.LessThanOrEqual;
    }

    static ExpressionType Flip(ExpressionType op) => op switch
    {
        ExpressionType.GreaterThan => ExpressionType.LessThan,
        ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
        ExpressionType.LessThan => ExpressionType.GreaterThan,
        ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
        _ => op
    };

    /// <summary>Escapes a RediSearch TAG value (special characters are backslash-escaped).</summary>
    public static string EscapeTag(string value)
    {
        var sb = new StringBuilder(value.Length + 8);
        foreach (var c in value)
        {
            if (" ,.<>{}[]\"':;!@#$%^&*()-+=~/\\".IndexOf(c) >= 0)
                sb.Append('\\');
            sb.Append(c);
        }
        return sb.ToString();
    }

    /// <summary>Escapes a RediSearch full-text term.</summary>
    public static string EscapeText(string value)
    {
        var sb = new StringBuilder(value.Length + 8);
        foreach (var c in value)
        {
            if (",.<>{}[]\"':;!@#$%^&*()-+=~/\\ ".IndexOf(c) >= 0)
                sb.Append('\\');
            sb.Append(c);
        }
        return sb.ToString();
    }
}
