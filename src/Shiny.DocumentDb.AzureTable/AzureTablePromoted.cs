using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.AzureTable;

/// <summary>A promoted/indexed property — a scalar written as a native top-level Table column
/// alongside <c>Data</c> so OData <c>$filter</c> pushdown and secondary lookups can target it.</summary>
internal sealed class IndexedMapping
{
    public required string ClrPath { get; init; }        // e.g. "Status" or "Address.City"
    public required string[] JsonSegments { get; init; } // resolved via naming policy
    public required string ColumnName { get; init; }     // e.g. "idx_status"
}

/// <summary>A single translatable comparison over a promoted column (<c>column op value</c>).</summary>
internal readonly record struct PromotedClause(string Column, string Op, object? Value);

internal static class AzureTablePromoted
{
    /// <summary>Extracts the CLR member path (outer→inner) from a <c>x =&gt; x.A.B</c> expression.</summary>
    public static string[] ExtractPath(LambdaExpression property)
    {
        var body = property.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } u)
            body = u.Operand;

        var segments = new List<string>();
        while (body is MemberExpression m)
        {
            segments.Add(m.Member.Name);
            body = m.Expression!;
        }
        if (segments.Count == 0)
            throw new ArgumentException("MapIndexedProperty requires a property access expression (e.g. x => x.Status).");
        segments.Reverse();
        return segments.ToArray();
    }

    public static IndexedMapping Build(string[] clrSegments, JsonSerializerOptions jsonOptions)
    {
        var jsonSegs = clrSegments
            .Select(s => jsonOptions.PropertyNamingPolicy?.ConvertName(s) ?? s)
            .ToArray();
        return new IndexedMapping
        {
            ClrPath = string.Join('.', clrSegments),
            JsonSegments = jsonSegs,
            ColumnName = "idx_" + string.Join('_', jsonSegs).Replace('-', '_')
        };
    }

    /// <summary>Reads the promoted value out of the serialized body so it can be written as a native column.</summary>
    public static object? ReadValue(JsonObject data, IndexedMapping mapping)
    {
        JsonNode? node = data;
        foreach (var seg in mapping.JsonSegments)
        {
            if (node is not JsonObject obj || obj[seg] is not { } next)
                return null;
            node = next;
        }
        if (node is not JsonValue v)
            return null;

        if (v.TryGetValue<long>(out var l)) return l;
        if (v.TryGetValue<double>(out var d)) return d;
        if (v.TryGetValue<bool>(out var b)) return b;
        if (v.TryGetValue<string>(out var s)) return s;
        return v.ToJsonString();
    }

    /// <summary>Pulls the AND-conjoined comparisons on promoted columns out of the predicates. The
    /// caller still evaluates the full predicate client-side, so an incomplete translation only
    /// widens the candidate set — it never drops a matching document.</summary>
    public static List<PromotedClause> ExtractClauses<T>(
        IEnumerable<Expression<Func<T, bool>>> predicates,
        IReadOnlyDictionary<string, IndexedMapping> byClrPath)
    {
        var clauses = new List<PromotedClause>();
        foreach (var p in predicates)
            Walk(p.Body, p.Parameters[0], byClrPath, clauses);
        return clauses;
    }

    static void Walk(Expression body, ParameterExpression param, IReadOnlyDictionary<string, IndexedMapping> byClrPath, List<PromotedClause> acc)
    {
        if (body is BinaryExpression { NodeType: ExpressionType.AndAlso } and)
        {
            Walk(and.Left, param, byClrPath, acc);
            Walk(and.Right, param, byClrPath, acc);
            return;
        }

        if (body is BinaryExpression cmp && TryOp(cmp.NodeType, out var op))
        {
            if (TryClause(cmp.Left, cmp.Right, op, param, byClrPath, out var c) ||
                TryClause(cmp.Right, cmp.Left, Flip(op), param, byClrPath, out c))
                acc.Add(c);
        }
    }

    static bool TryClause(Expression memberSide, Expression valueSide, string op, ParameterExpression param, IReadOnlyDictionary<string, IndexedMapping> byClrPath, out PromotedClause clause)
    {
        clause = default;
        var path = MemberPath(memberSide, param);
        if (path == null || !byClrPath.TryGetValue(path, out var mapping))
            return false;
        if (!TryEval(valueSide, out var value) || value == null)
            return false;
        clause = new PromotedClause(mapping.ColumnName, op, value);
        return true;
    }

    static string? MemberPath(Expression e, ParameterExpression param)
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

    static bool TryOp(ExpressionType t, out string op)
    {
        op = t switch
        {
            ExpressionType.Equal => "eq",
            ExpressionType.NotEqual => "ne",
            ExpressionType.GreaterThan => "gt",
            ExpressionType.GreaterThanOrEqual => "ge",
            ExpressionType.LessThan => "lt",
            ExpressionType.LessThanOrEqual => "le",
            _ => ""
        };
        return op.Length > 0;
    }

    static string Flip(string op) => op switch
    {
        "gt" => "lt",
        "ge" => "le",
        "lt" => "gt",
        "le" => "ge",
        _ => op
    };

    /// <summary>Renders an OData <c>$filter</c> fragment for the extracted clauses (null when none).</summary>
    public static string? ToODataFilter(IReadOnlyList<PromotedClause> clauses)
    {
        if (clauses.Count == 0)
            return null;
        var sb = new StringBuilder();
        for (var i = 0; i < clauses.Count; i++)
        {
            if (i > 0) sb.Append(" and ");
            sb.Append(clauses[i].Column).Append(' ').Append(clauses[i].Op).Append(' ').Append(Literal(clauses[i].Value));
        }
        return sb.ToString();
    }

    public static string Literal(object? value) => value switch
    {
        null => "null",
        bool b => b ? "true" : "false",
        string s => "'" + s.Replace("'", "''") + "'",
        Guid g => "'" + g + "'",
        DateTime dt => "datetime'" + dt.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture) + "'",
        DateTimeOffset dto => "datetime'" + dto.UtcDateTime.ToString("o", CultureInfo.InvariantCulture) + "'",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "null"
    };
}
