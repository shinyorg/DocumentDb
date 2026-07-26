using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Amazon.DynamoDBv2.Model;

namespace Shiny.DocumentDb.DynamoDb;

/// <summary>A promoted/indexed property — a scalar written as a native top-level DynamoDB attribute
/// alongside <c>Data</c> so <c>FilterExpression</c> pushdown, PartiQL, and GSIs can target it.</summary>
internal sealed class IndexedMapping
{
    public required string ClrPath { get; init; }        // e.g. "Status" or "Address.City"
    public required string[] JsonSegments { get; init; } // resolved via naming policy
    public required string AttributeName { get; init; }  // e.g. "idx_status"
}

/// <summary>A single translatable comparison over a promoted attribute (<c>attr op value</c>).</summary>
internal readonly record struct PromotedClause(string Attribute, string Op, object? Value);

internal static class DynamoDbPromoted
{
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
            AttributeName = "idx_" + string.Join('_', jsonSegs).Replace('-', '_')
        };
    }

    public static AttributeValue? ReadValue(JsonObject data, IndexedMapping mapping)
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

        if (v.TryGetValue<bool>(out var b)) return new AttributeValue { BOOL = b };
        if (v.TryGetValue<long>(out var l)) return new AttributeValue { N = l.ToString(CultureInfo.InvariantCulture) };
        if (v.TryGetValue<double>(out var d)) return new AttributeValue { N = d.ToString(CultureInfo.InvariantCulture) };
        if (v.TryGetValue<string>(out var s)) return new AttributeValue { S = s };
        return new AttributeValue { S = v.ToJsonString() };
    }

    public static AttributeValue ToAttributeValue(object value) => value switch
    {
        bool b => new AttributeValue { BOOL = b },
        string s => new AttributeValue { S = s },
        Guid g => new AttributeValue { S = g.ToString() },
        int or long or short or byte => new AttributeValue { N = Convert.ToInt64(value).ToString(CultureInfo.InvariantCulture) },
        float or double or decimal => new AttributeValue { N = Convert.ToDecimal(value).ToString(CultureInfo.InvariantCulture) },
        IFormattable f => new AttributeValue { S = f.ToString(null, CultureInfo.InvariantCulture) },
        _ => new AttributeValue { S = value.ToString() ?? "" }
    };

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
        clause = new PromotedClause(mapping.AttributeName, op, value);
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
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => ""
        };
        return op.Length > 0;
    }

    static string Flip(string op) => op switch
    {
        ">" => "<",
        ">=" => "<=",
        "<" => ">",
        "<=" => ">=",
        _ => op
    };

    /// <summary>Builds a DynamoDB <c>FilterExpression</c> for the clauses (null when none).</summary>
    public static (string Expression, Dictionary<string, string> Names, Dictionary<string, AttributeValue> Values)? ToFilterExpression(IReadOnlyList<PromotedClause> clauses)
    {
        if (clauses.Count == 0)
            return null;

        var sb = new StringBuilder();
        var names = new Dictionary<string, string>();
        var values = new Dictionary<string, AttributeValue>();
        for (var i = 0; i < clauses.Count; i++)
        {
            if (i > 0) sb.Append(" AND ");
            var nk = $"#f{i}";
            var vk = $":f{i}";
            names[nk] = clauses[i].Attribute;
            values[vk] = ToAttributeValue(clauses[i].Value!);
            sb.Append(nk).Append(' ').Append(clauses[i].Op).Append(' ').Append(vk);
        }
        return (sb.ToString(), names, values);
    }
}
