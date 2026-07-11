using System.Globalization;
using System.Text;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Translates the string-grammar grouped surface — <c>GroupBy("status").Project("status, count() as n,
/// sum(total) as revenue").Having("sum(total) &gt; 10000")</c> — into the same
/// <see cref="GroupedAggregateTranslator.Result"/> the typed LINQ path produces, so both surfaces emit
/// identical <c>GROUP BY</c> / <c>HAVING</c> SQL.
/// </summary>
static class GroupStringTranslator
{
    static readonly HashSet<string> Aggregates = new(StringComparer.OrdinalIgnoreCase) { "count", "sum", "avg", "min", "max" };

    public static bool IsAggregateFunction(string ident) => Aggregates.Contains(ident);

    public static GroupedAggregateTranslator.Result Translate<T>(
        string fields,
        IReadOnlyList<string> havingClauses,
        string keyField,
        JsonTypeInfo<T> typeInfo,
        IDatabaseProvider provider,
        IReadOnlyDictionary<string, ComputedMapping>? computed) where T : class
    {
        var (keyJsonPath, keyLeaf) = DocumentQueryExtensions.ResolveJsonPath(keyField, typeInfo);
        var keyColumn = provider.JsonExtract("Data", keyJsonPath);
        var parameters = new Dictionary<string, object?>(StringComparer.Ordinal);
        var paramIndex = 0;

        string ColumnFor(string fieldPath)
        {
            var (jsonPath, _) = DocumentQueryExtensions.ResolveJsonPath(fieldPath, typeInfo);
            if (jsonPath != keyJsonPath)
                throw new ArgumentException(
                    $"'{fieldPath}' is neither the group key nor an aggregate. A grouped projection may only " +
                    "reference the GroupBy key or an aggregate (count/sum/avg/min/max).");
            return keyColumn;
        }

        string TypedColumn(string fieldPath)
        {
            var (jsonPath, leafType) = DocumentQueryExtensions.ResolveJsonPathWithType(fieldPath, typeInfo);
            // Typed extraction (mirrors the LINQ path) so SUM/AVG of a decimal keeps its scale and MIN/MAX of a
            // date/string compares by that type rather than a lossy numeric cast.
            return provider.JsonExtractTyped("Data", jsonPath, leafType);
        }

        string Aggregate(string func, string? argPath)
        {
            if (func.Equals("count", StringComparison.OrdinalIgnoreCase))
                return "COUNT(*)";
            var f = func.ToUpperInvariant();
            var inner = TypedColumn(argPath ?? throw new ArgumentException($"{func}() requires a field argument."));
            // MIN/MAX must not COALESCE to 0 (wrong, and a type clash for date/string); an all-NULL group is
            // NULL. SUM/AVG fold an empty/all-NULL group to 0 to match the in-memory interpreter.
            return f is "MIN" or "MAX" ? $"{f}({inner})" : $"COALESCE({f}({inner}), 0)";
        }

        // ── projection ──
        var parser = new GroupGrammarParser(fields);
        var items = parser.ParseProjection();
        var pairs = new List<string>(items.Count * 2);
        var outputColumns = new Dictionary<string, string>(StringComparer.Ordinal);
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var item in items)
        {
            string alias;
            string columnSql;
            if (item.Function != null)
            {
                alias = item.Alias ?? throw new ArgumentException($"'{item.Function}(...)' projection requires an alias, e.g. '{item.Function}(total) as revenue'.");
                columnSql = Aggregate(item.Function, item.Argument);
            }
            else
            {
                alias = item.Alias ?? keyLeaf;
                columnSql = ColumnFor(item.FieldPath!);
            }

            if (!seen.Add(alias))
                throw new ArgumentException($"Projection resolves to duplicate output key '{alias}'.");
            pairs.Add($"'{alias}'");
            pairs.Add(columnSql);
            outputColumns[alias] = columnSql;
        }
        if (pairs.Count == 0)
            throw new ArgumentException("At least one field must be specified.", nameof(fields));

        // ── having ──
        string? havingSql = null;
        if (havingClauses.Count > 0)
        {
            var parts = new List<string>(havingClauses.Count);
            foreach (var clause in havingClauses)
            {
                var hp = new GroupGrammarParser(clause);
                parts.Add(hp.ParseHaving(EmitHavingOperand));
            }
            havingSql = parts.Count == 1 ? parts[0] : string.Join(" AND ", parts.Select(p => $"({p})"));
        }

        return new GroupedAggregateTranslator.Result(
            provider.JsonObject(pairs), keyColumn, havingSql, outputColumns, parameters);

        string EmitHavingOperand(GroupGrammarParser.Operand operand)
        {
            switch (operand.Kind)
            {
                case GroupGrammarParser.OperandKind.Aggregate:
                    return Aggregate(operand.Function!, operand.Argument);
                case GroupGrammarParser.OperandKind.Field:
                    return ColumnFor(operand.Text!);
                default:
                    var name = $"@h{paramIndex++}x";
                    parameters[name] = operand.Literal;
                    return name;
            }
        }
    }
}
