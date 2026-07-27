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
        => Build(
            fields, havingClauses, keyField, provider,
            path => DocumentQueryExtensions.ResolveJsonPath(path, typeInfo),
            path => DocumentQueryExtensions.ResolveJsonPathWithType(path, typeInfo),
            forceNumericAggregates: false);

    /// <summary>
    /// A JSON collection keyed by CLR type: paths resolve through the document's metadata (so naming
    /// policies and leaf types are honoured) but there is no <c>JsonTypeInfo&lt;T&gt;</c> to hand over,
    /// because the query's element type is the raw body.
    /// </summary>
    public static GroupedAggregateTranslator.Result TranslateJson(
        string fields,
        IReadOnlyList<string> havingClauses,
        string keyField,
        JsonTypeInfo typeInfo,
        IDatabaseProvider provider)
        => Build(
            fields, havingClauses, keyField, provider,
            path => ResolveJsonName(typeInfo, path),
            path => JsonPathFieldBinder.ResolvePath(typeInfo, path),
            forceNumericAggregates: false);

    static (string JsonPath, string LeafJsonName) ResolveJsonName(JsonTypeInfo typeInfo, string path)
    {
        var (jsonPath, _) = JsonPathFieldBinder.ResolvePath(typeInfo, path);
        var dot = jsonPath.LastIndexOf('.');
        return (jsonPath, dot < 0 ? jsonPath : jsonPath[(dot + 1)..]);
    }

    /// <summary>
    /// Schema-free twin: the path <em>is</em> the JSON path, and an extraction's type comes from an explicit
    /// <c>:type</c> hint rather than from metadata. <c>SUM</c>/<c>AVG</c>/<c>COUNT</c> hard-code
    /// <see cref="double"/> either way, so only <c>MIN</c>/<c>MAX</c> and the key column feel the difference —
    /// which is exactly where the hint matters (<c>min(total:number)</c> vs a text compare).
    /// </summary>
    public static GroupedAggregateTranslator.Result TranslateDynamic(
        string fields,
        IReadOnlyList<string> havingClauses,
        string keyField,
        IDatabaseProvider provider)
        => Build(
            fields, havingClauses, keyField, provider,
            DynamicPathResolver.ResolveJsonPath,
            DynamicPathResolver.ResolveJsonPathWithType,
            forceNumericAggregates: true);

    static GroupedAggregateTranslator.Result Build(
        string fields,
        IReadOnlyList<string> havingClauses,
        string keyField,
        IDatabaseProvider provider,
        Func<string, (string JsonPath, string LeafJsonName)> resolvePath,
        Func<string, (string JsonPath, Type LeafType)> resolvePathWithType,
        bool forceNumericAggregates)
    {
        var (keyJsonPath, keyLeaf) = resolvePath(keyField);
        var keyColumn = provider.JsonExtract("Data", keyJsonPath);
        var parameters = new Dictionary<string, object?>(StringComparer.Ordinal);
        var paramIndex = 0;

        string ColumnFor(string fieldPath)
        {
            var (jsonPath, _) = resolvePath(fieldPath);
            if (jsonPath != keyJsonPath)
                throw new ArgumentException(
                    $"'{fieldPath}' is neither the group key nor an aggregate. A grouped projection may only " +
                    "reference the GroupBy key or an aggregate (count/sum/avg/min/max).");
            return keyColumn;
        }

        string TypedColumn(string fieldPath)
        {
            var (jsonPath, leafType) = resolvePathWithType(fieldPath);
            // Typed extraction (mirrors the LINQ path) so SUM/AVG of a decimal keeps its scale and MIN/MAX of a
            // date/string compares by that type rather than a lossy numeric cast.
            return provider.JsonExtractTyped("Data", jsonPath, leafType);
        }

        string Aggregate(string func, string? argPath)
        {
            if (func.Equals("count", StringComparison.OrdinalIgnoreCase))
                return "COUNT(*)";
            var f = func.ToUpperInvariant();
            var path = argPath ?? throw new ArgumentException($"{func}() requires a field argument.");
            // Typed: always the leaf type, so SUM/AVG of a decimal keeps its scale.
            // Schema-free: an unhinted path resolves to string, which SUM/AVG cannot add on the providers
            // whose plain extract returns text — so those two extract numerically regardless. MIN/MAX still
            // honour the leaf type either way, which is where a `:type` hint decides between a numeric and a
            // lexicographic comparison.
            var inner = forceNumericAggregates && f is not ("MIN" or "MAX")
                ? provider.JsonExtractTyped("Data", resolvePathWithType(path).JsonPath, typeof(double))
                : TypedColumn(path);
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
