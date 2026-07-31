using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

/// <summary>
/// Translates the structured-filter JSON the LLM produces into the store's string grammar, for the lane
/// that has no CLR type. The typed lane builds an <c>Expression&lt;Func&lt;T,bool&gt;&gt;</c> over
/// <c>PropertyInfo</c>; there is none of that here, so the same filter shape is lowered to grammar text
/// instead — which reaches the same IR and the same SQL.
/// </summary>
/// <remarks>
/// Every model-supplied <b>value</b> is captured as an interpolation argument and bound as a parameter,
/// never formatted into the text; every model-supplied <b>path</b> is validated against the declared field
/// set (or the path allow-list in open mode) before it is written into the text.
/// </remarks>
static class JsonFilterTranslator
{
    public static GrammarFilter? Translate(JsonElement? filter, DocumentAICollectionRegistration registration)
    {
        if (filter is null || filter.Value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return null;

        var result = new GrammarFilter();
        Build(filter.Value, registration, result);
        return result;
    }

    static void Build(JsonElement node, DocumentAICollectionRegistration reg, GrammarFilter output)
    {
        if (node.ValueKind != JsonValueKind.Object)
            throw new InvalidOperationException("Filter node must be a JSON object.");

        // Combinators take precedence and are mutually exclusive with leaf comparisons.
        if (node.TryGetProperty("and", out var andNode))
        {
            Combine(andNode, reg, output, "and");
            return;
        }
        if (node.TryGetProperty("or", out var orNode))
        {
            Combine(orNode, reg, output, "or");
            return;
        }
        if (node.TryGetProperty("not", out var notNode))
        {
            output.Literal("not (");
            Build(notNode, reg, output);
            output.Literal(")");
            return;
        }

        BuildLeaf(node, reg, output);
    }

    static void Combine(JsonElement array, DocumentAICollectionRegistration reg, GrammarFilter output, string keyword)
    {
        if (array.ValueKind != JsonValueKind.Array || array.GetArrayLength() == 0)
            throw new InvalidOperationException($"'{keyword}' must be a non-empty array.");

        output.Literal("(");
        var first = true;
        foreach (var item in array.EnumerateArray())
        {
            if (!first)
                output.Literal($" {keyword} ");
            first = false;
            Build(item, reg, output);
        }
        output.Literal(")");
    }

    static void BuildLeaf(JsonElement node, DocumentAICollectionRegistration reg, GrammarFilter output)
    {
        if (!node.TryGetProperty("field", out var fieldNode))
            throw new InvalidOperationException("Leaf filter requires a 'field' property.");
        if (!node.TryGetProperty("op", out var opNode))
            throw new InvalidOperationException("Leaf filter requires an 'op' property.");

        var fieldName = fieldNode.GetString() ?? throw new InvalidOperationException("'field' must be a string.");
        var op = opNode.GetString() ?? throw new InvalidOperationException("'op' must be a string.");

        var field = reg.ResolveField(fieldName);

        // In open mode the leaf may carry its own type hint — there is no declaration to take one from.
        if (reg.Fields is null && node.TryGetProperty("type", out var typeNode) && typeNode.ValueKind == JsonValueKind.String)
            field = field with { Type = DocumentFieldTypes.FromGrammarHint(typeNode.GetString()) };

        node.TryGetProperty("value", out var valueNode);

        switch (op)
        {
            case "contains":
            case "startsWith":
            {
                if (valueNode.ValueKind != JsonValueKind.String)
                    throw new InvalidOperationException($"Operator '{op}' requires a string value.");
                // String functions take the plain path — a :type hint would contradict the string operand.
                output.Literal(op == "contains" ? "contains(" : "startswith(");
                output.Literal(field.Path);
                output.Literal(", ");
                output.Arg(valueNode.GetString());
                output.Literal(")");
                return;
            }

            case "in":
            {
                if (valueNode.ValueKind != JsonValueKind.Array)
                    throw new InvalidOperationException("'in' requires an array value.");
                if (valueNode.GetArrayLength() == 0)
                    throw new InvalidOperationException("'in' requires a non-empty array value.");

                output.Literal(field.HintedPath);
                output.Literal(" in (");
                var first = true;
                foreach (var item in valueNode.EnumerateArray())
                {
                    if (!first)
                        output.Literal(", ");
                    first = false;
                    output.Arg(ConvertValue(item, field.Type));
                }
                output.Literal(")");
                return;
            }

            default:
            {
                var symbol = op switch
                {
                    "eq" => "==",
                    "ne" => "!=",
                    "gt" => ">",
                    "gte" => ">=",
                    "lt" => "<",
                    "lte" => "<=",
                    _ => throw new InvalidOperationException($"Unknown operator '{op}'.")
                };

                output.Literal(field.HintedPath);
                output.Literal($" {symbol} ");
                output.Arg(ConvertValue(valueNode, field.Type));
                return;
            }
        }
    }

    /// <summary>
    /// Binds a JSON value to the CLR type the field was declared as. With no declaration (open mode, no
    /// hint) the value keeps its own natural type, which is what the grammar infers the field's type from.
    /// </summary>
    static object? ConvertValue(JsonElement value, DocumentFieldType? type)
    {
        if (value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return null;

        switch (type)
        {
            case DocumentFieldType.String:
                return value.ValueKind == JsonValueKind.String ? value.GetString() : value.ToString();
            case DocumentFieldType.Bool:
                return value.ValueKind is JsonValueKind.True or JsonValueKind.False
                    ? value.GetBoolean()
                    : bool.Parse(value.ToString());
            case DocumentFieldType.Guid:
                return Guid.Parse(AsText(value));
            case DocumentFieldType.Date:
                return value.ValueKind == JsonValueKind.String
                    ? DateTimeOffset.Parse(value.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
                    : value.GetDateTimeOffset();
            case DocumentFieldType.Int:
                return value.ValueKind == JsonValueKind.Number ? value.GetInt32() : int.Parse(AsText(value), CultureInfo.InvariantCulture);
            case DocumentFieldType.Long:
                return value.ValueKind == JsonValueKind.Number ? value.GetInt64() : long.Parse(AsText(value), CultureInfo.InvariantCulture);
            case DocumentFieldType.Decimal:
                return value.ValueKind == JsonValueKind.Number ? value.GetDecimal() : decimal.Parse(AsText(value), CultureInfo.InvariantCulture);
            case DocumentFieldType.Number:
            case DocumentFieldType.Double:
                return value.ValueKind == JsonValueKind.Number ? value.GetDouble() : double.Parse(AsText(value), CultureInfo.InvariantCulture);
        }

        return value.ValueKind switch
        {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.True or JsonValueKind.False => value.GetBoolean(),
            // Mirrors the grammar's own literal typing: an integer that fits int stays an int.
            JsonValueKind.Number when value.TryGetInt32(out var i) => i,
            JsonValueKind.Number when value.TryGetInt64(out var l) => l,
            JsonValueKind.Number => value.GetDouble(),
            _ => value.ToString()
        };
    }

    static string AsText(JsonElement value)
        => value.ValueKind == JsonValueKind.String ? value.GetString()! : value.ToString();
}

/// <summary>
/// A string-grammar clause under construction: literal text interleaved with values that must be bound as
/// parameters. Assembled into a <see cref="FilterInterpolatedStringHandler"/> at the point of use, exactly
/// as the compiler would for a <c>Where($"…")</c> written by hand.
/// </summary>
sealed class GrammarFilter
{
    readonly List<object?> parts = new();
    readonly StringBuilder pending = new();
    int literalLength;
    int argCount;

    public void Literal(string text)
    {
        this.pending.Append(text);
        this.literalLength += text.Length;
    }

    public void Arg(object? value)
    {
        this.Flush();
        this.parts.Add(new ArgumentHole(value));
        this.argCount++;
    }

    void Flush()
    {
        if (this.pending.Length == 0)
            return;
        this.parts.Add(this.pending.ToString());
        this.pending.Clear();
    }

    public IJsonDocumentQuery ApplyTo(IJsonDocumentQuery query)
    {
        this.Flush();

        var handler = new FilterInterpolatedStringHandler(this.literalLength, this.argCount);
        foreach (var part in this.parts)
        {
            if (part is ArgumentHole hole)
                handler.AppendFormatted(hole.Value);
            else
                handler.AppendLiteral((string)part!);
        }
        return query.Where(handler);
    }

    sealed record ArgumentHole(object? Value);
}
