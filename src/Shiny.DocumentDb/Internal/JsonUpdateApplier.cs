using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Applies one update assignment — a plain value or a <see cref="DocumentUpdateOperation"/> — to a parsed document.
/// The single implementation behind every provider that updates by reading, changing and rewriting the JSON, so an
/// increment or an array pull means the same thing on all of them.
/// </summary>
static class JsonUpdateApplier
{
    /// <summary>Applies every assignment, in order.</summary>
    public static void ApplyAll(JsonObject root, IReadOnlyList<(string JsonPath, object? Value)> assignments, JsonSerializerOptions options)
    {
        for (var i = 0; i < assignments.Count; i++)
            Apply(root, assignments[i].JsonPath, assignments[i].Value, options);
    }

    /// <summary>
    /// Applies one assignment at a dotted <paramref name="jsonPath"/>. A plain set leaves the document alone when an
    /// intermediate object is missing, as the stores always have; an operation creates the intermediate objects it needs.
    /// </summary>
    public static void Apply(JsonObject root, string jsonPath, object? value, JsonSerializerOptions options)
    {
        if (value is not DocumentUpdateOperation op)
        {
            var parent = Navigate(root, jsonPath, create: false, out var leaf);
            if (parent != null)
                parent[leaf] = ToNode(value, options);
            return;
        }

        if (op.Kind == DocumentUpdateKind.Unset)
        {
            Navigate(root, jsonPath, create: false, out var unsetLeaf)?.Remove(unsetLeaf);
            return;
        }

        var target = Navigate(root, jsonPath, create: true, out var name)!;
        target.TryGetPropertyValue(name, out var current);
        switch (op.Kind)
        {
            case DocumentUpdateKind.Increment:
                target[name] = Add(current, op.Operand!, jsonPath);
                break;

            case DocumentUpdateKind.Push:
                ArrayAt(target, name, current, jsonPath).Add(ToNode(op.Operand, options));
                break;

            case DocumentUpdateKind.AddToSet:
            {
                var array = ArrayAt(target, name, current, jsonPath);
                var item = ToNode(op.Operand, options);
                if (!array.Any(x => JsonNode.DeepEquals(x, item)))
                    array.Add(item);
                break;
            }

            case DocumentUpdateKind.Pull:
            {
                if (current is null)
                    return;
                if (current is not JsonArray array)
                    throw NotAnArray(jsonPath, current);
                var item = ToNode(op.Operand, options);
                for (var i = array.Count - 1; i >= 0; i--)
                {
                    if (JsonNode.DeepEquals(array[i], item))
                        array.RemoveAt(i);
                }
                break;
            }
        }
    }

    /// <summary>
    /// The JSON node for a plain value or an operation operand, formatted the way a normal document write would
    /// format it (dates as ISO strings, enums per the store's enum handling, objects through the store's options).
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Object operands serialize through the store's options; the caller's type must be in its resolver, as for any document.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Object operands serialize through the store's options; the caller's type must be in its resolver, as for any document.")]
    public static JsonNode? ToNode(object? value, JsonSerializerOptions options)
    {
        if (value is null)
            return null;
        if (value is JsonNode node)
            return node.DeepClone();

        // The store's own options first, so an element pushed onto an array is formatted exactly like the elements
        // the store wrote (custom converters included) and Pull / AddToSet compare like with like. A scalar the
        // options cannot resolve (a source-generated resolver that never saw it) falls back to the built-in
        // literal formatting the relational providers use.
        if (options.TryGetTypeInfo(value.GetType(), out var typeInfo))
            return JsonSerializer.SerializeToNode(value, typeInfo);

        var normalized = DocumentStore.NormalizeUpdateValue(value, options);
        if (IsScalar(normalized!))
            return JsonNode.Parse(DocumentStore.ToJsonLiteral(normalized));

        return JsonSerializer.SerializeToNode(value, options.GetTypeInfo(value.GetType()));
    }

    /// <summary>The operand of an array operation as the JSON text a provider binds or embeds.</summary>
    public static string ToJson(object? value, JsonSerializerOptions options)
        => ToNode(value, options)?.ToJsonString() ?? "null";

    static bool IsScalar(object value)
        => value is string or bool or char or Guid or DateTime or DateTimeOffset or DateOnly or TimeOnly or TimeSpan or Enum
           || DocumentUpdateOperation.IsNumber(value);

    // Walks to the object holding the last path segment. With create=false a missing (or non-object) intermediate
    // returns null; with create=true it is created as an empty object.
    static JsonObject? Navigate(JsonObject root, string jsonPath, bool create, out string leaf)
    {
        var parts = jsonPath.Split('.');
        leaf = parts[^1];
        var current = root;
        for (var i = 0; i < parts.Length - 1; i++)
        {
            current.TryGetPropertyValue(parts[i], out var next);
            if (next is JsonObject obj)
            {
                current = obj;
            }
            else if (create && next is null)
            {
                var created = new JsonObject();
                current[parts[i]] = created;
                current = created;
            }
            else
            {
                if (create)
                    throw new InvalidOperationException($"Cannot update '{jsonPath}': '{parts[i]}' is not an object.");
                return null;
            }
        }
        return current;
    }

    static JsonArray ArrayAt(JsonObject parent, string name, JsonNode? current, string jsonPath)
    {
        if (current is JsonArray existing)
            return existing;
        if (current is not null)
            throw NotAnArray(jsonPath, current);

        var created = new JsonArray();
        parent[name] = created;
        return created;
    }

    static JsonNode Add(JsonNode? current, object amount, string jsonPath)
    {
        if (current is null)
            return NumberNode(amount);

        if (current is not JsonValue value || value.GetValueKind() != JsonValueKind.Number)
            throw new InvalidOperationException($"Cannot increment '{jsonPath}': the stored value {current.ToJsonString()} is not a number.");

        // Integral + integral stays integral; anything else goes through decimal (exact for the values documents
        // hold) and falls back to double only when decimal would overflow.
        if (IsIntegral(amount) && value.TryGetValue<long>(out var stored))
            return JsonValue.Create(checked(stored + Convert.ToInt64(amount, CultureInfo.InvariantCulture)));

        if (value.TryGetValue<decimal>(out var storedDecimal) && amount is not double and not float)
        {
            try
            {
                return JsonValue.Create(storedDecimal + Convert.ToDecimal(amount, CultureInfo.InvariantCulture));
            }
            catch (OverflowException)
            {
                // fall through to double
            }
        }

        return JsonValue.Create(value.GetValue<double>() + Convert.ToDouble(amount, CultureInfo.InvariantCulture));
    }

    static JsonNode NumberNode(object amount) => amount switch
    {
        decimal m => JsonValue.Create(m),
        double d => JsonValue.Create(d),
        float f => JsonValue.Create((double)f),
        ulong u => JsonValue.Create(u),
        _ => JsonValue.Create(Convert.ToInt64(amount, CultureInfo.InvariantCulture))
    };

    static bool IsIntegral(object value) => value is int or long or short or byte or sbyte or uint or ushort;

    static InvalidOperationException NotAnArray(string jsonPath, JsonNode current)
        => new($"Cannot update '{jsonPath}' as an array: the stored value {current.ToJsonString()} is not an array.");
}
