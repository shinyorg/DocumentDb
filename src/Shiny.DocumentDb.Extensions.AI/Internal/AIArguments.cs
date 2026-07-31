using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

/// <summary>
/// Reads the loosely-typed argument bag an <see cref="AIFunction"/> is invoked with. Shared by both lanes —
/// the arguments an LLM sends are the same shape whether the tool is backed by a document type or a
/// schema-free collection.
/// </summary>
static class AIArguments
{
    /// <summary>Reads a typed argument, or returns <paramref name="fallback"/> when it is absent or null.</summary>
    public static TValue? Get<TValue>(AIFunctionArguments arguments, string key, TValue? fallback = default)
    {
        if (!arguments.TryGetValue(key, out var raw) || raw is null)
            return fallback;
        if (raw is TValue typed)
            return typed;
        if (raw is JsonElement element)
            return FromJsonElement<TValue>(element, fallback);
        // Last resort: ToString(). Avoids reflection-based JsonSerializer.Deserialize<TValue>.
        if (typeof(TValue) == typeof(string))
            return (TValue)(object)(raw.ToString() ?? string.Empty);
        return fallback;
    }

    /// <summary>Reads an argument that is itself JSON (the structured filter, a document body).</summary>
    public static JsonElement? GetJson(AIFunctionArguments arguments, string key)
    {
        arguments.TryGetValue(key, out var raw);
        return raw is JsonElement element ? element : null;
    }

    [SuppressMessage("Trimming", "IL2026", Justification = "Only used for primitive types and JsonElement passthrough.")]
    [SuppressMessage("AOT", "IL3050", Justification = "Only used for primitive types and JsonElement passthrough.")]
    static TValue? FromJsonElement<TValue>(JsonElement element, TValue? fallback)
    {
        if (typeof(TValue) == typeof(JsonElement))
            return (TValue)(object)element;
        if (typeof(TValue) == typeof(string))
            return element.ValueKind == JsonValueKind.String ? (TValue?)(object?)element.GetString() : (TValue?)(object?)element.ToString();
        if (typeof(TValue) == typeof(int))
            return element.TryGetInt32(out var i) ? (TValue)(object)i : fallback;
        if (typeof(TValue) == typeof(long))
            return element.TryGetInt64(out var l) ? (TValue)(object)l : fallback;
        if (typeof(TValue) == typeof(bool))
            return element.ValueKind == JsonValueKind.True || element.ValueKind == JsonValueKind.False
                ? (TValue)(object)element.GetBoolean() : fallback;
        if (typeof(TValue) == typeof(double))
            return element.TryGetDouble(out var d) ? (TValue)(object)d : fallback;
        return fallback;
    }
}
