using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

abstract class DocumentAIFunctionBase<T> : AIFunction where T : class
{
    protected IDocumentStore Store { get; }
    protected DocumentAITypeRegistration<T> Registration { get; }
    protected IReadOnlyList<DocumentField> Fields { get; }

    readonly string name;
    readonly string description;
    readonly JsonElement schema;

    protected DocumentAIFunctionBase(
        IDocumentStore store,
        DocumentAITypeRegistration<T> registration,
        IReadOnlyList<DocumentField> fields,
        string name,
        string description,
        JsonElement schema)
    {
        this.Store = store;
        this.Registration = registration;
        this.Fields = fields;
        this.name = name;
        this.description = description;
        this.schema = schema;
    }

    public override string Name => this.name;
    public override string Description => this.description;
    public override JsonElement JsonSchema => this.schema;

    /// <summary>Reads a typed argument from the LLM-supplied bag, or returns <paramref name="fallback"/> if absent.</summary>
    protected static TValue? GetArg<TValue>(AIFunctionArguments arguments, string key, TValue? fallback = default)
    {
        if (!arguments.TryGetValue(key, out var raw) || raw is null)
            return fallback;
        if (raw is TValue typed)
            return typed;
        if (raw is JsonElement element)
            return JsonElementToValue<TValue>(element, fallback);
        // Last resort: ToString(). Avoids reflection-based JsonSerializer.Deserialize<TValue>.
        if (typeof(TValue) == typeof(string))
            return (TValue)(object)(raw.ToString() ?? string.Empty);
        return fallback;
    }

    [SuppressMessage("Trimming", "IL2026", Justification = "Only used for primitive types and JsonElement passthrough.")]
    [SuppressMessage("AOT", "IL3050", Justification = "Only used for primitive types and JsonElement passthrough.")]
    static TValue? JsonElementToValue<TValue>(JsonElement element, TValue? fallback)
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
