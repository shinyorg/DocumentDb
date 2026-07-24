using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.AI;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

abstract class DocumentAIFunctionBase<T> : AIFunction where T : class
{
    protected IDocumentStore Store { get; }
    protected DocumentAITypeRegistration<T> Registration { get; }
    protected IReadOnlyList<DocumentField> Fields { get; }

    readonly string name;
    readonly string description;
    readonly JsonElement schema;
    // Compile-free (AOT-safe) evaluators for the registered non-removable filters — same interpreter the
    // in-memory providers use. Built once per tool instance.
    readonly Func<T, bool>[] filterPredicates;

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
        this.filterPredicates = registration.Filters.Count == 0
            ? Array.Empty<Func<T, bool>>()
            : registration.Filters.Select(ExpressionInterpreter.Interpret).ToArray();
    }

    public override string Name => this.name;
    public override string Description => this.description;
    public override JsonElement JsonSchema => this.schema;

    /// <summary>True when one or more non-removable <c>Where</c> filters are configured for this type.</summary>
    protected bool HasFilters => this.filterPredicates.Length > 0;

    /// <summary>AND-applies the registered non-removable filters to a store query (pushed down to the provider).</summary>
    protected IDocumentQuery<T> ApplyFilters(IDocumentQuery<T> query)
    {
        foreach (var f in this.Registration.Filters)
            query = query.Where(f);
        return query;
    }

    /// <summary>Evaluates the registered filters in-memory (AOT-safe) — true when the document is within scope.</summary>
    protected bool InScope(T document)
    {
        foreach (var predicate in this.filterPredicates)
        {
            if (!predicate(document))
                return false;
        }
        return true;
    }

    /// <summary>
    /// Reads the raw Id value off a materialized document via <c>JsonTypeInfo</c> (the "Id" convention),
    /// so the scoped update path can re-fetch the stored record. Returns null when the type exposes no
    /// property named "Id" (e.g. a custom-named mapped Id).
    /// </summary>
    protected object? TryGetDocumentId(T document)
    {
        foreach (var prop in this.Registration.JsonTypeInfo.Properties)
        {
            if (prop.AttributeProvider is MemberInfo mi && mi.Name == "Id")
                return prop.Get?.Invoke(document);
        }
        return null;
    }

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
