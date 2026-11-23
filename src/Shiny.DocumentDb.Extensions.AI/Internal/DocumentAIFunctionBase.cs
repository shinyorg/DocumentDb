using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.AI;
using Shiny.DocumentDb.Hosting;

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
    // Properties the registration hides (IgnoreProperties / AllowProperties). They are kept out of every
    // tool result and every LLM-supplied write, not just out of the schema. Empty when nothing is hidden.
    readonly JsonPropertyInfo[] hiddenProperties;
    readonly HashSet<string> visibleNames;
    readonly JsonPropertyInfo? idProperty;

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
            : registration.Filters.Select(DocumentPredicate.Compile).ToArray();

        this.visibleNames = new HashSet<string>(fields.Select(f => f.JsonName), StringComparer.Ordinal);
        this.hiddenProperties = registration.JsonTypeInfo.Properties
            .Where(p => !this.visibleNames.Contains(p.Name))
            .ToArray();
        this.idProperty = registration.JsonTypeInfo.Properties
            .FirstOrDefault(p => p.AttributeProvider is MemberInfo { Name: "Id" });
    }

    public override string Name => this.name;
    public override string Description => this.description;
    public override JsonElement JsonSchema => this.schema;

    /// <summary>True when one or more non-removable <c>Where</c> filters are configured for this type.</summary>
    protected bool HasFilters => this.filterPredicates.Length > 0 || this.Registration.DynamicFilters.Count > 0;

    /// <summary>
    /// Resolves this call's scope: the static filters (interpreted once, in the constructor) plus any
    /// request-resolved ones, evaluated now. Call it <b>once</b> per invocation and use the result for both
    /// the query push-down and the in-memory checks — resolving twice invites the two enforcement paths
    /// disagreeing, which is a scope bypass.
    /// </summary>
    protected ValueTask<DocumentAIScope<T>> ResolveScope(AIFunctionArguments arguments, CancellationToken cancellationToken)
        => DocumentAIScopeResolver.Resolve(
            this.Registration.Filters,
            this.filterPredicates,
            this.Registration.DynamicFilters,
            arguments,
            cancellationToken);

    /// <summary>
    /// Reads the raw Id value off a materialized document via <c>JsonTypeInfo</c> (the "Id" convention),
    /// so the scoped update path can re-fetch the stored record. Returns null when the type exposes no
    /// property named "Id" (e.g. a custom-named mapped Id).
    /// </summary>
    protected object? TryGetDocumentId(T document)
        => this.idProperty?.Get?.Invoke(document);

    /// <summary>True when the registration hides one or more properties from the LLM.</summary>
    protected bool HasHiddenProperties => this.hiddenProperties.Length > 0;

    /// <summary>
    /// Converts the id the LLM sent — always a string, because the tool schema says so — into the CLR type
    /// of the document's <c>Id</c> property, so a Guid/int/long/custom-keyed type can be looked up. Returns
    /// <c>null</c> when the text cannot be an id of that type, which callers treat as "not found".
    /// </summary>
    protected object? ParseId(string id)
    {
        var type = this.idProperty?.PropertyType;
        if (type is null || type == typeof(string))
            return id;

        if (type == typeof(Guid))
            return Guid.TryParse(id, out var g) ? g : null;
        if (type == typeof(int))
            return int.TryParse(id, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i) ? i : null;
        if (type == typeof(long))
            return long.TryParse(id, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l) ? l : null;

        // A MapIdType custom id: read it the way the document's own JSON would, as a string token first and
        // then as a bare literal (a numeric wrapper type, say).
        // The custom type must be resolvable from the store's serializer options; if it is not, the text is
        // passed through and the store reports the mismatch as before.
        JsonTypeInfo info;
        try
        {
            info = this.Registration.JsonTypeInfo.Options.GetTypeInfo(type);
        }
        catch (NotSupportedException)
        {
            return id;
        }
        return TryDeserialize(JsonValue.Create(id).ToJsonString(), info)
            ?? TryDeserialize(id, info);
    }

    static object? TryDeserialize(string json, JsonTypeInfo info)
    {
        try
        {
            return JsonSerializer.Deserialize(json, info);
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or FormatException)
        {
            return null;
        }
    }

    /// <summary>
    /// Drops every hidden property from a document headed back to the LLM. With nothing hidden the node is
    /// returned untouched.
    /// </summary>
    protected JsonNode? Redact(JsonNode? node)
    {
        if (this.hiddenProperties.Length == 0 || node is not JsonObject obj)
            return node;

        var drop = obj
            .Select(kv => kv.Key)
            .Where(key => !this.visibleNames.Contains(key))
            .ToList();
        foreach (var key in drop)
            obj.Remove(key);

        return obj;
    }

    /// <summary>
    /// Removes hidden properties from an LLM-supplied document body, so the model cannot write a field it
    /// was never shown. Matched case-insensitively — the serializer may bind <c>Embedding</c> to
    /// <c>embedding</c>.
    /// </summary>
    protected string StripHidden(string json)
    {
        if (this.hiddenProperties.Length == 0 || JsonNode.Parse(json) is not JsonObject obj)
            return json;

        var drop = obj
            .Select(kv => kv.Key)
            .Where(key => this.hiddenProperties.Any(p => string.Equals(p.Name, key, StringComparison.OrdinalIgnoreCase)))
            .ToList();
        foreach (var key in drop)
            obj.Remove(key);

        return obj.ToJsonString();
    }

    /// <summary>
    /// Carries the hidden properties of the stored document over to its replacement, so an LLM update
    /// (which never sees them) does not wipe them.
    /// </summary>
    protected void CopyHiddenProperties(T from, T to)
    {
        foreach (var prop in this.hiddenProperties.Where(p => p.Get is not null && p.Set is not null))
            prop.Set!(to, prop.Get!(from));
    }

    /// <summary>Reads a typed argument from the LLM-supplied bag, or returns <paramref name="fallback"/> if absent.</summary>
    protected static TValue? GetArg<TValue>(AIFunctionArguments arguments, string key, TValue? fallback = default)
        => AIArguments.Get(arguments, key, fallback);
}
