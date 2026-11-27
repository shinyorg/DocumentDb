using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The one seam every provider passes a document body through on its way into and out of the store, for the
/// features that depend on the body's shape rather than on the backend:
/// <list type="bullet">
/// <item>schema versioning — upcasting old documents on read and stamping the current version on write; and</item>
/// <item>inheritance — storing derived types under their base type, serializing each by its runtime type with its
/// discriminator set, and reading each back as the type its discriminator names.</item>
/// </list>
/// What it knows is a property of the CLR document types — how their JSON has changed over time and how they derive
/// from one another — so it is held process-wide, keyed by type, the same way <see cref="SoftDelete"/> keeps its
/// mappings. A type with nothing registered costs one generation check per call.
/// </summary>
static class DocumentShapes
{
    /// <summary>The JSON property that carries a document's schema version.</summary>
    public const string VersionProperty = "_v";

    static readonly object gate = new();
    static readonly Dictionary<Type, SortedDictionary<int, Action<JsonObject>>> upcasters = new();
    static readonly Dictionary<Type, Hierarchy> hierarchies = new();     // root → hierarchy
    static readonly Dictionary<Type, Type> roots = new();                // derived → root
    static readonly Dictionary<Type, Func<LambdaExpression, LambdaExpression>> retargeters = new();
    static readonly ConditionalWeakTable<object, object> upcastInstances = new();
    static readonly object marker = new();
    static readonly ConcurrentDictionary<Type, (int Generation, ShapePlan? Plan)> planCache = new();
    static int generation;

    // ── Registration ────────────────────────────────────────────────────

    /// <summary>Registers the step that turns version <paramref name="fromVersion"/> of <paramref name="type"/>'s JSON
    /// into version <paramref name="fromVersion"/> + 1. Registering the same step again replaces it.</summary>
    public static void AddUpcaster(Type type, int fromVersion, Action<JsonObject> upcast)
    {
        ArgumentNullException.ThrowIfNull(upcast);
        if (fromVersion < 1)
            throw new ArgumentOutOfRangeException(nameof(fromVersion), fromVersion, "Schema versions start at 1: the first upcaster turns version 1 into version 2.");

        lock (gate)
        {
            if (!upcasters.TryGetValue(type, out var chain))
                upcasters[type] = chain = new SortedDictionary<int, Action<JsonObject>>();
            chain[fromVersion] = upcast;
            generation++;
        }
    }

    /// <summary>
    /// Declares <paramref name="root"/> as the base of a stored hierarchy whose discriminator is
    /// <paramref name="discriminator"/>; <paramref name="rootValue"/> is the root's own value when it is not abstract.
    /// </summary>
    public static void RegisterHierarchy(Type root, PropertyInfo discriminator, string? rootValue)
    {
        lock (gate)
        {
            if (roots.TryGetValue(root, out var parent))
                throw new InvalidOperationException($"'{root.Name}' is already mapped as a derived type of '{parent.Name}'; map the hierarchy on '{parent.Name}'.");
            if (hierarchies.TryGetValue(root, out var existing) && existing.Discriminator.Name != discriminator.Name)
                throw new InvalidOperationException($"'{root.Name}' already uses '{existing.Discriminator.Name}' as its discriminator; it cannot also use '{discriminator.Name}'.");

            var hierarchy = existing ?? new Hierarchy(root, discriminator);
            hierarchy.SetValue(root, rootValue);
            hierarchies[root] = hierarchy;
            generation++;
        }
    }

    /// <summary>Maps <paramref name="derived"/> into <paramref name="root"/>'s hierarchy under <paramref name="value"/>.</summary>
    public static void RegisterDerived(Type root, Type derived, string value, Func<LambdaExpression, LambdaExpression> retarget)
    {
        ArgumentNullException.ThrowIfNull(retarget);
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        lock (gate)
        {
            var hierarchy = hierarchies[root];
            if (hierarchy.ByValue.TryGetValue(value, out var taken) && taken != derived)
                throw new InvalidOperationException($"Discriminator '{value}' is already mapped to '{taken.Name}' in the '{root.Name}' hierarchy.");
            if (roots.TryGetValue(derived, out var otherRoot) && otherRoot != root)
                throw new InvalidOperationException($"'{derived.Name}' is already mapped into the '{otherRoot.Name}' hierarchy.");

            hierarchy.SetValue(derived, value);
            roots[derived] = root;
            retargeters[derived] = retarget;
            generation++;
        }
    }

    /// <summary>Re-targets a predicate over one of <paramref name="derived"/>'s base types at <paramref name="derived"/>.</summary>
    public static LambdaExpression Retarget(Type derived, LambdaExpression predicate)
    {
        lock (gate)
            return retargeters[derived](predicate);
    }

    /// <summary>The stored base type of <paramref name="type"/> when it is a mapped derived type; null otherwise.</summary>
    public static Type? RootOf(Type type)
    {
        if (Volatile.Read(ref generation) == 0)
            return null;
        lock (gate)
            return roots.TryGetValue(type, out var root) ? root : null;
    }

    /// <summary>
    /// The discriminator values a query for <paramref name="type"/> must match — its own and those of every mapped
    /// type that derives from it — for building the query filter.
    /// </summary>
    public static IReadOnlyList<string> DiscriminatorValuesFor(Type root, Type type)
    {
        lock (gate)
            return hierarchies[root].ByType.Where(x => type.IsAssignableFrom(x.Key)).Select(x => x.Value).ToList();
    }

    /// <summary>The version a document of <paramref name="type"/> is written at, or null when it is not versioned.</summary>
    public static int? CurrentVersion(Type type) => Plan(type)?.CurrentVersion;

    // ── Write ───────────────────────────────────────────────────────────

    /// <summary>
    /// Serializes a document for storage: by its runtime type (with its discriminator set) when it belongs to a mapped
    /// hierarchy, then stamped with the current schema version when the type is versioned.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    public static string Serialize<T>(T value, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
    {
        var plan = Plan(typeof(T));
        if (plan == null)
            return typeInfo != null ? JsonSerializer.Serialize(value, typeInfo) : JsonSerializer.Serialize(value, options);

        string json;
        if (plan.Hierarchy != null && value is not null)
        {
            var runtime = value.GetType();
            plan.Hierarchy.Stamp(value, runtime);
            json = runtime == typeof(T) && typeInfo != null
                ? JsonSerializer.Serialize(value, typeInfo)
                : options.TypeInfoResolver != null
                    ? JsonSerializer.Serialize(value, options.GetTypeInfo(runtime))
                    : JsonSerializer.Serialize(value, runtime, options);
        }
        else
        {
            json = typeInfo != null ? JsonSerializer.Serialize(value, typeInfo) : JsonSerializer.Serialize(value, options);
        }
        return StampVersion(plan, json);
    }

    /// <summary>
    /// Adds the current schema version to a serialized <typeparamref name="T"/> when the type is versioned; returns
    /// the JSON untouched otherwise. A body that already carries the version property keeps its own value.
    /// </summary>
    public static string StampWrite<T>(string json) => StampWrite(typeof(T), json);

    /// <inheritdoc cref="StampWrite{T}(string)"/>
    public static string StampWrite(Type type, string json)
    {
        var plan = Plan(type);
        return plan == null ? json : StampVersion(plan, json);
    }

    static string StampVersion(ShapePlan plan, string json)
    {
        if (plan.CurrentVersion is not { } current || ReadTopLevelVersion(json, out _))
            return json;

        var end = json.LastIndexOf('}');
        if (end < 0)
            return json;

        var empty = json.AsSpan(0, end).TrimEnd().EndsWith("{");
        var sb = new StringBuilder(json.Length + 16);
        sb.Append(json, 0, end);
        if (!empty)
            sb.Append(',');
        sb.Append('"').Append(VersionProperty).Append("\":").Append(current);
        sb.Append(json, end, json.Length - end);
        return sb.ToString();
    }

    // ── Read ────────────────────────────────────────────────────────────

    /// <summary>
    /// Brings a stored <typeparamref name="T"/> body up to the current schema version. Returns true (and the
    /// rewritten JSON) when an upcaster ran; false, with the JSON untouched, when the body is already current, was
    /// written by a newer version, or the type is not versioned.
    /// </summary>
    public static bool TryUpcast<T>(ref string json) => TryUpcast(typeof(T), ref json);

    /// <inheritdoc cref="TryUpcast{T}(ref string)"/>
    public static bool TryUpcast(Type type, ref string json)
    {
        var plan = Plan(type);
        return plan != null && TryUpcast(plan, ref json);
    }

    static bool TryUpcast(ShapePlan plan, ref string json)
    {
        if (plan.CurrentVersion is not { } current)
            return false;

        var version = ReadTopLevelVersion(json, out var stored) ? stored : 1;
        if (version >= current)
            return false;

        if (JsonNode.Parse(json) is not JsonObject node)
            return false;

        for (var v = version; v < current; v++)
        {
            plan.Steps[v - 1](node);
            node[VersionProperty] = v + 1;
        }
        json = node.ToJsonString();
        return true;
    }

    /// <summary>
    /// Deserializes a stored <typeparamref name="T"/> body: upcast first when it is older than the current schema
    /// version, then read as the type its discriminator names when <typeparamref name="T"/> belongs to a mapped
    /// hierarchy. Returns <c>default</c> for a body whose discriminator names a type that is not a
    /// <typeparamref name="T"/> (a sibling in the hierarchy). A document that was upcast is remembered (weakly) so
    /// <see cref="SchemaVersioningExtensions.UpgradeDocuments{T}"/> can rewrite just those.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when typeInfo is null (reflection fallback).")]
    public static T? Deserialize<T>(string json, JsonTypeInfo<T>? typeInfo, JsonSerializerOptions options)
    {
        var plan = Plan(typeof(T));
        if (plan == null)
            return typeInfo != null ? JsonSerializer.Deserialize(json, typeInfo) : JsonSerializer.Deserialize<T>(json, options);

        var upcast = TryUpcast(plan, ref json);
        T? document;
        var concrete = plan.Hierarchy?.Resolve(json, options) ?? typeof(T);
        if (concrete == typeof(T))
            document = typeInfo != null ? JsonSerializer.Deserialize(json, typeInfo) : JsonSerializer.Deserialize<T>(json, options);
        else if (typeof(T).IsAssignableFrom(concrete))
            document = options.TypeInfoResolver != null
                ? (T?)JsonSerializer.Deserialize(json, options.GetTypeInfo(concrete))
                : (T?)JsonSerializer.Deserialize(json, concrete, options);
        else
            return default;

        if (upcast && document is not null)
            upcastInstances.AddOrUpdate(document, marker);
        return document;
    }

    /// <summary>True when <paramref name="document"/> was materialized from a body older than the current version.</summary>
    public static bool WasUpcast(object document) => upcastInstances.TryGetValue(document, out _);

    /// <summary>Forgets that <paramref name="document"/> was upcast — once it has been rewritten at the current version.</summary>
    public static void ClearUpcast(object document) => upcastInstances.Remove(document);

    // ── Internals ───────────────────────────────────────────────────────

    sealed class Hierarchy(Type root, PropertyInfo discriminator)
    {
        public Type Root { get; } = root;
        public PropertyInfo Discriminator { get; } = discriminator;
        public Dictionary<string, Type> ByValue { get; } = new(StringComparer.Ordinal);
        public Dictionary<Type, string> ByType { get; } = new();
        (JsonSerializerOptions? Options, string Name) jsonName;

        public void SetValue(Type type, string? value)
        {
            if (this.ByType.Remove(type, out var previous))
                this.ByValue.Remove(previous);
            if (value == null)
                return;
            this.ByType[type] = value;
            this.ByValue[value] = type;
        }

        // Sets the discriminator property to the runtime type's value, so the stored body (and the caller's instance)
        // always say what the document is.
        public void Stamp(object document, Type runtime)
        {
            if (this.ByType.TryGetValue(runtime, out var value))
                this.Discriminator.SetValue(document, value);
            else if (runtime != this.Root)
                throw new InvalidOperationException(
                    $"'{runtime.Name}' is not mapped in the '{this.Root.Name}' hierarchy. Add it with MapDerivedTypes(..., d => d.Add<{runtime.Name}>(\"...\")).");
        }

        // The concrete type a body's discriminator names: a mapped type, or the root for a missing/unknown value.
        public Type Resolve(string json, JsonSerializerOptions options)
        {
            var name = this.JsonName(options);
            return ReadTopLevelString(json, name) is { } value && this.ByValue.TryGetValue(value, out var type)
                ? type
                : this.Root;
        }

        string JsonName(JsonSerializerOptions options)
        {
            var cached = this.jsonName;
            if (ReferenceEquals(cached.Options, options))
                return cached.Name;

            var name = this.Discriminator.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name
                       ?? options.PropertyNamingPolicy?.ConvertName(this.Discriminator.Name)
                       ?? this.Discriminator.Name;
            this.jsonName = (options, name);
            return name;
        }
    }

    sealed class ShapePlan(int? currentVersion, Action<JsonObject>[] steps, Hierarchy? hierarchy)
    {
        public int? CurrentVersion { get; } = currentVersion;
        public Action<JsonObject>[] Steps { get; } = steps;
        public Hierarchy? Hierarchy { get; } = hierarchy;
    }

    static ShapePlan? Plan(Type type)
    {
        var current = Volatile.Read(ref generation);
        if (current == 0)
            return null;
        if (planCache.TryGetValue(type, out var cached) && cached.Generation == current)
            return cached.Plan;

        lock (gate)
        {
            var plan = Build(type);
            planCache[type] = (generation, plan);
            return plan;
        }
    }

    // A derived type shares its root's plan: one upcaster chain and one discriminator for the whole hierarchy.
    static ShapePlan? Build(Type type)
    {
        var root = roots.TryGetValue(type, out var r) ? r : type;
        hierarchies.TryGetValue(root, out var hierarchy);

        int? currentVersion = null;
        var steps = Array.Empty<Action<JsonObject>>();
        if (upcasters.TryGetValue(root, out var chain) && chain.Count > 0)
        {
            var last = chain.Keys.Max();
            steps = new Action<JsonObject>[last];
            for (var v = 1; v <= last; v++)
            {
                if (!chain.TryGetValue(v, out var step))
                    throw new InvalidOperationException(
                        $"'{root.Name}' has upcasters up to version {last} but none from version {v}. Register one upcaster for every version from 1 to {last}.");
                steps[v - 1] = step;
            }
            currentVersion = last + 1;
        }

        return currentVersion == null && hierarchy == null ? null : new ShapePlan(currentVersion, steps, hierarchy);
    }

    // Reads the top-level version property without materializing the document.
    static bool ReadTopLevelVersion(string json, out int version)
    {
        version = 0;
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
        return SeekTopLevel(ref reader, VersionProperty)
               && reader.TokenType == JsonTokenType.Number
               && reader.TryGetInt32(out version);
    }

    static string? ReadTopLevelString(string json, string property)
    {
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
        return SeekTopLevel(ref reader, property) && reader.TokenType == JsonTokenType.String ? reader.GetString() : null;
    }

    // Leaves the reader on the value of the named top-level property; false when there is none.
    static bool SeekTopLevel(ref Utf8JsonReader reader, string property)
    {
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            return false;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            var isTarget = reader.TokenType == JsonTokenType.PropertyName && reader.ValueTextEquals(property);
            reader.Read();
            if (isTarget)
                return true;
            reader.Skip();
        }
        return false;
    }
}
