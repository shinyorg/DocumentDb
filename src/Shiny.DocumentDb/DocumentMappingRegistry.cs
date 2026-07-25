using System.Linq.Expressions;
using System.Text.Json;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// The per-type mapping state every store's options carries — table/collection names, id overrides and
/// converters, query filters, version/temporal/full-text/computed/blob mappings, and the interceptor pipeline.
/// <para>
/// An options class <b>holds</b> one of these and delegates to it, keeping its own strongly-typed fluent
/// methods (which return the concrete options type). That way a new mapping concept is implemented once here
/// rather than re-implemented in every provider's options class, which is how these drifted before.
/// </para>
/// </summary>
public sealed class DocumentMappingRegistry
{
    readonly Dictionary<string, string> typeNameMappings = new();
    readonly HashSet<string> mappedNames = new(StringComparer.OrdinalIgnoreCase);
    readonly Dictionary<Type, string> idPropertyOverrides = new();
    readonly Dictionary<Type, List<QueryFilter>> queryFilters = new();
    readonly Dictionary<Type, VersionMapping> versionMappings = new();
    readonly Dictionary<Type, TemporalMapping> temporalMappings = new();
    readonly Dictionary<Type, FullTextMapping> fullTextMappings = new();
    readonly Dictionary<Type, List<BlobMapping>> blobMappings = new();

    /// <summary>Custom document-id CLR types (Ulid, strongly-typed wrappers…).</summary>
    public IdConverterRegistry IdConverters { get; } = new();

    /// <summary>Computed (alias / materialized) property mappings.</summary>
    public ComputedMappingRegistry Computed { get; } = new();

    /// <summary>The store's write interceptors.</summary>
    public InterceptorPipeline Interceptors { get; } = new();

    // ── Type → table / collection / container name ──────────────────────

    /// <summary>Maps a document type name to its own table/collection/container.</summary>
    /// <exception cref="ArgumentException">Another type is already mapped to that name.</exception>
    public void MapTypeName(string typeName, string mappedName, string paramName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(mappedName);
        if (!this.mappedNames.Add(mappedName))
            throw new ArgumentException($"'{mappedName}' is already mapped to another type.", paramName);
        this.typeNameMappings[typeName] = mappedName;
    }

    /// <summary>The mapped name for a type name, or <paramref name="fallback"/> when it is not mapped.</summary>
    public string ResolveMappedName(string typeName, string fallback)
        => this.typeNameMappings.TryGetValue(typeName, out var mapped) ? mapped : fallback;

    /// <summary>True when the type name has its own table/collection/container.</summary>
    public bool IsMapped(string typeName) => this.typeNameMappings.ContainsKey(typeName);

    // ── Id property override ────────────────────────────────────────────

    public void MapIdProperty<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        ArgumentNullException.ThrowIfNull(idProperty);
        this.idPropertyOverrides[typeof(T)] = ExtractPropertyName(idProperty);
    }

    public void MapIdProperty<T>(string propertyName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        this.idPropertyOverrides[typeof(T)] = propertyName;
    }

    public string? ResolveIdPropertyName(Type type)
        => this.idPropertyOverrides.TryGetValue(type, out var name) ? name : null;

    // ── Query filters ───────────────────────────────────────────────────

    public void AddQueryFilter<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);
        if (!this.queryFilters.TryGetValue(typeof(T), out var list))
            this.queryFilters[typeof(T)] = list = new List<QueryFilter>();
        list.Add(new QueryFilter(name, predicate));
    }

    public IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type)
        => this.queryFilters.TryGetValue(type, out var list) ? list : Array.Empty<QueryFilter>();

    // ── Optimistic concurrency ──────────────────────────────────────────

    public void MapVersionProperty<T>(Expression<Func<T, int>> property) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        if (property.Body is not MemberExpression member)
            throw new ArgumentException("Expression must be a simple property access.", nameof(property));

        var propertyName = member.Member.Name;
        var propInfo = typeof(T).GetProperty(propertyName)
            ?? throw new ArgumentException($"Property '{propertyName}' not found on type '{typeof(T).Name}'.");

        this.versionMappings[typeof(T)] = new VersionMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            GetVersion = obj => (int)propInfo.GetValue(obj)!,
            SetVersion = (obj, v) => propInfo.SetValue(obj, v)
        };
    }

    public void MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        this.versionMappings[typeof(T)] = new VersionMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            GetVersion = obj => getter((T)obj),
            SetVersion = (obj, v) => setter((T)obj, v)
        };
    }

    public VersionMapping? ResolveVersionMapping(Type type)
        => this.versionMappings.TryGetValue(type, out var mapping) ? mapping : null;

    /// <summary>Resolves each version property's JSON path once the serializer options are known.</summary>
    public void ResolveVersionJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.versionMappings.Values)
        {
            if (mapping.JsonPath != null!)
                continue;
            mapping.JsonPath = jsonOptions.PropertyNamingPolicy?.ConvertName(mapping.PropertyName) ?? mapping.PropertyName;
        }
    }

    // ── Temporal ────────────────────────────────────────────────────────

    public void MapTemporal<T>(Action<TemporalOptions>? configure) where T : class
    {
        var options = new TemporalOptions();
        configure?.Invoke(options);
        if (options.MaxVersions is <= 0)
            throw new ArgumentOutOfRangeException(nameof(configure), "TemporalOptions.MaxVersions must be greater than zero.");

        this.temporalMappings[typeof(T)] = new TemporalMapping
        {
            DocumentType = typeof(T),
            Retention = options.Retention,
            MaxVersions = options.MaxVersions,
            CaptureActor = options.CaptureActor,
            ResolveActor = options.ResolveActor
        };
    }

    internal TemporalMapping? ResolveTemporalMapping(Type type)
        => this.temporalMappings.TryGetValue(type, out var mapping) ? mapping : null;

    // ── Full text ───────────────────────────────────────────────────────

    public void MapFullTextProperty<T>(IReadOnlyList<Expression<Func<T, string?>>> properties, FullTextLanguage language) where T : class
    {
        ArgumentNullException.ThrowIfNull(properties);
        this.fullTextMappings[typeof(T)] = FullTextMappingFactory.FromExpressions(properties, language);
    }

    public void MapFullTextProperty<T>(IReadOnlyList<string> propertyNames, Func<T, IEnumerable<string?>> textSelector, FullTextLanguage language) where T : class
        => this.fullTextMappings[typeof(T)] = FullTextMappingFactory.FromAccessor(propertyNames, textSelector, language);

    /// <summary>Resolves each full-text mapping's JSON paths once the serializer options are known.</summary>
    public void ResolveFullTextJsonPaths(JsonSerializerOptions jsonOptions)
        => FullTextMappingFactory.ResolveJsonPaths(this.fullTextMappings.Values, jsonOptions);

    /// <summary>Every mapped full-text type — for schema/index creation at table init.</summary>
    public IReadOnlyDictionary<Type, FullTextMapping> FullTextMappings => this.fullTextMappings;

    /// <summary>Every temporal-mapped type — for history-table creation at init.</summary>
    public IEnumerable<Type> TemporalTypes => this.temporalMappings.Keys;

    public FullTextMapping? ResolveFullTextMapping(Type type)
        => this.fullTextMappings.TryGetValue(type, out var mapping) ? mapping : null;

    // ── Blobs ───────────────────────────────────────────────────────────

    public void MapBlob<T>(Expression<Func<T, DocumentBlob?>> property, Action<BlobOptions>? configure) where T : class
    {
        var options = new BlobOptions();
        configure?.Invoke(options);
        this.AddBlob(BlobMappingFactory.FromExpression(property, options));
    }

    public void MapBlobCollection<T>(Expression<Func<T, DocumentBlobCollection?>> property, Action<BlobOptions>? configure) where T : class
    {
        var options = new BlobOptions();
        configure?.Invoke(options);
        this.AddBlob(BlobMappingFactory.FromCollectionExpression(property, options));
    }

    /// <summary>Adds (or replaces, by property name) a blob mapping built by the caller.</summary>
    public void AddBlobMapping(BlobMapping mapping) => this.AddBlob(mapping);

    void AddBlob(BlobMapping mapping)
    {
        if (!this.blobMappings.TryGetValue(mapping.DocumentType, out var list))
            this.blobMappings[mapping.DocumentType] = list = new List<BlobMapping>();
        list.RemoveAll(m => m.PropertyName.Equals(mapping.PropertyName, StringComparison.Ordinal));
        list.Add(mapping);
    }

    public IReadOnlyList<BlobMapping> ResolveBlobMappings(Type type)
        => this.blobMappings.TryGetValue(type, out var list) ? list : Array.Empty<BlobMapping>();

    // ── Computed ────────────────────────────────────────────────────────

    public IReadOnlyList<ComputedMapping> ResolveComputedMappings(Type type) => this.Computed.Resolve(type);

    public IReadOnlyDictionary<string, ComputedMapping>? ResolveComputedLookup(Type type) => this.Computed.ResolveLookup(type);

    public void ResolveComputedJsonNames(JsonSerializerOptions jsonOptions) => this.Computed.ResolveJsonNames(jsonOptions);

    internal static string ExtractPropertyName<T>(Expression<Func<T, object>> expression)
    {
        var body = expression.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;

        if (body is MemberExpression member)
            return member.Member.Name;

        throw new ArgumentException(
            "Expression must be a simple property access (e.g., x => x.MyId).",
            nameof(expression));
    }
}
