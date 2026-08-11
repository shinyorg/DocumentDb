using System.Diagnostics.CodeAnalysis;
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
    readonly Dictionary<Type, SpatialMapping> spatialMappings = new();
    readonly Dictionary<Type, VectorMapping> vectorMappings = new();

    /// <summary>Custom document-id CLR types (Ulid, strongly-typed wrappers…).</summary>
    public IdConverterRegistry IdConverters { get; } = new();

    /// <summary>Computed (alias / materialized) property mappings.</summary>
    public ComputedMappingRegistry Computed { get; } = new();

    /// <summary>The store's write interceptors.</summary>
    public InterceptorPipeline Interceptors { get; } = new();

    // ── Type → table / collection / container name ──────────────────────

    /// <summary>
    /// Maps a document type name to its <b>own</b> table/collection/container.
    /// </summary>
    /// <remarks>
    /// A custom name is exclusive to one document type — this is the rule, not an implementation limit.
    /// <c>cfg.Table</c> means "this type lives somewhere of its own", so two types claiming the same name are
    /// making contradictory statements and the second one loses. Several types <i>sharing</i> a table is the
    /// separate, supported thing: leave <c>Table</c> unset on all of them and set
    /// <c>DocumentStoreOptions.TableName</c>, which puts every type in that one table discriminated by
    /// <c>TypeName</c>.
    /// </remarks>
    /// <exception cref="ArgumentException">Another type is already mapped to that name.</exception>
    public void MapTypeName(string typeName, string mappedName, string paramName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(mappedName);

        // Re-mapping the same type is allowed — a ConfigureDocument block can set cfg.Table after the
        // generated [Document] configuration already did. Two types sharing one name is still an error.
        if (this.typeNameMappings.TryGetValue(typeName, out var previous))
        {
            if (string.Equals(previous, mappedName, StringComparison.OrdinalIgnoreCase))
                return;
            this.mappedNames.Remove(previous);
        }

        if (!this.mappedNames.Add(mappedName))
            throw new ArgumentException(
                $"'{mappedName}' is already mapped to another type, and a custom table is exclusive to one type " +
                $"— '{typeName}' cannot share it. To put several types in the same table, leave Table unset on " +
                "all of them and set DocumentStoreOptions.TableName instead; they are discriminated by TypeName.",
                paramName);

        this.typeNameMappings[typeName] = mappedName;
    }

    /// <summary>The mapped name for a type name, or <paramref name="fallback"/> when it is not mapped.</summary>
    public string ResolveMappedName(string typeName, string fallback)
        => this.typeNameMappings.TryGetValue(typeName, out var mapped) ? mapped : fallback;

    /// <summary>True when the type name has its own table/collection/container.</summary>
    public bool IsMapped(string typeName) => this.typeNameMappings.ContainsKey(typeName);

    /// <summary>Every explicitly mapped table/collection/container name.</summary>
    public IReadOnlyCollection<string> MappedNames => this.mappedNames;

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

    public void MapVersionProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression<Func<T, int>> property) where T : class
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

    /// <summary>Every mapped version property — for the configuration validation pass.</summary>
    public IEnumerable<VersionMapping> VersionMappings => this.versionMappings.Values;

    /// <summary>Resolves each version property's JSON path once the serializer options are known.</summary>
    public void ResolveVersionJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.versionMappings.Values)
        {
            if (mapping.JsonPath != null!)
                continue;
            mapping.JsonPath = JsonPropertyNameResolver.ResolveJsonName(jsonOptions, mapping.DocumentType, mapping.PropertyName);
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

    internal IEnumerable<TemporalMapping> TemporalMappings => this.temporalMappings.Values;

    // ── Full text ───────────────────────────────────────────────────────

    public void MapFullTextProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(IReadOnlyList<Expression<Func<T, string?>>> properties, FullTextLanguage language) where T : class
    {
        ArgumentNullException.ThrowIfNull(properties);
        this.AddSingle(this.fullTextMappings, FullTextMappingFactory.FromExpressions(properties, language), typeof(T), "full-text", m => string.Join(", ", m.PropertyNames));
    }

    public void MapFullTextProperty<T>(IReadOnlyList<string> propertyNames, Func<T, IEnumerable<string?>> textSelector, FullTextLanguage language) where T : class
        => this.AddSingle(this.fullTextMappings, FullTextMappingFactory.FromAccessor(propertyNames, textSelector, language), typeof(T), "full-text", m => string.Join(", ", m.PropertyNames));

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

    public void MapBlob<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression<Func<T, DocumentBlob?>> property, Action<BlobOptions>? configure) where T : class
    {
        var options = new BlobOptions();
        configure?.Invoke(options);
        this.AddBlob(BlobMappingFactory.FromExpression(property, options));
    }

    public void MapBlobCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression<Func<T, DocumentBlobCollection?>> property, Action<BlobOptions>? configure) where T : class
    {
        var options = new BlobOptions();
        configure?.Invoke(options);
        this.AddBlob(BlobMappingFactory.FromCollectionExpression(property, options));
    }

    public void MapBlob<T>(
        string propertyName,
        Func<T, DocumentBlob?> getter,
        Action<T, DocumentBlob?> setter,
        Action<BlobOptions>? configure) where T : class
    {
        var options = new BlobOptions();
        configure?.Invoke(options);
        this.AddBlob(BlobMappingFactory.FromAccessors(propertyName, getter, setter, options));
    }

    public void MapBlobCollection<T>(
        string propertyName,
        Func<T, DocumentBlobCollection?> getter,
        Action<T, DocumentBlobCollection?> setter,
        Action<BlobOptions>? configure) where T : class
    {
        var options = new BlobOptions();
        configure?.Invoke(options);
        this.AddBlob(BlobMappingFactory.FromCollectionAccessors(propertyName, getter, setter, options));
    }

    /// <summary>Adds (or replaces, by property name) a blob mapping built by the caller.</summary>
    public void AddBlobMapping(BlobMapping mapping) => this.AddBlob(mapping);

    void AddBlob(BlobMapping mapping)
    {
        if (!this.blobMappings.TryGetValue(mapping.DocumentType, out var list))
            this.blobMappings[mapping.DocumentType] = list = new List<BlobMapping>();

        list.RemoveAll(m => m.PropertyName.Equals(mapping.PropertyName, StringComparison.Ordinal));

        var duplicate = list.Find(m => !m.IsCollection && !mapping.IsCollection && string.Equals(m.Key, mapping.Key, StringComparison.OrdinalIgnoreCase));
        if (duplicate != null)
            throw new ArgumentException(
                $"Blob key '{mapping.Key}' is already mapped on '{mapping.DocumentType.Name}' by property '{duplicate.PropertyName}'. Set a distinct Key via the BlobOptions overload.");

        list.Add(mapping);
    }

    public IReadOnlyList<BlobMapping> ResolveBlobMappings(Type type)
        => this.blobMappings.TryGetValue(type, out var list) ? list : Array.Empty<BlobMapping>();

    /// <summary>Every blob-mapped type — for sidecar table creation at init.</summary>
    public IReadOnlyDictionary<Type, List<BlobMapping>> BlobMappings => this.blobMappings;

    // ── Spatial ─────────────────────────────────────────────────────────

    public void MapSpatialProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression<Func<T, GeoPoint?>> property) where T : class
        => this.AddSpatial<T>(SpatialMappingFactory.FromPointExpression(property));

    public void MapSpatialProperty<T>(string propertyName, Func<T, GeoPoint?> accessor) where T : class
        => this.AddSpatial<T>(SpatialMappingFactory.FromPointAccessor(propertyName, accessor));

    public void MapSpatialProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression<Func<T, Geometry?>> property) where T : class
        => this.AddSpatial<T>(SpatialMappingFactory.FromGeometryExpression(property));

    public void MapSpatialProperty<T>(string propertyName, Func<T, Geometry?> accessor) where T : class
        => this.AddSpatial<T>(SpatialMappingFactory.FromGeometryAccessor(propertyName, accessor));

    void AddSpatial<T>(SpatialMapping mapping) where T : class
        => this.AddSingle(this.spatialMappings, mapping, typeof(T), "spatial", m => m.PropertyName);

    public SpatialMapping? ResolveSpatialMapping(Type type)
        => this.spatialMappings.TryGetValue(type, out var mapping) ? mapping : null;

    /// <summary>Every mapped spatial type — for index creation at init and capability probes.</summary>
    public IReadOnlyDictionary<Type, SpatialMapping> SpatialMappings => this.spatialMappings;

    /// <summary>Resolves each spatial property's JSON path once the serializer options are known.</summary>
    public void ResolveSpatialJsonPaths(JsonSerializerOptions jsonOptions)
        => SpatialMappingFactory.ResolveJsonPaths(this.spatialMappings.Values, jsonOptions);

    static readonly IReadOnlySet<string> NoSpatialPaths = new HashSet<string>(StringComparer.Ordinal);
    readonly System.Collections.Concurrent.ConcurrentDictionary<Type, IReadOnlySet<string>> spatialPathCache = new();

    /// <summary>
    /// The JSON paths a spatial predicate may address for this type — empty when it has no spatial mapping.
    /// The query pipeline validates a <c>DocumentFunctions</c> geometry argument against this, because most
    /// providers serve those predicates from the spatial sidecar (which only holds the mapped geometry) and
    /// would otherwise silently answer about a different property.
    /// </summary>
    public IReadOnlySet<string> SpatialJsonPathsFor(Type type)
        => this.spatialMappings.Count == 0
            ? NoSpatialPaths
            : this.spatialPathCache.GetOrAdd(type, t => this.spatialMappings.TryGetValue(t, out var m)
                ? new HashSet<string>(StringComparer.Ordinal) { m.JsonPath }
                : NoSpatialPaths);

    // ── Vector ──────────────────────────────────────────────────────────

    public void MapVectorProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        Expression<Func<T, ReadOnlyMemory<float>>> property,
        int dimensions,
        VectorDistance metric,
        VectorIndexKind? indexKind,
        Action<VectorIndexOptions>? configureIndex) where T : class
        => this.AddSingle(
            this.vectorMappings,
            VectorMappingFactory.FromExpression(property, dimensions, metric, indexKind, configureIndex),
            typeof(T),
            "vector",
            m => m.PropertyName);

    public void MapVectorProperty<T>(
        string propertyName,
        Func<T, ReadOnlyMemory<float>> getter,
        Action<T, ReadOnlyMemory<float>> setter,
        int dimensions,
        VectorDistance metric,
        VectorIndexKind? indexKind,
        Action<VectorIndexOptions>? configureIndex) where T : class
        => this.AddSingle(
            this.vectorMappings,
            VectorMappingFactory.FromAccessors(propertyName, getter, setter, dimensions, metric, indexKind, configureIndex),
            typeof(T),
            "vector",
            m => m.PropertyName);

    public VectorMapping? ResolveVectorMapping(Type type)
        => this.vectorMappings.TryGetValue(type, out var mapping) ? mapping : null;

    /// <summary>Every mapped vector type — for index creation at init and capability probes.</summary>
    public IReadOnlyDictionary<Type, VectorMapping> VectorMappings => this.vectorMappings;

    /// <summary>
    /// Fills in the effective index kind for every vector mapping that did not name one — each provider
    /// passes its own default (DiskANN on Cosmos, HNSW elsewhere). Called at store construction.
    /// </summary>
    public void ResolveVectorIndexKinds(VectorIndexKind providerDefault)
    {
        foreach (var mapping in this.vectorMappings.Values)
            mapping.IndexKind = mapping.RequestedIndexKind ?? providerDefault;
    }

    /// <summary>Resolves each vector property's JSON path once the serializer options are known.</summary>
    public void ResolveVectorJsonPaths(JsonSerializerOptions jsonOptions)
        => VectorMappingFactory.ResolveJsonPaths(this.vectorMappings.Values, jsonOptions);

    // A document type carries at most one spatial / vector / full-text mapping. Replacing one silently
    // was the old behavior and hid real configuration mistakes (two MapSpatialProperty calls in the same
    // ConfigureDocument block), so a second mapping now says so.
    void AddSingle<TMapping>(
        Dictionary<Type, TMapping> target,
        TMapping mapping,
        Type documentType,
        string kind,
        Func<TMapping, string> describe)
    {
        if (target.TryGetValue(documentType, out var existing))
            throw new InvalidOperationException(
                $"'{documentType.Name}' already has a {kind} mapping on '{describe(existing)}'; it cannot also be mapped on '{describe(mapping)}'. A document type carries one {kind} mapping.");

        target[documentType] = mapping;
    }

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
