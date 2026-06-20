using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

public class CosmosDbDocumentStoreOptions
{
    readonly Dictionary<string, string> typeMappings = new();
    readonly HashSet<string> mappedContainerNames = new(StringComparer.OrdinalIgnoreCase);
    readonly Dictionary<Type, string> idPropertyOverrides = new();
    readonly IdConverterRegistry idConverters = new();
    readonly Dictionary<Type, List<QueryFilter>> queryFilters = new();
    internal readonly Dictionary<Type, VersionMapping> versionMappings = new();
    internal readonly Dictionary<Type, CosmosDbSpatialMapping> spatialMappings = new();
    internal readonly Dictionary<Type, VectorMapping> vectorMappings = new();
    internal readonly Dictionary<Type, TemporalMapping> temporalMappings = new();

    public required string ConnectionString { get; set; }
    public required string DatabaseName { get; set; }
    public string ContainerName { get; set; } = "documents";
    public TypeNameResolution TypeNameResolution { get; set; } = TypeNameResolution.ShortName;
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws an
    /// InvalidOperationException if the type cannot be resolved from the configured TypeInfoResolver.
    /// Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>
    /// Optional callback invoked with diagnostic messages (Cosmos SQL queries).
    /// </summary>
    public Action<string>? Logging { get; set; }

    /// <summary>
    /// Default throughput for auto-created containers. Defaults to 400 RU/s.
    /// </summary>
    public int DefaultThroughput { get; set; } = 400;

    /// <summary>
    /// Optional pre-configured CosmosClient. When null, a new client is created from <see cref="ConnectionString"/>.
    /// </summary>
    public CosmosClient? CosmosClient { get; set; }

    /// <summary>
    /// Maps a document type to its own dedicated container.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapTypeToContainer<T>() where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToContainer<T>(typeName);
    }

    /// <summary>
    /// Maps a document type to its own dedicated container with a custom Id property.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapTypeToContainer<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToContainer<T>(typeName, idProperty);
    }

    /// <summary>
    /// Maps a document type to a dedicated container with the specified name.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapTypeToContainer<T>(string containerName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);

        if (!this.mappedContainerNames.Add(containerName))
            throw new ArgumentException($"Container '{containerName}' is already mapped to another type.", nameof(containerName));

        this.typeMappings[typeName] = containerName;
        return this;
    }

    /// <summary>
    /// Maps a document type to a dedicated container with the specified name and a custom Id property.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapTypeToContainer<T>(string containerName, Expression<Func<T, object>> idProperty) where T : class
    {
        this.MapTypeToContainer<T>(containerName);
        this.idPropertyOverrides[typeof(T)] = ExtractPropertyName(idProperty);
        return this;
    }

    internal string ResolveContainerName(string typeName)
        => this.typeMappings.TryGetValue(typeName, out var container) ? container : this.ContainerName;

    /// <summary>
    /// Registers a converter so a document Id can be a CLR type beyond Guid/int/long/string
    /// (e.g. a <c>Ulid</c> or a strongly-typed wrapper). The Id is still stored as a string.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.idConverters.Register(converter);
        return this;
    }

    /// <summary>Registers a custom Id type using inline delegates.</summary>
    public CosmosDbDocumentStoreOptions MapIdType<TId>(
        Func<TId, string> toString,
        Func<string, TId> parse,
        Func<TId, bool>? isDefault = null,
        Func<TId>? generate = null)
    {
        ArgumentNullException.ThrowIfNull(toString);
        ArgumentNullException.ThrowIfNull(parse);
        this.idConverters.Register(new DelegateIdConverter<TId>(toString, parse, isDefault, generate));
        return this;
    }

    internal string? ResolveIdPropertyName(Type type)
        => this.idPropertyOverrides.TryGetValue(type, out var name) ? name : null;

    internal IdConverterRegistry IdConverters => this.idConverters;

    /// <summary>
    /// Registers a global query filter for <typeparamref name="T"/>. See
    /// <see cref="DocumentStoreOptions.AddQueryFilter{T}(Expression{Func{T, bool}})"/> for semantics.
    /// </summary>
    public CosmosDbDocumentStoreOptions AddQueryFilter<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(null, predicate);
    }

    /// <summary>Registers a named global query filter for <typeparamref name="T"/>.</summary>
    public CosmosDbDocumentStoreOptions AddQueryFilter<T>(string name, Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(name, predicate);
    }

    CosmosDbDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        if (!this.queryFilters.TryGetValue(typeof(T), out var list))
            this.queryFilters[typeof(T)] = list = new List<QueryFilter>();
        list.Add(new QueryFilter(name, predicate));
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type)
        => this.queryFilters.TryGetValue(type, out var list) ? list : Array.Empty<QueryFilter>();

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors { get; } = new();

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public CosmosDbDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public CosmosDbDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    /// <summary>Registers a before-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public CosmosDbDocumentStoreOptions OnBeforeWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddBefore<T>(handler); return this; }

    /// <summary>Registers an after-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public CosmosDbDocumentStoreOptions OnAfterWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddAfter<T>(handler); return this; }

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public CosmosDbDocumentStoreOptions MapVersionProperty<T>(Expression<Func<T, int>> property) where T : class
    {
        var body = property.Body;
        if (body is not MemberExpression member)
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
        return this;
    }

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency. AOT-safe overload.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        this.versionMappings[typeof(T)] = new VersionMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            GetVersion = obj => getter((T)obj),
            SetVersion = (obj, v) => setter((T)obj, v)
        };
        return this;
    }

    internal VersionMapping? ResolveVersionMapping(Type type)
        => this.versionMappings.TryGetValue(type, out var mapping) ? mapping : null;

    /// <summary>
    /// Enables append-only system-time temporal history for <typeparamref name="T"/>. Every
    /// Insert/Update/Upsert/Remove writes a versioned snapshot to a <c>{container}_history</c> sidecar
    /// container (partitioned by <c>/typeName</c>), so the document's state can be read back as of any
    /// point in time via the <see cref="ITemporalDocumentStore"/> methods (History/AsOf/Restore/…).
    /// Opt-in and per type — only mapped types incur the extra history write. Bulk <c>Clear</c> records
    /// no history.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapTemporal<T>(Action<TemporalOptions>? configure = null) where T : class
    {
        var opts = new TemporalOptions();
        configure?.Invoke(opts);
        if (opts.MaxVersions is <= 0)
            throw new ArgumentOutOfRangeException(nameof(configure), "TemporalOptions.MaxVersions must be greater than zero.");

        this.temporalMappings[typeof(T)] = new TemporalMapping
        {
            DocumentType = typeof(T),
            Retention = opts.Retention,
            MaxVersions = opts.MaxVersions,
            CaptureActor = opts.CaptureActor
        };
        return this;
    }

    internal TemporalMapping? ResolveTemporalMapping(Type type)
        => this.temporalMappings.TryGetValue(type, out var mapping) ? mapping : null;

    internal void ResolveVersionJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.versionMappings.Values)
        {
            if (mapping.JsonPath != null!)
                continue;
            var jsonName = jsonOptions.PropertyNamingPolicy?.ConvertName(mapping.PropertyName) ?? mapping.PropertyName;
            mapping.JsonPath = jsonName;
        }
    }

    /// <summary>
    /// Declares that type T has a GeoPoint property to be used for spatial queries.
    /// The property will be serialized as GeoJSON and indexed with a CosmosDB spatial index.
    /// For full AOT safety, use the overload accepting a string propertyName and Func delegate.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression; the type is user-constructed and not subject to trimming.")]
    public CosmosDbDocumentStoreOptions MapSpatialProperty<T>(Expression<Func<T, GeoPoint>> property) where T : class
    {
        var body = property.Body;
        if (body is not MemberExpression member)
            throw new ArgumentException(
                "Expression must be a simple property access (e.g., x => x.Location).",
                nameof(property));

        var propertyName = member.Member.Name;
        var propInfo = typeof(T).GetProperty(propertyName)
            ?? throw new ArgumentException($"Property '{propertyName}' not found on type '{typeof(T).Name}'.");

        this.spatialMappings[typeof(T)] = new CosmosDbSpatialMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            GetGeoPoint = obj => (GeoPoint)propInfo.GetValue(obj)!
        };
        return this;
    }

    /// <summary>
    /// Declares that type T has a GeoPoint property to be used for spatial queries.
    /// AOT-safe overload that accepts a direct accessor delegate.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapSpatialProperty<T>(string propertyName, Func<T, GeoPoint> accessor) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(accessor);

        this.spatialMappings[typeof(T)] = new CosmosDbSpatialMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            GetGeoPoint = obj => accessor((T)obj)
        };
        return this;
    }

    internal CosmosDbSpatialMapping? ResolveSpatialMapping(Type type) =>
        this.spatialMappings.TryGetValue(type, out var mapping) ? mapping : null;

    internal void ResolveSpatialJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.spatialMappings.Values)
        {
            if (mapping.JsonPath != null)
                continue;

            var jsonName = jsonOptions.PropertyNamingPolicy?.ConvertName(mapping.PropertyName) ?? mapping.PropertyName;
            mapping.JsonPath = jsonName;
        }
    }

    /// <summary>
    /// Declares that type T has a <see cref="ReadOnlyMemory{T}"/> embedding property to be used
    /// for ANN vector search. The container's VectorEmbeddingPolicy and IndexingPolicy will be
    /// configured on first use to include this path.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public CosmosDbDocumentStoreOptions MapVectorProperty<T>(
        Expression<Func<T, ReadOnlyMemory<float>>> property,
        int dimensions,
        VectorDistance metric = VectorDistance.Cosine,
        VectorIndexKind indexKind = VectorIndexKind.DiskAnn,
        Action<VectorIndexOptions>? configureIndex = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        if (dimensions <= 0)
            throw new ArgumentOutOfRangeException(nameof(dimensions));

        if (property.Body is not MemberExpression member)
            throw new ArgumentException("Expression must be a simple property access.", nameof(property));

        var propertyName = member.Member.Name;
        var propInfo = typeof(T).GetProperty(propertyName)
            ?? throw new ArgumentException($"Property '{propertyName}' not found on type '{typeof(T).Name}'.");

        var indexOpts = new VectorIndexOptions();
        configureIndex?.Invoke(indexOpts);

        this.vectorMappings[typeof(T)] = new VectorMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            JsonPath = null!,
            Dimensions = dimensions,
            Metric = metric,
            IndexKind = indexKind,
            IndexOptions = indexOpts,
            GetVector = obj => (ReadOnlyMemory<float>)propInfo.GetValue(obj)!,
            SetVector = (obj, v) => propInfo.SetValue(obj, v)
        };
        return this;
    }

    /// <summary>AOT-safe overload that accepts direct accessor + setter delegates.</summary>
    public CosmosDbDocumentStoreOptions MapVectorProperty<T>(
        string propertyName,
        Func<T, ReadOnlyMemory<float>> getter,
        Action<T, ReadOnlyMemory<float>> setter,
        int dimensions,
        VectorDistance metric = VectorDistance.Cosine,
        VectorIndexKind indexKind = VectorIndexKind.DiskAnn,
        Action<VectorIndexOptions>? configureIndex = null) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(getter);
        ArgumentNullException.ThrowIfNull(setter);
        if (dimensions <= 0)
            throw new ArgumentOutOfRangeException(nameof(dimensions));

        var indexOpts = new VectorIndexOptions();
        configureIndex?.Invoke(indexOpts);

        this.vectorMappings[typeof(T)] = new VectorMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            JsonPath = null!,
            Dimensions = dimensions,
            Metric = metric,
            IndexKind = indexKind,
            IndexOptions = indexOpts,
            GetVector = obj => getter((T)obj),
            SetVector = (obj, v) => setter((T)obj, v)
        };
        return this;
    }

    internal VectorMapping? ResolveVectorMapping(Type type) =>
        this.vectorMappings.TryGetValue(type, out var mapping) ? mapping : null;

    internal void ResolveVectorJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.vectorMappings.Values)
        {
            if (mapping.JsonPath != null!)
                continue;
            var jsonName = jsonOptions.PropertyNamingPolicy?.ConvertName(mapping.PropertyName) ?? mapping.PropertyName;
            mapping.JsonPath = jsonName;
        }
    }

    static string ExtractPropertyName<T>(Expression<Func<T, object>> expression)
    {
        var body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;

        if (body is MemberExpression member)
            return member.Member.Name;

        throw new ArgumentException(
            "Expression must be a simple property access (e.g., x => x.MyId).",
            nameof(expression));
    }
}

internal class CosmosDbSpatialMapping
{
    public required Type DocumentType { get; init; }
    public required string PropertyName { get; init; }
    public string? JsonPath { get; set; }
    public required Func<object, GeoPoint> GetGeoPoint { get; init; }
}
