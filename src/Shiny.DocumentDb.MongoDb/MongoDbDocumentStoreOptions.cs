using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using MongoDB.Driver;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.MongoDb;

public class MongoDbDocumentStoreOptions : IDocumentStoreOptions
{
    /// <summary>The shared per-type mapping state — see <see cref="DocumentMappingRegistry"/>.</summary>
    internal DocumentMappingRegistry Mappings { get; } = new();

    readonly Dictionary<string, string> typeMappings = new();
    readonly HashSet<string> mappedCollectionNames = new(StringComparer.OrdinalIgnoreCase);
    internal readonly Dictionary<Type, VectorMapping> vectorMappings = new();
    internal readonly Dictionary<Type, MongoDbSpatialMapping> spatialMappings = new();

    public required string ConnectionString { get; set; }
    public required string DatabaseName { get; set; }
    public string CollectionName { get; set; } = "documents";
    public TypeNameResolution TypeNameResolution { get; set; } = TypeNameResolution.ShortName;
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws an
    /// InvalidOperationException if the type cannot be resolved from the configured TypeInfoResolver.
    /// Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>
    /// Optional callback invoked with diagnostic messages.
    /// </summary>
    public Action<string>? Logging { get; set; }

    /// <summary>
    /// Optional pre-configured MongoClient. When null, a new client is created from <see cref="ConnectionString"/>.
    /// </summary>
    public IMongoClient? MongoClient { get; set; }

    /// <summary>
    /// Maps a document type to its own dedicated collection.
    /// </summary>
    public MongoDbDocumentStoreOptions MapTypeToCollection<T>() where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToCollection<T>(typeName);
    }

    /// <summary>
    /// Maps a document type to its own dedicated collection with a custom Id property.
    /// </summary>
    public MongoDbDocumentStoreOptions MapTypeToCollection<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToCollection<T>(typeName, idProperty);
    }

    /// <summary>
    /// Maps a document type to a dedicated collection with the specified name.
    /// </summary>
    public MongoDbDocumentStoreOptions MapTypeToCollection<T>(string collectionName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);

        if (!this.mappedCollectionNames.Add(collectionName))
            throw new ArgumentException($"Collection '{collectionName}' is already mapped to another type.", nameof(collectionName));

        this.typeMappings[typeName] = collectionName;
        return this;
    }

    /// <summary>
    /// Maps a document type to a dedicated collection with the specified name and a custom Id property.
    /// </summary>
    public MongoDbDocumentStoreOptions MapTypeToCollection<T>(string collectionName, Expression<Func<T, object>> idProperty) where T : class
    {
        this.MapTypeToCollection<T>(collectionName);
        this.Mappings.MapIdProperty(idProperty);
        return this;
    }

    internal string ResolveCollectionName(string typeName)
        => this.typeMappings.TryGetValue(typeName, out var collection) ? collection : this.CollectionName;

    /// <summary>
    /// Registers a converter so a document Id can be a CLR type beyond Guid/int/long/string
    /// (e.g. a <c>Ulid</c> or a strongly-typed wrapper). The Id is still stored as a string.
    /// </summary>
    public MongoDbDocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.Mappings.IdConverters.Register(converter);
        return this;
    }

    /// <summary>Registers a custom Id type using inline delegates.</summary>
    public MongoDbDocumentStoreOptions MapIdType<TId>(
        Func<TId, string> toString,
        Func<string, TId> parse,
        Func<TId, bool>? isDefault = null,
        Func<TId>? generate = null)
    {
        ArgumentNullException.ThrowIfNull(toString);
        ArgumentNullException.ThrowIfNull(parse);
        this.Mappings.IdConverters.Register(new DelegateIdConverter<TId>(toString, parse, isDefault, generate));
        return this;
    }

    internal string? ResolveIdPropertyName(Type type) => this.Mappings.ResolveIdPropertyName(type);

    internal IdConverterRegistry IdConverters => this.Mappings.IdConverters;

    /// <summary>
    /// Registers a global query filter for <typeparamref name="T"/>. See
    /// <see cref="DocumentStoreOptions.AddQueryFilter{T}(Expression{Func{T, bool}})"/> for semantics.
    /// </summary>
    public MongoDbDocumentStoreOptions AddQueryFilter<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(null, predicate);
    }

    /// <summary>Registers a named global query filter for <typeparamref name="T"/>.</summary>
    public MongoDbDocumentStoreOptions AddQueryFilter<T>(string name, Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(name, predicate);
    }

    MongoDbDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors { get; } = new();

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public MongoDbDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public MongoDbDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    /// <summary>Registers a before-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public MongoDbDocumentStoreOptions OnBeforeWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddBefore<T>(handler); return this; }

    /// <summary>Registers an after-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public MongoDbDocumentStoreOptions OnAfterWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddAfter<T>(handler); return this; }

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public MongoDbDocumentStoreOptions MapVersionProperty<T>(Expression<Func<T, int>> property) where T : class
    {
        this.Mappings.MapVersionProperty(property);
        return this;
    }

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency. AOT-safe overload.
    /// </summary>
    public MongoDbDocumentStoreOptions MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
    {
        this.Mappings.MapVersionProperty(propertyName, getter, setter);
        return this;
    }

    internal VersionMapping? ResolveVersionMapping(Type type) => this.Mappings.ResolveVersionMapping(type);

    /// <summary>
    /// Enables append-only system-time temporal history for <typeparamref name="T"/>. Every
    /// Insert/Update/Upsert/Remove writes a versioned snapshot to a <c>{collection}_history</c> sidecar
    /// collection, so the document's state can be read back as of any point in time via the
    /// <see cref="ITemporalDocumentStore"/> methods (History/AsOf/Restore/GetDiffBetween/…). Opt-in and
    /// per type — only mapped types incur the extra history write. Bulk <c>Clear</c> records no history.
    /// </summary>
    public MongoDbDocumentStoreOptions MapTemporal<T>(Action<TemporalOptions>? configure = null) where T : class
    {
        this.Mappings.MapTemporal<T>(configure);
        return this;
    }

    internal TemporalMapping? ResolveTemporalMapping(Type type) => this.Mappings.ResolveTemporalMapping(type);

    internal void ResolveVersionJsonPaths(JsonSerializerOptions jsonOptions)
    {
        this.Mappings.ResolveVersionJsonPaths(jsonOptions);
    }

    /// <summary>
    /// Declares a <see cref="ReadOnlyMemory{T}"/> embedding property for ANN vector search.
    /// Requires MongoDB Atlas Vector Search — non-Atlas connections throw at NearestVectors call time.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public MongoDbDocumentStoreOptions MapVectorProperty<T>(
        Expression<Func<T, ReadOnlyMemory<float>>> property,
        int dimensions,
        VectorDistance metric = VectorDistance.Cosine,
        VectorIndexKind indexKind = VectorIndexKind.Hnsw,
        Action<VectorIndexOptions>? configureIndex = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        if (dimensions <= 0) throw new ArgumentOutOfRangeException(nameof(dimensions));

        if (property.Body is not MemberExpression member)
            throw new ArgumentException("Expression must be a simple property access.", nameof(property));

        var propertyName = member.Member.Name;
        var propInfo = typeof(T).GetProperty(propertyName)
            ?? throw new ArgumentException($"Property '{propertyName}' not found on '{typeof(T).Name}'.");

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
    public MongoDbDocumentStoreOptions MapVectorProperty<T>(
        string propertyName,
        Func<T, ReadOnlyMemory<float>> getter,
        Action<T, ReadOnlyMemory<float>> setter,
        int dimensions,
        VectorDistance metric = VectorDistance.Cosine,
        VectorIndexKind indexKind = VectorIndexKind.Hnsw,
        Action<VectorIndexOptions>? configureIndex = null) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(getter);
        ArgumentNullException.ThrowIfNull(setter);
        if (dimensions <= 0) throw new ArgumentOutOfRangeException(nameof(dimensions));

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

    internal VectorMapping? ResolveVectorMapping(Type type)
        => this.vectorMappings.TryGetValue(type, out var mapping) ? mapping : null;

    // ── Spatial (2dsphere) ────────────────────────────────────────────────

    /// <summary>Declares a <see cref="GeoPoint"/> property for spatial queries.</summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public MongoDbDocumentStoreOptions MapSpatialProperty<T>(Expression<Func<T, GeoPoint?>> property) where T : class
    {
        var (name, info) = ResolveMember<T>(property.Body);
        this.spatialMappings[typeof(T)] = new MongoDbSpatialMapping
        {
            DocumentType = typeof(T), PropertyName = name, JsonPath = null!,
            GetGeoPoint = obj => (GeoPoint?)info.GetValue(obj)
        };
        return this;
    }

    /// <summary>AOT-safe point overload accepting a direct accessor delegate.</summary>
    public MongoDbDocumentStoreOptions MapSpatialProperty<T>(string propertyName, Func<T, GeoPoint?> accessor) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(accessor);
        this.spatialMappings[typeof(T)] = new MongoDbSpatialMapping
        {
            DocumentType = typeof(T), PropertyName = propertyName, JsonPath = null!,
            GetGeoPoint = obj => accessor((T)obj)
        };
        return this;
    }

    /// <summary>Declares a full geometry property for spatial queries.</summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public MongoDbDocumentStoreOptions MapSpatialProperty<T>(Expression<Func<T, Geometry?>> property) where T : class
    {
        var (name, info) = ResolveMember<T>(property.Body);
        return this.MapSpatialProperty<T>(name, obj => (Geometry?)info.GetValue(obj));
    }

    /// <summary>AOT-safe geometry overload accepting a direct accessor delegate.</summary>
    public MongoDbDocumentStoreOptions MapSpatialProperty<T>(string propertyName, Func<T, Geometry?> accessor) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(accessor);
        this.spatialMappings[typeof(T)] = new MongoDbSpatialMapping
        {
            DocumentType = typeof(T), PropertyName = propertyName, JsonPath = null!,
            GetGeometry = obj => accessor((T)obj),
            GetGeoPoint = obj => accessor((T)obj) is Internal.GeoPointGeometry pg ? pg.Point : null
        };
        return this;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property resolved by name from a user-provided expression.")]
    static (string name, System.Reflection.PropertyInfo info) ResolveMember<T>(Expression body)
    {
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;
        if (body is not MemberExpression member)
            throw new ArgumentException("Expression must be a simple property access (e.g., x => x.Area).", nameof(body));
        var name = member.Member.Name;
        var info = typeof(T).GetProperty(name)
            ?? throw new ArgumentException($"Property '{name}' not found on '{typeof(T).Name}'.");
        return (name, info);
    }

    internal MongoDbSpatialMapping? ResolveSpatialMapping(Type type)
        => this.spatialMappings.TryGetValue(type, out var mapping) ? mapping : null;

    internal void ResolveSpatialJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.spatialMappings.Values)
        {
            if (mapping.JsonPath != null!)
                continue;
            mapping.JsonPath = jsonOptions.PropertyNamingPolicy?.ConvertName(mapping.PropertyName) ?? mapping.PropertyName;
        }
    }

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

    /// <summary>
    /// Declares a string property as full-text searchable via a MongoDB <c>$text</c> index. One text
    /// index per collection — a single full-text-mapped type per collection is the supported shape.
    /// See <see cref="DocumentStoreOptions.MapFullTextProperty{T}(Expression{Func{T, string}}, FullTextLanguage)"/>.
    /// </summary>
    public MongoDbDocumentStoreOptions MapFullTextProperty<T>(
        Expression<Func<T, string?>> property,
        FullTextLanguage language = FullTextLanguage.English) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        this.Mappings.MapFullTextProperty<T>([property], language);
        return this;
    }

    /// <summary>Declares several string properties combined into one <c>$text</c> index.</summary>
    public MongoDbDocumentStoreOptions MapFullTextProperty<T>(
        IReadOnlyList<Expression<Func<T, string?>>> properties,
        FullTextLanguage language = FullTextLanguage.English) where T : class
    {
        ArgumentNullException.ThrowIfNull(properties);
        this.Mappings.MapFullTextProperty(properties, language);
        return this;
    }

    /// <summary>AOT-safe overload mapping full-text to a direct text selector (combine fields or index a string collection).</summary>
    public MongoDbDocumentStoreOptions MapFullTextProperty<T>(
        IReadOnlyList<string> propertyNames,
        Func<T, IEnumerable<string?>> textSelector,
        FullTextLanguage language = FullTextLanguage.English) where T : class
    {
        this.Mappings.MapFullTextProperty(propertyNames, textSelector, language);
        return this;
    }

    internal FullTextMapping? ResolveFullTextMapping(Type type) => this.Mappings.ResolveFullTextMapping(type);

    internal void ResolveFullTextJsonPaths(JsonSerializerOptions jsonOptions)
        => this.Mappings.ResolveFullTextJsonPaths(jsonOptions);


    /// <summary>Maps a computed property — a derived value not stored in the document JSON that can be
    /// filtered, sorted, projected, and read back as a normal property.</summary>
    public MongoDbDocumentStoreOptions MapComputedProperty<T, TValue>(Expression<Func<T, TValue>> property, Expression<Func<T, TValue>> definition, bool indexed = false) where T : class
    {
        this.Mappings.Computed.Add(ComputedMappingFactory.FromExpression(property, definition, indexed));
        return this;
    }

    /// <summary>AOT-clean overload taking the property name and an explicit setter delegate.</summary>
    public MongoDbDocumentStoreOptions MapComputedProperty<T, TValue>(string propertyName, Expression<Func<T, TValue>> definition, Action<T, TValue> setter, bool indexed = false) where T : class
    {
        this.Mappings.Computed.Add(ComputedMappingFactory.FromExpression(propertyName, definition, setter, indexed));
        return this;
    }

    internal IReadOnlyList<ComputedMapping> ResolveComputedMappings(Type type) => this.Mappings.ResolveComputedMappings(type);
    internal IReadOnlyDictionary<string, ComputedMapping>? ResolveComputedLookup(Type type) => this.Mappings.ResolveComputedLookup(type);
    internal void ResolveComputedJsonNames(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveComputedJsonNames(jsonOptions);

    // ── Blobs ──────────────────────────────────────────────────────────────

    /// <summary>See <see cref="DocumentStoreOptions.MapBlob{T}(Expression{Func{T, DocumentBlob}}, Action{BlobOptions})"/>.</summary>
    public MongoDbDocumentStoreOptions MapBlob<T>(Expression<Func<T, DocumentBlob?>> property, Action<BlobOptions>? configure = null) where T : class
    {
        var o = new BlobOptions();
        configure?.Invoke(o);
        this.AddBlob(BlobMappingFactory.FromExpression(property, o));
        return this;
    }

    /// <summary>See <see cref="DocumentStoreOptions.MapBlobCollection{T}(Expression{Func{T, DocumentBlobCollection}}, Action{BlobOptions})"/>.</summary>
    public MongoDbDocumentStoreOptions MapBlobCollection<T>(Expression<Func<T, DocumentBlobCollection?>> property, Action<BlobOptions>? configure = null) where T : class
    {
        var o = new BlobOptions();
        configure?.Invoke(o);
        this.AddBlob(BlobMappingFactory.FromCollectionExpression(property, o));
        return this;
    }

    void AddBlob(BlobMapping mapping)
    {
        this.Mappings.AddBlobMapping(mapping);
    }

    internal IReadOnlyList<BlobMapping> ResolveBlobMappings(Type type) => this.Mappings.ResolveBlobMappings(type);

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

    // ── IDocumentStoreOptions (explicit — the provider-agnostic slice; the typed overloads above stay fluent) ──
    IDocumentStoreOptions IDocumentStoreOptions.AddInterceptor(IDocumentInterceptor interceptor)
        => this.AddInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddBulkInterceptor(IDocumentBulkInterceptor interceptor)
        => this.AddBulkInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddQueryFilter<T>(string? name, Expression<Func<T, bool>> predicate)
        => name == null ? this.AddQueryFilter(predicate) : this.AddQueryFilter(name, predicate);
}

internal class MongoDbSpatialMapping
{
    public required Type DocumentType { get; init; }
    public required string PropertyName { get; init; }
    public required string JsonPath { get; set; }
    public required Func<object, GeoPoint?> GetGeoPoint { get; init; }
    public Func<object, Geometry?>? GetGeometry { get; init; }

    public Geometry? ResolveGeometry(object document)
    {
        if (this.GetGeometry != null)
            return this.GetGeometry(document);
        var point = this.GetGeoPoint(document);
        return point is null ? null : (Geometry)point.Value;
    }
}
