using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Shiny.DocumentDb.Internal;
using StackExchange.Redis;

namespace Shiny.DocumentDb.Redis;

/// <summary>
/// Options for the Redis Stack document store (RedisJSON + RediSearch). Each document is a RedisJSON key
/// <c>doc:{typeName}:{id}</c>; declaring queryable fields with <see cref="MapIndexedProperty"/> /
/// <see cref="MapFullTextProperty"/> / <see cref="MapVectorProperty"/> / <see cref="MapSpatialProperty"/>
/// builds a per-type RediSearch index so those predicates push down server-side. Fields that are not declared
/// are still stored and filterable, but only client-side.
/// </summary>
public class RedisDocumentStoreOptions : IDocumentStoreOptions
{
    /// <summary>The shared per-type mapping state — see <see cref="DocumentMappingRegistry"/>.</summary>
    internal DocumentMappingRegistry Mappings { get; } = new();

    internal readonly Dictionary<Type, List<RedisIndexedSpec>> indexedSpecs = new();
    internal readonly Dictionary<Type, VectorMapping> vectorMappings = new();
    internal readonly Dictionary<Type, RedisSpatialMapping> spatialMappings = new();

    /// <summary>A pre-built multiplexer (shared across the app / tests). Wins over <see cref="ConnectionString"/>.</summary>
    public IConnectionMultiplexer? Multiplexer { get; set; }

    /// <summary>A StackExchange.Redis connection string (e.g. <c>localhost:6379</c>). Used when <see cref="Multiplexer"/> is null.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>The logical Redis database index. Defaults to -1 (the multiplexer's default database).</summary>
    public int Database { get; set; } = -1;

    /// <summary>
    /// An optional key-namespace prefix applied to every document key, sequence counter, and search index
    /// (<c>{namespace}:doc:{typeName}:{id}</c>). Use it to isolate multiple logical stores sharing one Redis
    /// database (the test suite maps each case to its own namespace). Empty by default.
    /// </summary>
    public string KeyNamespace { get; set; } = "";

    public TypeNameResolution TypeNameResolution { get; set; } = TypeNameResolution.ShortName;
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws if the type
    /// cannot be resolved from the configured TypeInfoResolver. Set false in AOT deployments. Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>Optional diagnostic callback.</summary>
    public Action<string>? Logging { get; set; }

    // ── Id mapping ──────────────────────────────────────────────────────

    /// <summary>Maps a document type to a custom Id property.</summary>
    public RedisDocumentStoreOptions MapIdProperty<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        this.Mappings.MapIdProperty(idProperty);
        return this;
    }

    internal string? ResolveIdPropertyName(Type type) => this.Mappings.ResolveIdPropertyName(type);

    /// <summary>Registers a converter so a document Id can be a CLR type beyond Guid/int/long/string.</summary>
    public RedisDocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.Mappings.IdConverters.Register(converter);
        return this;
    }

    /// <summary>Registers a custom Id type using inline delegates.</summary>
    public RedisDocumentStoreOptions MapIdType<TId>(
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

    internal IdConverterRegistry IdConverters => this.Mappings.IdConverters;

    // ── Query filters ───────────────────────────────────────────────────

    /// <summary>Registers an unnamed global query filter for <typeparamref name="T"/>.</summary>
    public RedisDocumentStoreOptions AddQueryFilter<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(null, predicate);
    }

    /// <summary>Registers a named global query filter for <typeparamref name="T"/>.</summary>
    public RedisDocumentStoreOptions AddQueryFilter<T>(string name, Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(name, predicate);
    }

    RedisDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors { get; } = new();

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public RedisDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public RedisDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    /// <summary>Registers a before-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public RedisDocumentStoreOptions OnBeforeWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddBefore<T>(handler); return this; }

    /// <summary>Registers an after-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public RedisDocumentStoreOptions OnAfterWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddAfter<T>(handler); return this; }

    // ── Version / CAS ───────────────────────────────────────────────────

    /// <summary>
    /// Maps an <c>int</c> version property for optimistic concurrency. Insert seeds version 1; Update/Upsert
    /// check the stored version, increment it, and guard the swap with a Lua CAS script on the <c>version</c>
    /// envelope field. A version drift throws <see cref="ConcurrencyException"/>.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public RedisDocumentStoreOptions MapVersionProperty<T>(Expression<Func<T, int>> property) where T : class
    {
        this.Mappings.MapVersionProperty(property);
        return this;
    }

    /// <summary>AOT-safe version-property overload.</summary>
    public RedisDocumentStoreOptions MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
    {
        this.Mappings.MapVersionProperty(propertyName, getter, setter);
        return this;
    }

    internal VersionMapping? ResolveVersionMapping(Type type) => this.Mappings.ResolveVersionMapping(type);

    // ── Indexed properties (RediSearch TAG / NUMERIC pushdown) ──────────

    /// <summary>
    /// Declares a scalar property as a queryable RediSearch schema field so LINQ <c>Where</c> predicates and
    /// <c>OrderBy</c> over it push down to <c>FT.SEARCH</c>/<c>FT.AGGREGATE</c>. Numeric CLR types index as
    /// NUMERIC (ranges + sort); everything else indexes as TAG (exact match + <c>in</c>). Undeclared fields
    /// still work but are filtered client-side. Map before first use.
    /// </summary>
    public RedisDocumentStoreOptions MapIndexedProperty<T>(Expression<Func<T, object>> property) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        var (segments, memberType) = ExtractPathAndType(property);
        if (!this.indexedSpecs.TryGetValue(typeof(T), out var list))
            this.indexedSpecs[typeof(T)] = list = new List<RedisIndexedSpec>();
        list.Add(new RedisIndexedSpec(segments, IsNumericType(memberType)));
        return this;
    }

    internal IReadOnlyList<RedisIndexedSpec> ResolveIndexedSpecs(Type type)
        => this.indexedSpecs.TryGetValue(type, out var list) ? list : Array.Empty<RedisIndexedSpec>();

    // ── Vector ──────────────────────────────────────────────────────────

    /// <summary>Declares a <see cref="ReadOnlyMemory{T}"/> embedding property for RediSearch KNN vector search.</summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public RedisDocumentStoreOptions MapVectorProperty<T>(
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

    /// <summary>AOT-safe vector overload accepting accessor + setter delegates.</summary>
    public RedisDocumentStoreOptions MapVectorProperty<T>(
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

    // ── Full-text ───────────────────────────────────────────────────────

    /// <summary>Declares a string property as full-text searchable via a RediSearch TEXT index field.</summary>
    public RedisDocumentStoreOptions MapFullTextProperty<T>(
        Expression<Func<T, string?>> property,
        FullTextLanguage language = FullTextLanguage.English) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        this.Mappings.MapFullTextProperty<T>([property], language);
        return this;
    }

    /// <summary>Declares several string properties combined into the type's TEXT index.</summary>
    public RedisDocumentStoreOptions MapFullTextProperty<T>(
        IReadOnlyList<Expression<Func<T, string?>>> properties,
        FullTextLanguage language = FullTextLanguage.English) where T : class
    {
        ArgumentNullException.ThrowIfNull(properties);
        this.Mappings.MapFullTextProperty(properties, language);
        return this;
    }

    internal FullTextMapping? ResolveFullTextMapping(Type type) => this.Mappings.ResolveFullTextMapping(type);

    // ── Spatial ─────────────────────────────────────────────────────────

    /// <summary>Declares a <see cref="GeoPoint"/> property for radius / bounding-box / nearest queries.</summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public RedisDocumentStoreOptions MapSpatialProperty<T>(Expression<Func<T, GeoPoint?>> property) where T : class
    {
        var (name, info) = ResolveMember<T>(property.Body);
        this.spatialMappings[typeof(T)] = new RedisSpatialMapping
        {
            DocumentType = typeof(T),
            PropertyName = name,
            JsonPath = null!,
            GetGeoPoint = obj => (GeoPoint?)info.GetValue(obj)
        };
        return this;
    }

    /// <summary>AOT-safe spatial overload accepting a direct accessor delegate.</summary>
    public RedisDocumentStoreOptions MapSpatialProperty<T>(string propertyName, Func<T, GeoPoint?> accessor) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(accessor);
        this.spatialMappings[typeof(T)] = new RedisSpatialMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            JsonPath = null!,
            GetGeoPoint = obj => accessor((T)obj)
        };
        return this;
    }

    internal RedisSpatialMapping? ResolveSpatialMapping(Type type)
        => this.spatialMappings.TryGetValue(type, out var mapping) ? mapping : null;

    // ── Computed ────────────────────────────────────────────────────────

    /// <summary>Maps a computed property — a derived value not stored in the document JSON that can be
    /// read back as a normal property.</summary>
    public RedisDocumentStoreOptions MapComputedProperty<T, TValue>(Expression<Func<T, TValue>> property, Expression<Func<T, TValue>> definition, bool indexed = false) where T : class
    {
        this.Mappings.Computed.Add(ComputedMappingFactory.FromExpression(property, definition, indexed));
        return this;
    }

    /// <summary>AOT-clean overload taking the property name and an explicit setter delegate.</summary>
    public RedisDocumentStoreOptions MapComputedProperty<T, TValue>(string propertyName, Expression<Func<T, TValue>> definition, Action<T, TValue> setter, bool indexed = false) where T : class
    {
        this.Mappings.Computed.Add(ComputedMappingFactory.FromExpression(propertyName, definition, setter, indexed));
        return this;
    }

    internal IReadOnlyList<ComputedMapping> ResolveComputedMappings(Type type) => this.Mappings.ResolveComputedMappings(type);

    // ── Blobs ──────────────────────────────────────────────────────────────

    /// <summary>See <see cref="DocumentStoreOptions.MapBlob{T}(Expression{Func{T, DocumentBlob}}, Action{BlobOptions})"/>.</summary>
    public RedisDocumentStoreOptions MapBlob<T>(Expression<Func<T, DocumentBlob?>> property, Action<BlobOptions>? configure = null) where T : class
    {
        var o = new BlobOptions();
        configure?.Invoke(o);
        this.AddBlob(BlobMappingFactory.FromExpression(property, o));
        return this;
    }

    /// <summary>See <see cref="DocumentStoreOptions.MapBlobCollection{T}(Expression{Func{T, DocumentBlobCollection}}, Action{BlobOptions})"/>.</summary>
    public RedisDocumentStoreOptions MapBlobCollection<T>(Expression<Func<T, DocumentBlobCollection?>> property, Action<BlobOptions>? configure = null) where T : class
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

    // ── JSON-path resolution (deferred until the JsonSerializerOptions are known) ─────

    internal void ResolveVersionJsonPaths(JsonSerializerOptions jsonOptions)
    {
        this.Mappings.ResolveVersionJsonPaths(jsonOptions);
    }

    internal void ResolveVectorJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.vectorMappings.Values)
        {
            if (mapping.JsonPath == null!)
                mapping.JsonPath = jsonOptions.PropertyNamingPolicy?.ConvertName(mapping.PropertyName) ?? mapping.PropertyName;
        }
    }

    internal void ResolveSpatialJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.spatialMappings.Values)
        {
            if (mapping.JsonPath == null!)
                mapping.JsonPath = jsonOptions.PropertyNamingPolicy?.ConvertName(mapping.PropertyName) ?? mapping.PropertyName;
        }
    }

    internal void ResolveFullTextJsonPaths(JsonSerializerOptions jsonOptions)
        => this.Mappings.ResolveFullTextJsonPaths(jsonOptions);

    internal void ResolveComputedJsonNames(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveComputedJsonNames(jsonOptions);

    // ── Helpers ─────────────────────────────────────────────────────────

    static bool IsNumericType(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;
        return t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte)
            || t == typeof(double) || t == typeof(float) || t == typeof(decimal);
    }

    static (string[] Segments, Type MemberType) ExtractPathAndType<T>(Expression<Func<T, object>> property)
    {
        var body = property.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } u)
            body = u.Operand;

        var memberType = body.Type;
        var segments = new List<string>();
        while (body is MemberExpression m)
        {
            segments.Add(m.Member.Name);
            body = m.Expression!;
        }
        if (segments.Count == 0)
            throw new ArgumentException("MapIndexedProperty requires a property access expression (e.g. x => x.Status).", nameof(property));
        segments.Reverse();
        return (segments.ToArray(), memberType);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property resolved by name from a user-provided expression.")]
    static (string name, System.Reflection.PropertyInfo info) ResolveMember<T>(Expression body)
    {
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;
        if (body is not MemberExpression member)
            throw new ArgumentException("Expression must be a simple property access (e.g., x => x.Location).", nameof(body));
        var name = member.Member.Name;
        var info = typeof(T).GetProperty(name)
            ?? throw new ArgumentException($"Property '{name}' not found on '{typeof(T).Name}'.");
        return (name, info);
    }

    static string ExtractPropertyName<T>(Expression<Func<T, object>> expression)
    {
        var body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;
        if (body is MemberExpression member)
            return member.Member.Name;
        throw new ArgumentException("Expression must be a simple property access (e.g., x => x.MyId).", nameof(expression));
    }

    // ── IDocumentStoreOptions (explicit — the provider-agnostic slice; the typed overloads above stay fluent) ──
    IDocumentStoreOptions IDocumentStoreOptions.AddInterceptor(IDocumentInterceptor interceptor)
        => this.AddInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddBulkInterceptor(IDocumentBulkInterceptor interceptor)
        => this.AddBulkInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddQueryFilter<T>(string? name, Expression<Func<T, bool>> predicate)
        => name == null ? this.AddQueryFilter(predicate) : this.AddQueryFilter(name, predicate);
}

/// <summary>A declared indexed property: CLR path segments plus whether the CLR type is numeric.</summary>
internal sealed record RedisIndexedSpec(string[] ClrSegments, bool IsNumeric);

/// <summary>A mapped spatial property with a runtime accessor and resolved JSON path.</summary>
internal sealed class RedisSpatialMapping
{
    public required Type DocumentType { get; init; }
    public required string PropertyName { get; init; }
    public required string JsonPath { get; set; }
    public required Func<object, GeoPoint?> GetGeoPoint { get; init; }
}
