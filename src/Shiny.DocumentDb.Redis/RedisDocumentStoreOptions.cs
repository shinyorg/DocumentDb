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

    // The provider-agnostic view of the serializer options, so a cross-cutting feature (field-level encryption)
    // can attach a JsonTypeInfo modifier without knowing which options class it holds.
    JsonSerializerOptions? IDocumentStoreOptions.SerializerOptions
    {
        get => this.JsonSerializerOptions;
        set => this.JsonSerializerOptions = value;
    }

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws if the type
    /// cannot be resolved from the configured TypeInfoResolver. Set false in AOT deployments. Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>Optional diagnostic callback.</summary>
    public Action<string>? Logging { get; set; }

    // ── Id mapping ──────────────────────────────────────────────────────

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

    RedisDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors => this.Mappings.Interceptors;

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public RedisDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public RedisDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    // ── Version / CAS ───────────────────────────────────────────────────

    internal VersionMapping? ResolveVersionMapping(Type type) => this.Mappings.ResolveVersionMapping(type);

    // ── Indexed properties (RediSearch TAG / NUMERIC pushdown) ──────────

    /// <summary>
    /// Declares a scalar property as a queryable RediSearch schema field so LINQ <c>Where</c> predicates and
    /// <c>OrderBy</c> over it push down to <c>FT.SEARCH</c>/<c>FT.AGGREGATE</c>. Numeric CLR types index as
    /// NUMERIC (ranges + sort); everything else indexes as TAG (exact match + <c>in</c>). Undeclared fields
    /// still work but are filtered client-side. Map before first use.
    /// </summary>
    internal RedisDocumentStoreOptions MapIndexedProperty<T>(Expression<Func<T, object>> property) where T : class
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

    internal VectorMapping? ResolveVectorMapping(Type type) => this.Mappings.ResolveVectorMapping(type);

    // ── Full-text ───────────────────────────────────────────────────────

    internal FullTextMapping? ResolveFullTextMapping(Type type) => this.Mappings.ResolveFullTextMapping(type);

    // ── Spatial ─────────────────────────────────────────────────────────

    internal SpatialMapping? ResolveSpatialMapping(Type type) => this.Mappings.ResolveSpatialMapping(type);

    // ── Computed ────────────────────────────────────────────────────────

    internal IReadOnlyList<ComputedMapping> ResolveComputedMappings(Type type) => this.Mappings.ResolveComputedMappings(type);

    // ── Blobs ──────────────────────────────────────────────────────────────

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

    internal void ResolveVectorJsonPaths(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveVectorJsonPaths(jsonOptions);

    internal void ResolveSpatialJsonPaths(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveSpatialJsonPaths(jsonOptions);

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

    static (string name, System.Reflection.PropertyInfo info) ResolveMember<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression body)
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
    /// <summary>What the Redis backend supports — read by the configuration validation pass.</summary>
    DocumentStoreCapabilities IDocumentStoreOptions.Capabilities => new()
    {
        ProviderName = "Redis",
        PerTypeStorageName = false,
        Spatial = true,
        Vector = true,
        FullText = true,
        Temporal = false,
        Blobs = true,
        ComputedProperties = true
    };

    DocumentMappingRegistry IDocumentStoreOptions.Mappings => this.Mappings;

    IDocumentStoreOptions IDocumentStoreOptions.AddInterceptor(IDocumentInterceptor interceptor)
        => this.AddInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddBulkInterceptor(IDocumentBulkInterceptor interceptor)
        => this.AddBulkInterceptor(interceptor);
}

/// <summary>A declared indexed property: CLR path segments plus whether the CLR type is numeric.</summary>
internal sealed record RedisIndexedSpec(string[] ClrSegments, bool IsNumeric);
