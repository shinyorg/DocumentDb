using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.CosmosDb;

public class CosmosDbDocumentStoreOptions : IDocumentStoreOptions
{
    /// <summary>The shared per-type mapping state — see <see cref="DocumentMappingRegistry"/>.</summary>
    internal DocumentMappingRegistry Mappings { get; } = new();

    public required string ConnectionString { get; set; }
    public required string DatabaseName { get; set; }
    public string ContainerName { get; set; } = "documents";
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

    internal string ResolveContainerName(string typeName)
        => this.Mappings.ResolveMappedName(typeName, this.ContainerName);

    /// <summary>
    /// Registers a converter so a document Id can be a CLR type beyond Guid/int/long/string
    /// (e.g. a <c>Ulid</c> or a strongly-typed wrapper). The Id is still stored as a string.
    /// </summary>
    public CosmosDbDocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.Mappings.IdConverters.Register(converter);
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
        this.Mappings.IdConverters.Register(new DelegateIdConverter<TId>(toString, parse, isDefault, generate));
        return this;
    }

    internal string? ResolveIdPropertyName(Type type) => this.Mappings.ResolveIdPropertyName(type);

    internal IdConverterRegistry IdConverters => this.Mappings.IdConverters;

    CosmosDbDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors => this.Mappings.Interceptors;

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public CosmosDbDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public CosmosDbDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    internal VersionMapping? ResolveVersionMapping(Type type) => this.Mappings.ResolveVersionMapping(type);

    internal TemporalMapping? ResolveTemporalMapping(Type type) => this.Mappings.ResolveTemporalMapping(type);

    internal void ResolveVersionJsonPaths(JsonSerializerOptions jsonOptions)
    {
        this.Mappings.ResolveVersionJsonPaths(jsonOptions);
    }

    internal SpatialMapping? ResolveSpatialMapping(Type type) => this.Mappings.ResolveSpatialMapping(type);

    internal void ResolveSpatialJsonPaths(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveSpatialJsonPaths(jsonOptions);

    internal VectorMapping? ResolveVectorMapping(Type type) => this.Mappings.ResolveVectorMapping(type);

    internal void ResolveVectorJsonPaths(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveVectorJsonPaths(jsonOptions);

    internal FullTextMapping? ResolveFullTextMapping(Type type) =>
        this.Mappings.ResolveFullTextMapping(type);

    internal void ResolveFullTextJsonPaths(JsonSerializerOptions jsonOptions)
        => this.Mappings.ResolveFullTextJsonPaths(jsonOptions);

    internal IReadOnlyList<ComputedMapping> ResolveComputedMappings(Type type) => this.Mappings.ResolveComputedMappings(type);
    internal IReadOnlyDictionary<string, ComputedMapping>? ResolveComputedLookup(Type type) => this.Mappings.ResolveComputedLookup(type);
    internal void ResolveComputedJsonNames(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveComputedJsonNames(jsonOptions);

    // ── Blobs ──────────────────────────────────────────────────────────────

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
    /// <summary>What the Cosmos DB backend supports — read by the configuration validation pass.</summary>
    DocumentStoreCapabilities IDocumentStoreOptions.Capabilities => new()
    {
        ProviderName = "Cosmos DB",
        PerTypeStorageName = true,
        Spatial = true,
        Vector = true,
        FullText = true,
        Temporal = true,
        Blobs = true,
        ComputedProperties = true
    };

    DocumentMappingRegistry IDocumentStoreOptions.Mappings => this.Mappings;

    IDocumentStoreOptions IDocumentStoreOptions.AddInterceptor(IDocumentInterceptor interceptor)
        => this.AddInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddBulkInterceptor(IDocumentBulkInterceptor interceptor)
        => this.AddBulkInterceptor(interceptor);
    // A null indexKind means "the provider's default", which on Cosmos is DiskANN — not the interface's HNSW,
    // which Cosmos does not support at all. That is exactly why the shared member takes a nullable kind.
}
