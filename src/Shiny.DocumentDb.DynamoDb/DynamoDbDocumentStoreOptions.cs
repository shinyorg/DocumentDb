using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.DynamoDb;

/// <summary>
/// Options for the Amazon DynamoDB document store. A single table holds every type:
/// partition key <c>pk = typeName</c> (HASH) and sort key <c>sk = id</c> (RANGE).
/// </summary>
public class DynamoDbDocumentStoreOptions : IDocumentStoreOptions
{
    /// <summary>The shared per-type mapping state — see <see cref="DocumentMappingRegistry"/>.</summary>
    internal DocumentMappingRegistry Mappings { get; } = new();

    readonly Dictionary<Type, string> partitionOverrides = new();
    readonly List<(Type Type, string[] Segments)> indexedSpecs = new();

    /// <summary>Supply a pre-built <see cref="IAmazonDynamoDB"/> client (e.g. shared across tests / DynamoDB Local). Wins over the credential/region options.</summary>
    public IAmazonDynamoDB? Client { get; set; }

    /// <summary>Explicit AWS credentials. When null the standard AWS credential chain is used.</summary>
    public AWSCredentials? Credentials { get; set; }

    /// <summary>The AWS region (e.g. <c>us-east-1</c>). Ignored when <see cref="ServiceUrl"/> is set.</summary>
    public RegionEndpoint? Region { get; set; }

    /// <summary>An explicit service URL — set this to point at DynamoDB Local (e.g. <c>http://localhost:8000</c>).</summary>
    public string? ServiceUrl { get; set; }

    /// <summary>The single table that holds every document type. Defaults to <c>Documents</c>.</summary>
    public string TableName { get; set; } = "Documents";

    /// <summary>When true the table is created (on-demand billing) if it does not exist. Defaults to false.</summary>
    public bool AutoCreateTable { get; set; }

    /// <summary>When true reads (<c>Get</c>/<c>Query</c>) are strongly consistent. Defaults to false (eventually consistent, matching DynamoDB).</summary>
    public bool ConsistentRead { get; set; }

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

    /// <summary>Overrides the partition key (default the resolved type name) for a document type.</summary>
    internal DynamoDbDocumentStoreOptions MapTypeToPartition<T>(string partitionKey) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        this.partitionOverrides[typeof(T)] = partitionKey;
        return this;
    }

    internal string ResolvePartitionKey(Type type, string typeName)
        => this.partitionOverrides.TryGetValue(type, out var pk) ? pk : typeName;

    internal string? ResolveIdPropertyName(Type type) => this.Mappings.ResolveIdPropertyName(type);

    /// <summary>Registers a converter so a document Id can be a CLR type beyond Guid/int/long/string.</summary>
    public DynamoDbDocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.Mappings.IdConverters.Register(converter);
        return this;
    }

    /// <summary>Registers a custom Id type using inline delegates.</summary>
    public DynamoDbDocumentStoreOptions MapIdType<TId>(
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

    DynamoDbDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors => this.Mappings.Interceptors;

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public DynamoDbDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public DynamoDbDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    internal VersionMapping? ResolveVersionMapping(Type type) => this.Mappings.ResolveVersionMapping(type);

    /// <summary>
    /// Promotes a scalar property to a native top-level DynamoDB attribute (alongside the JSON <c>Data</c>)
    /// so server-side <c>FilterExpression</c> pushdown and the string
    /// <see cref="IDocumentStore.Query{T}(string,System.Text.Json.Serialization.Metadata.JsonTypeInfo{T},object,CancellationToken)"/>
    /// (PartiQL) overload can target it. LINQ predicates over a promoted property are pushed down to shrink
    /// the candidate set (the full predicate still runs client-side). Map before first use.
    /// </summary>
    // ── Blobs ──────────────────────────────────────────────────────────────

    void AddBlob(BlobMapping mapping)
    {
        this.Mappings.AddBlobMapping(mapping);
    }

    internal IReadOnlyList<BlobMapping> ResolveBlobMappings(Type type) => this.Mappings.ResolveBlobMappings(type);

    internal DynamoDbDocumentStoreOptions MapIndexedProperty<T>(Expression<Func<T, object>> property) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        this.indexedSpecs.Add((typeof(T), DynamoDbPromoted.ExtractPath(property)));
        return this;
    }

    internal IReadOnlyList<(Type Type, string[] Segments)> IndexedSpecs => this.indexedSpecs;

    internal void ResolveVersionJsonPaths(JsonSerializerOptions jsonOptions)
    {
        this.Mappings.ResolveVersionJsonPaths(jsonOptions);
    }

    static string ExtractPropertyName<T>(Expression<Func<T, object>> expression)
    {
        var body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;

        if (body is MemberExpression member)
            return member.Member.Name;

        throw new ArgumentException(
            "Expression must be a simple property access (e.g., x => x.MyId).", nameof(expression));
    }

    // ── IDocumentStoreOptions (explicit — the provider-agnostic slice; the typed overloads above stay fluent) ──
    /// <summary>What the DynamoDB backend supports — read by the configuration validation pass.</summary>
    DocumentStoreCapabilities IDocumentStoreOptions.Capabilities => new()
    {
        ProviderName = "DynamoDB",
        PerTypeStorageName = false,
        Spatial = false,
        Vector = false,
        FullText = false,
        Temporal = false,
        Blobs = true,
        ComputedProperties = false
    };

    DocumentMappingRegistry IDocumentStoreOptions.Mappings => this.Mappings;

    IDocumentStoreOptions IDocumentStoreOptions.AddInterceptor(IDocumentInterceptor interceptor)
        => this.AddInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddBulkInterceptor(IDocumentBulkInterceptor interceptor)
        => this.AddBulkInterceptor(interceptor);
}
