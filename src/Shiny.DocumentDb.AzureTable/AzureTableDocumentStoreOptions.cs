using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Azure;
using Azure.Core;
using Azure.Data.Tables;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.AzureTable;

/// <summary>
/// Options for the Azure Table Storage document store. Targets Azure Table Storage and the
/// Cosmos DB Table API (both speak the same <c>Azure.Data.Tables</c> protocol). A single table holds
/// every type: <c>PartitionKey = typeName</c> and <c>RowKey = id</c>.
/// </summary>
public class AzureTableDocumentStoreOptions
{
    readonly Dictionary<Type, string> partitionOverrides = new();
    readonly Dictionary<Type, string> idPropertyOverrides = new();
    readonly IdConverterRegistry idConverters = new();
    readonly Dictionary<Type, List<QueryFilter>> queryFilters = new();
    readonly List<(Type Type, string[] Segments)> indexedSpecs = new();
    internal readonly Dictionary<Type, VersionMapping> versionMappings = new();

    /// <summary>
    /// A storage account / Cosmos Table API connection string, e.g.
    /// <c>UseDevelopmentStorage=true</c> or a real account connection string. When set it takes
    /// precedence over <see cref="ServiceUri"/> + credential. Ignored when
    /// <see cref="TableServiceClient"/> is supplied directly.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>The table service endpoint (used with <see cref="TokenCredential"/>,
    /// <see cref="SharedKeyCredential"/>, or <see cref="SasCredential"/> when no connection string is set).</summary>
    public Uri? ServiceUri { get; set; }

    /// <summary>A <c>DefaultAzureCredential</c> / managed-identity token credential (used with <see cref="ServiceUri"/>).</summary>
    public TokenCredential? TokenCredential { get; set; }

    /// <summary>An account name + key credential (used with <see cref="ServiceUri"/>).</summary>
    public TableSharedKeyCredential? SharedKeyCredential { get; set; }

    /// <summary>A SAS credential (used with <see cref="ServiceUri"/>).</summary>
    public AzureSasCredential? SasCredential { get; set; }

    /// <summary>Supply a pre-built <see cref="Azure.Data.Tables.TableServiceClient"/> (e.g. shared across tests). Wins over all other credential options.</summary>
    public TableServiceClient? TableServiceClient { get; set; }

    /// <summary>The single table that holds every document type. Defaults to <c>Documents</c>.</summary>
    public string TableName { get; set; } = "Documents";

    /// <summary>When true (default) the table is created if it does not already exist.</summary>
    public bool AutoCreateTable { get; set; } = true;

    public TypeNameResolution TypeNameResolution { get; set; } = TypeNameResolution.ShortName;
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws if the type
    /// cannot be resolved from the configured TypeInfoResolver. Set false in AOT deployments. Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>Optional diagnostic callback.</summary>
    public Action<string>? Logging { get; set; }

    /// <summary>Overrides the partition key (default the resolved type name) for a document type.</summary>
    public AzureTableDocumentStoreOptions MapTypeToPartition<T>(string partitionKey) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        this.partitionOverrides[typeof(T)] = partitionKey;
        return this;
    }

    internal string ResolvePartitionKey(Type type, string typeName)
        => this.partitionOverrides.TryGetValue(type, out var pk) ? pk : typeName;

    /// <summary>Maps a document type to a custom Id property.</summary>
    public AzureTableDocumentStoreOptions MapIdProperty<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        this.idPropertyOverrides[typeof(T)] = ExtractPropertyName(idProperty);
        return this;
    }

    internal string? ResolveIdPropertyName(Type type)
        => this.idPropertyOverrides.TryGetValue(type, out var name) ? name : null;

    /// <summary>Registers a converter so a document Id can be a CLR type beyond Guid/int/long/string.</summary>
    public AzureTableDocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.idConverters.Register(converter);
        return this;
    }

    /// <summary>Registers a custom Id type using inline delegates.</summary>
    public AzureTableDocumentStoreOptions MapIdType<TId>(
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

    internal IdConverterRegistry IdConverters => this.idConverters;

    /// <summary>Registers an unnamed global query filter for <typeparamref name="T"/>.</summary>
    public AzureTableDocumentStoreOptions AddQueryFilter<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(null, predicate);
    }

    /// <summary>Registers a named global query filter for <typeparamref name="T"/>.</summary>
    public AzureTableDocumentStoreOptions AddQueryFilter<T>(string name, Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(name, predicate);
    }

    AzureTableDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
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
    public AzureTableDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public AzureTableDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    /// <summary>Registers a before-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public AzureTableDocumentStoreOptions OnBeforeWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddBefore<T>(handler); return this; }

    /// <summary>Registers an after-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public AzureTableDocumentStoreOptions OnAfterWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddAfter<T>(handler); return this; }

    /// <summary>
    /// Maps an <c>int</c> version property for optimistic concurrency. Insert seeds version 1; Update/Upsert
    /// check the stored version, increment it, and use the Table <c>ETag</c> (If-Match) as the physical CAS token.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public AzureTableDocumentStoreOptions MapVersionProperty<T>(Expression<Func<T, int>> property) where T : class
    {
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
        return this;
    }

    /// <summary>AOT-safe version-property overload.</summary>
    public AzureTableDocumentStoreOptions MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
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
    /// Promotes a scalar property to a native top-level Table column (alongside the JSON <c>Data</c>) so
    /// server-side OData <c>$filter</c> pushdown and the string <see cref="IDocumentStore.Query{T}(string,System.Text.Json.Serialization.Metadata.JsonTypeInfo{T},object,CancellationToken)"/>
    /// overload can target it. LINQ predicates over a promoted property are pushed down to shrink the
    /// candidate set (the full predicate still runs client-side). Map before first use.
    /// </summary>
    // ── Blobs ──────────────────────────────────────────────────────────────
    readonly Dictionary<Type, List<BlobMapping>> blobMappings = new();

    /// <summary>See <see cref="DocumentStoreOptions.MapBlob{T}(Expression{Func{T, DocumentBlob}}, Action{BlobOptions})"/>.</summary>
    public AzureTableDocumentStoreOptions MapBlob<T>(Expression<Func<T, DocumentBlob?>> property, Action<BlobOptions>? configure = null) where T : class
    {
        var o = new BlobOptions();
        configure?.Invoke(o);
        this.AddBlob(BlobMappingFactory.FromExpression(property, o));
        return this;
    }

    /// <summary>See <see cref="DocumentStoreOptions.MapBlobCollection{T}(Expression{Func{T, DocumentBlobCollection}}, Action{BlobOptions})"/>.</summary>
    public AzureTableDocumentStoreOptions MapBlobCollection<T>(Expression<Func<T, DocumentBlobCollection?>> property, Action<BlobOptions>? configure = null) where T : class
    {
        var o = new BlobOptions();
        configure?.Invoke(o);
        this.AddBlob(BlobMappingFactory.FromCollectionExpression(property, o));
        return this;
    }

    void AddBlob(BlobMapping mapping)
    {
        if (!this.blobMappings.TryGetValue(mapping.DocumentType, out var list))
            this.blobMappings[mapping.DocumentType] = list = new List<BlobMapping>();
        list.RemoveAll(m => m.PropertyName.Equals(mapping.PropertyName, StringComparison.Ordinal));
        list.Add(mapping);
    }

    internal IReadOnlyList<BlobMapping> ResolveBlobMappings(Type type)
        => this.blobMappings.TryGetValue(type, out var list) ? list : Array.Empty<BlobMapping>();

    public AzureTableDocumentStoreOptions MapIndexedProperty<T>(Expression<Func<T, object>> property) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        this.indexedSpecs.Add((typeof(T), AzureTablePromoted.ExtractPath(property)));
        return this;
    }

    internal IReadOnlyList<(Type Type, string[] Segments)> IndexedSpecs => this.indexedSpecs;

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
}
