using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.IndexedDb;

public class IndexedDbDocumentStoreOptions : IDocumentStoreOptions
{
    /// <summary>The shared per-type mapping state — see <see cref="DocumentMappingRegistry"/>.</summary>
    internal DocumentMappingRegistry Mappings { get; } = new();

    /// <summary>
    /// The name of the IndexedDB database.
    /// </summary>
    public required string DatabaseName { get; set; }

    /// <summary>
    /// The IndexedDB database version. Increment when adding new object stores.
    /// </summary>
    public int Version { get; set; } = 1;

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
    /// The name of the default shared object store.
    /// Types not explicitly mapped via <see cref="MapTypeToStore{T}"/> are stored here.
    /// Defaults to "documents".
    /// </summary>
    public string StoreName { get; set; } = "documents";

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws an
    /// InvalidOperationException if the type cannot be resolved from the configured TypeInfoResolver.
    /// Set to false in AOT deployments to get clear errors instead of hard-to-diagnose trimming failures.
    /// Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>
    /// Optional callback invoked with diagnostic messages.
    /// </summary>
    public Action<string>? Logging { get; set; }

    internal string ResolveStoreName(string typeName)
        => this.Mappings.ResolveMappedName(typeName, this.StoreName);

    /// <summary>
    /// Registers a converter so a document Id can be a CLR type beyond Guid/int/long/string
    /// (e.g. a <c>Ulid</c> or a strongly-typed wrapper). The Id is still stored as a string.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.Mappings.IdConverters.Register(converter);
        return this;
    }

    /// <summary>Registers a custom Id type using inline delegates.</summary>
    public IndexedDbDocumentStoreOptions MapIdType<TId>(
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

    IndexedDbDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors => this.Mappings.Interceptors;

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public IndexedDbDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public IndexedDbDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    internal VersionMapping? ResolveVersionMapping(Type type) => this.Mappings.ResolveVersionMapping(type);

    internal TemporalMapping? ResolveTemporalMapping(Type type) => this.Mappings.ResolveTemporalMapping(type);

    internal FullTextMapping? ResolveFullTextMapping(Type type) => this.Mappings.ResolveFullTextMapping(type);

    internal IReadOnlyList<ComputedMapping> ResolveComputedMappings(Type type) => this.Mappings.ResolveComputedMappings(type);
    internal IReadOnlyDictionary<string, ComputedMapping>? ResolveComputedLookup(Type type) => this.Mappings.ResolveComputedLookup(type);
    internal void ResolveComputedJsonNames(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveComputedJsonNames(jsonOptions);

    /// <summary>The <c>{store}_history</c> object store backing temporal history for a given type.</summary>
    internal string ResolveHistoryStoreName(string typeName) => this.ResolveStoreName(typeName) + "_history";

    internal void ResolveVersionJsonPaths(JsonSerializerOptions jsonOptions)
    {
        this.Mappings.ResolveVersionJsonPaths(jsonOptions);
    }

    internal IEnumerable<string> GetAllStoreNames()
    {
        yield return this.StoreName;
        foreach (var store in this.Mappings.MappedNames)
            yield return store;
        // History sidecars must be declared up front so the IndexedDB schema upgrade creates them.
        foreach (var type in this.Mappings.TemporalTypes)
            yield return this.ResolveHistoryStoreName(TypeNameResolver.Resolve(type, this.TypeNameResolution));
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

    // ── IDocumentStoreOptions (explicit — the provider-agnostic slice; the typed overloads above stay fluent) ──
    /// <summary>What the IndexedDB backend supports — read by the configuration validation pass.</summary>
    DocumentStoreCapabilities IDocumentStoreOptions.Capabilities => new()
    {
        ProviderName = "IndexedDB",
        PerTypeStorageName = true,
        Spatial = false,
        Vector = false,
        FullText = true,
        Temporal = true,
        Blobs = false,
        ComputedProperties = true
    };

    DocumentMappingRegistry IDocumentStoreOptions.Mappings => this.Mappings;

    IDocumentStoreOptions IDocumentStoreOptions.AddInterceptor(IDocumentInterceptor interceptor)
        => this.AddInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddBulkInterceptor(IDocumentBulkInterceptor interceptor)
        => this.AddBulkInterceptor(interceptor);
}
