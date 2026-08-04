using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Google.Api.Gax;
using Google.Cloud.Firestore;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.Firestore;

/// <summary>
/// Options for the Google Firestore document store. Each document type maps to its own Firestore collection
/// (the resolved type name, overridable via <see cref="MapTypeToCollection{T}"/>); the document id is the
/// document's string id. The body is stored as a Firestore native map so fields auto-index and queries push down.
/// </summary>
public class FirestoreDocumentStoreOptions : IDocumentStoreOptions
{
    /// <summary>The shared per-type mapping state — see <see cref="DocumentMappingRegistry"/>.</summary>
    internal DocumentMappingRegistry Mappings { get; } = new();

    /// <summary>Supply a pre-built <see cref="FirestoreDb"/> (e.g. pointed at the emulator, or shared across tests). Wins over <see cref="ProjectId"/>.</summary>
    public FirestoreDb? FirestoreDb { get; set; }

    /// <summary>The Google Cloud project id. Required when <see cref="FirestoreDb"/> is not supplied.</summary>
    public string? ProjectId { get; set; }

    /// <summary>The Firestore database id. Defaults to <c>(default)</c>.</summary>
    public string DatabaseId { get; set; } = "(default)";

    /// <summary>
    /// Optional prefix prepended (as <c>{prefix}_{collection}</c>) to every resolved collection name. Lets
    /// several logically-separate stores share one Firestore database without colliding — handy for tests, and
    /// for multi-store apps on a single project. Applies after <see cref="MapTypeToCollection{T}"/>.
    /// </summary>
    public string? CollectionPrefix { get; set; }

    /// <summary>
    /// Controls whether the <c>FIRESTORE_EMULATOR_HOST</c> environment variable is honored when the store builds
    /// its own <see cref="FirestoreDb"/> from <see cref="ProjectId"/>. Defaults to
    /// <see cref="EmulatorDetection.EmulatorOrProduction"/> so a set emulator host is picked up automatically.
    /// </summary>
    public EmulatorDetection EmulatorDetection { get; set; } = EmulatorDetection.EmulatorOrProduction;

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

    internal string ResolveCollectionName(Type type, string typeName)
    {
        var name = this.Mappings.ResolveMappedName(typeName, typeName);
        return string.IsNullOrWhiteSpace(this.CollectionPrefix) ? name : $"{this.CollectionPrefix}_{name}";
    }

    internal string? ResolveIdPropertyName(Type type) => this.Mappings.ResolveIdPropertyName(type);

    /// <summary>Registers a converter so a document Id can be a CLR type beyond Guid/int/long/string.</summary>
    public FirestoreDocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.Mappings.IdConverters.Register(converter);
        return this;
    }

    /// <summary>Registers a custom Id type using inline delegates.</summary>
    public FirestoreDocumentStoreOptions MapIdType<TId>(
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

    FirestoreDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors => this.Mappings.Interceptors;

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public FirestoreDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public FirestoreDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    // ── Blobs ──────────────────────────────────────────────────────────────

    void AddBlob(BlobMapping mapping)
    {
        this.Mappings.AddBlobMapping(mapping);
    }

    internal IReadOnlyList<BlobMapping> ResolveBlobMappings(Type type) => this.Mappings.ResolveBlobMappings(type);

    internal VersionMapping? ResolveVersionMapping(Type type) => this.Mappings.ResolveVersionMapping(type);

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
    /// <summary>What the Firestore backend supports — read by the configuration validation pass.</summary>
    DocumentStoreCapabilities IDocumentStoreOptions.Capabilities => new()
    {
        ProviderName = "Firestore",
        PerTypeStorageName = true,
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
