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

    readonly Dictionary<Type, string> collectionOverrides = new();

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

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws if the type
    /// cannot be resolved from the configured TypeInfoResolver. Set false in AOT deployments. Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>Optional diagnostic callback.</summary>
    public Action<string>? Logging { get; set; }

    /// <summary>Overrides the Firestore collection (default the resolved type name) for a document type.</summary>
    public FirestoreDocumentStoreOptions MapTypeToCollection<T>(string collectionName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);
        this.collectionOverrides[typeof(T)] = collectionName;
        return this;
    }

    internal string ResolveCollectionName(Type type, string typeName)
    {
        var name = this.collectionOverrides.TryGetValue(type, out var mapped) ? mapped : typeName;
        return string.IsNullOrWhiteSpace(this.CollectionPrefix) ? name : $"{this.CollectionPrefix}_{name}";
    }

    /// <summary>Maps a document type to a custom Id property.</summary>
    public FirestoreDocumentStoreOptions MapIdProperty<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        this.Mappings.MapIdProperty(idProperty);
        return this;
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

    /// <summary>Registers an unnamed global query filter for <typeparamref name="T"/>.</summary>
    public FirestoreDocumentStoreOptions AddQueryFilter<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(null, predicate);
    }

    /// <summary>Registers a named global query filter for <typeparamref name="T"/>.</summary>
    public FirestoreDocumentStoreOptions AddQueryFilter<T>(string name, Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(name, predicate);
    }

    FirestoreDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors { get; } = new();

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public FirestoreDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public FirestoreDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    /// <summary>Registers a before-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public FirestoreDocumentStoreOptions OnBeforeWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddBefore<T>(handler); return this; }

    /// <summary>Registers an after-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public FirestoreDocumentStoreOptions OnAfterWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddAfter<T>(handler); return this; }

    /// <summary>
    /// Maps an <c>int</c> version property for optimistic concurrency. Insert seeds version 1; Update/Upsert
    /// check the stored version inside a Firestore transaction, increment it, and write — a stale version
    /// throws <see cref="ConcurrencyException"/>.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public FirestoreDocumentStoreOptions MapVersionProperty<T>(Expression<Func<T, int>> property) where T : class
    {
        this.Mappings.MapVersionProperty(property);
        return this;
    }

    /// <summary>AOT-safe version-property overload.</summary>
    public FirestoreDocumentStoreOptions MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
    {
        this.Mappings.MapVersionProperty(propertyName, getter, setter);
        return this;
    }

    // ── Blobs ──────────────────────────────────────────────────────────────

    /// <summary>See <see cref="DocumentStoreOptions.MapBlob{T}(Expression{Func{T, DocumentBlob}}, Action{BlobOptions})"/>.</summary>
    public FirestoreDocumentStoreOptions MapBlob<T>(Expression<Func<T, DocumentBlob?>> property, Action<BlobOptions>? configure = null) where T : class
    {
        var o = new BlobOptions();
        configure?.Invoke(o);
        this.AddBlob(BlobMappingFactory.FromExpression(property, o));
        return this;
    }

    /// <summary>See <see cref="DocumentStoreOptions.MapBlobCollection{T}(Expression{Func{T, DocumentBlobCollection}}, Action{BlobOptions})"/>.</summary>
    public FirestoreDocumentStoreOptions MapBlobCollection<T>(Expression<Func<T, DocumentBlobCollection?>> property, Action<BlobOptions>? configure = null) where T : class
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
    IDocumentStoreOptions IDocumentStoreOptions.AddInterceptor(IDocumentInterceptor interceptor)
        => this.AddInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddBulkInterceptor(IDocumentBulkInterceptor interceptor)
        => this.AddBulkInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddQueryFilter<T>(string? name, Expression<Func<T, bool>> predicate)
        => name == null ? this.AddQueryFilter(predicate) : this.AddQueryFilter(name, predicate);
}
