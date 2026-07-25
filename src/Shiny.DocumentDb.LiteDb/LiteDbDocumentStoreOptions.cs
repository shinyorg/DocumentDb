using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.LiteDb;

public class LiteDbDocumentStoreOptions : IDocumentStoreOptions
{
    /// <summary>The shared per-type mapping state — see <see cref="DocumentMappingRegistry"/>.</summary>
    internal DocumentMappingRegistry Mappings { get; } = new();

    readonly Dictionary<string, string> typeMappings = new();
    readonly HashSet<string> mappedCollectionNames = new(StringComparer.OrdinalIgnoreCase);

    public required string ConnectionString { get; set; }
    public TypeNameResolution TypeNameResolution { get; set; } = TypeNameResolution.ShortName;
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// The name of the default shared document collection.
    /// Types not explicitly mapped via <see cref="MapTypeToCollection{T}"/> are stored here.
    /// Defaults to "documents".
    /// </summary>
    public string CollectionName { get; set; } = "documents";

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

    /// <summary>
    /// Maps a document type to its own dedicated collection.
    /// The collection name is auto-derived from the type name using the configured <see cref="TypeNameResolution"/>.
    /// </summary>
    public LiteDbDocumentStoreOptions MapTypeToCollection<T>() where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToCollection<T>(typeName);
    }

    /// <summary>
    /// Maps a document type to its own dedicated collection with a custom Id property.
    /// </summary>
    public LiteDbDocumentStoreOptions MapTypeToCollection<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToCollection<T>(typeName, idProperty);
    }

    /// <summary>
    /// Maps a document type to a dedicated collection with the specified name.
    /// </summary>
    public LiteDbDocumentStoreOptions MapTypeToCollection<T>(string collectionName) where T : class
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
    public LiteDbDocumentStoreOptions MapTypeToCollection<T>(string collectionName, Expression<Func<T, object>> idProperty) where T : class
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
    public LiteDbDocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.Mappings.IdConverters.Register(converter);
        return this;
    }

    /// <summary>Registers a custom Id type using inline delegates.</summary>
    public LiteDbDocumentStoreOptions MapIdType<TId>(
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
    public LiteDbDocumentStoreOptions AddQueryFilter<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(null, predicate);
    }

    /// <summary>Registers a named global query filter for <typeparamref name="T"/>.</summary>
    public LiteDbDocumentStoreOptions AddQueryFilter<T>(string name, Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(name, predicate);
    }

    LiteDbDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors { get; } = new();

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public LiteDbDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public LiteDbDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    /// <summary>Registers a before-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public LiteDbDocumentStoreOptions OnBeforeWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddBefore<T>(handler); return this; }

    /// <summary>Registers an after-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public LiteDbDocumentStoreOptions OnAfterWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddAfter<T>(handler); return this; }

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency.
    /// On insert the version is set to 1. On update the version is checked and incremented.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public LiteDbDocumentStoreOptions MapVersionProperty<T>(Expression<Func<T, int>> property) where T : class
    {
        this.Mappings.MapVersionProperty(property);
        return this;
    }

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency. AOT-safe overload.
    /// </summary>
    public LiteDbDocumentStoreOptions MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
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
    public LiteDbDocumentStoreOptions MapTemporal<T>(Action<TemporalOptions>? configure = null) where T : class
    {
        this.Mappings.MapTemporal<T>(configure);
        return this;
    }

    internal TemporalMapping? ResolveTemporalMapping(Type type) => this.Mappings.ResolveTemporalMapping(type);

    /// <summary>
    /// Declares a string property as full-text searchable. LiteDB has no native full-text engine, so
    /// searches use an in-memory TF-IDF scan over the collection. See
    /// <see cref="DocumentStoreOptions.MapFullTextProperty{T}(Expression{Func{T, string}}, FullTextLanguage)"/>.
    /// </summary>
    public LiteDbDocumentStoreOptions MapFullTextProperty<T>(
        Expression<Func<T, string?>> property,
        FullTextLanguage language = FullTextLanguage.English) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        this.Mappings.MapFullTextProperty<T>([property], language);
        return this;
    }

    /// <summary>Declares several string properties combined into one full-text index (in-memory TF-IDF).</summary>
    public LiteDbDocumentStoreOptions MapFullTextProperty<T>(
        IReadOnlyList<Expression<Func<T, string?>>> properties,
        FullTextLanguage language = FullTextLanguage.English) where T : class
    {
        ArgumentNullException.ThrowIfNull(properties);
        this.Mappings.MapFullTextProperty(properties, language);
        return this;
    }

    /// <summary>AOT-safe overload mapping full-text to a direct text selector (combine fields or index a string collection).</summary>
    public LiteDbDocumentStoreOptions MapFullTextProperty<T>(
        IReadOnlyList<string> propertyNames,
        Func<T, IEnumerable<string?>> textSelector,
        FullTextLanguage language = FullTextLanguage.English) where T : class
    {
        this.Mappings.MapFullTextProperty(propertyNames, textSelector, language);
        return this;
    }

    internal FullTextMapping? ResolveFullTextMapping(Type type) => this.Mappings.ResolveFullTextMapping(type);


    /// <summary>
    /// Maps a computed property — a value derived from other fields that is not stored in the document JSON
    /// but can be filtered, sorted, projected, and read back as a normal property.
    /// See <see cref="DocumentStoreOptions.MapComputedProperty{T, TValue}(Expression{Func{T, TValue}}, Expression{Func{T, TValue}}, bool)"/>.
    /// On LiteDB the value is evaluated in-memory; the <paramref name="indexed"/> flag is accepted for API
    /// parity but has no native column to back.
    /// </summary>
    public LiteDbDocumentStoreOptions MapComputedProperty<T, TValue>(
        Expression<Func<T, TValue>> property,
        Expression<Func<T, TValue>> definition,
        bool indexed = false) where T : class
    {
        this.Mappings.Computed.Add(ComputedMappingFactory.FromExpression(property, definition, indexed));
        return this;
    }

    /// <summary>AOT-clean overload taking the property name and an explicit setter delegate.</summary>
    public LiteDbDocumentStoreOptions MapComputedProperty<T, TValue>(
        string propertyName,
        Expression<Func<T, TValue>> definition,
        Action<T, TValue> setter,
        bool indexed = false) where T : class
    {
        this.Mappings.Computed.Add(ComputedMappingFactory.FromExpression(propertyName, definition, setter, indexed));
        return this;
    }

    internal IReadOnlyList<ComputedMapping> ResolveComputedMappings(Type type) => this.Mappings.ResolveComputedMappings(type);
    internal IReadOnlyDictionary<string, ComputedMapping>? ResolveComputedLookup(Type type) => this.Mappings.ResolveComputedLookup(type);
    internal void ResolveComputedJsonNames(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveComputedJsonNames(jsonOptions);

    // ── Blobs ──────────────────────────────────────────────────────────────

    /// <summary>See <see cref="DocumentStoreOptions.MapBlob{T}(Expression{Func{T, DocumentBlob}}, Action{BlobOptions})"/>.</summary>
    public LiteDbDocumentStoreOptions MapBlob<T>(Expression<Func<T, DocumentBlob?>> property, Action<BlobOptions>? configure = null) where T : class
    {
        var o = new BlobOptions();
        configure?.Invoke(o);
        this.AddBlob(BlobMappingFactory.FromExpression(property, o));
        return this;
    }

    /// <summary>See <see cref="DocumentStoreOptions.MapBlobCollection{T}(Expression{Func{T, DocumentBlobCollection}}, Action{BlobOptions})"/>.</summary>
    public LiteDbDocumentStoreOptions MapBlobCollection<T>(Expression<Func<T, DocumentBlobCollection?>> property, Action<BlobOptions>? configure = null) where T : class
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
