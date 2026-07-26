using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.IndexedDb;

public class IndexedDbDocumentStoreOptions : IDocumentStoreOptions
{
    /// <summary>The shared per-type mapping state — see <see cref="DocumentMappingRegistry"/>.</summary>
    internal DocumentMappingRegistry Mappings { get; } = new();

    readonly Dictionary<string, string> typeMappings = new();
    readonly HashSet<string> mappedStoreNames = new(StringComparer.OrdinalIgnoreCase);

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

    /// <summary>
    /// Maps a document type to its own dedicated object store.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapTypeToStore<T>() where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToStore<T>(typeName);
    }

    /// <summary>
    /// Maps a document type to its own dedicated object store with a custom Id property.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapTypeToStore<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToStore<T>(typeName, idProperty);
    }

    /// <summary>
    /// Maps a document type to a dedicated object store with the specified name.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapTypeToStore<T>(string storeName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);

        if (!this.mappedStoreNames.Add(storeName))
            throw new ArgumentException($"Store '{storeName}' is already mapped to another type.", nameof(storeName));

        this.typeMappings[typeName] = storeName;
        return this;
    }

    /// <summary>
    /// Maps a document type to a dedicated object store with the specified name and a custom Id property.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapTypeToStore<T>(string storeName, Expression<Func<T, object>> idProperty) where T : class
    {
        this.MapTypeToStore<T>(storeName);
        this.Mappings.MapIdProperty(idProperty);
        return this;
    }

    internal string ResolveStoreName(string typeName)
        => this.typeMappings.TryGetValue(typeName, out var store) ? store : this.StoreName;

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

    /// <summary>
    /// Registers a global query filter for <typeparamref name="T"/>. See
    /// <see cref="DocumentStoreOptions.AddQueryFilter{T}(Expression{Func{T, bool}})"/> for semantics.
    /// </summary>
    public IndexedDbDocumentStoreOptions AddQueryFilter<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(null, predicate);
    }

    /// <summary>Registers a named global query filter for <typeparamref name="T"/>.</summary>
    public IndexedDbDocumentStoreOptions AddQueryFilter<T>(string name, Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(name, predicate);
    }

    IndexedDbDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors { get; } = new();

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public IndexedDbDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public IndexedDbDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    /// <summary>Registers a before-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public IndexedDbDocumentStoreOptions OnBeforeWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddBefore<T>(handler); return this; }

    /// <summary>Registers an after-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public IndexedDbDocumentStoreOptions OnAfterWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddAfter<T>(handler); return this; }

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapVersionProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression<Func<T, int>> property) where T : class
    {
        this.Mappings.MapVersionProperty(property);
        return this;
    }

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency. AOT-safe overload.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
    {
        this.Mappings.MapVersionProperty(propertyName, getter, setter);
        return this;
    }

    internal VersionMapping? ResolveVersionMapping(Type type) => this.Mappings.ResolveVersionMapping(type);

    /// <summary>
    /// Enables append-only system-time temporal history for <typeparamref name="T"/>. Every
    /// Insert/Update/Upsert/Remove writes a versioned snapshot to a <c>{store}_history</c> sidecar object
    /// store, so the document's state can be read back as of any point in time via the
    /// <see cref="ITemporalDocumentStore"/> methods (History/AsOf/Restore/GetDiffBetween/…). Opt-in and
    /// per type — only mapped types incur the extra history write. Bulk <c>Clear</c> records no history.
    /// <para>
    /// Because temporal adds new object stores, an existing database must be opened at a higher
    /// <see cref="Version"/> so the schema upgrade creates them — increment <see cref="Version"/> when
    /// adding <c>MapTemporal</c> to an already-deployed store.
    /// </para>
    /// </summary>
    public IndexedDbDocumentStoreOptions MapTemporal<T>(Action<TemporalOptions>? configure = null) where T : class
    {
        this.Mappings.MapTemporal<T>(configure);
        return this;
    }

    internal TemporalMapping? ResolveTemporalMapping(Type type) => this.Mappings.ResolveTemporalMapping(type);

    /// <summary>
    /// Declares a string property as full-text searchable. IndexedDB has no native full-text engine, so
    /// searches use an in-memory TF-IDF scan. See
    /// <see cref="DocumentStoreOptions.MapFullTextProperty{T}(Expression{Func{T, string}}, FullTextLanguage)"/>.
    /// </summary>
    public IndexedDbDocumentStoreOptions MapFullTextProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        Expression<Func<T, string?>> property,
        FullTextLanguage language = FullTextLanguage.English) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        this.Mappings.MapFullTextProperty([property], language);
        return this;
    }

    /// <summary>Declares several string properties combined into one full-text index (in-memory TF-IDF).</summary>
    public IndexedDbDocumentStoreOptions MapFullTextProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        IReadOnlyList<Expression<Func<T, string?>>> properties,
        FullTextLanguage language = FullTextLanguage.English) where T : class
    {
        ArgumentNullException.ThrowIfNull(properties);
        this.Mappings.MapFullTextProperty(properties, language);
        return this;
    }

    /// <summary>AOT-safe overload mapping full-text to a direct text selector (combine fields or index a string collection).</summary>
    public IndexedDbDocumentStoreOptions MapFullTextProperty<T>(
        IReadOnlyList<string> propertyNames,
        Func<T, IEnumerable<string?>> textSelector,
        FullTextLanguage language = FullTextLanguage.English) where T : class
    {
        this.Mappings.MapFullTextProperty(propertyNames, textSelector, language);
        return this;
    }

    internal FullTextMapping? ResolveFullTextMapping(Type type) => this.Mappings.ResolveFullTextMapping(type);


    /// <summary>Maps a computed property — a derived value not stored in the document JSON that can be
    /// filtered, sorted, projected, and read back as a normal property. Evaluated in-memory on IndexedDB.</summary>
    public IndexedDbDocumentStoreOptions MapComputedProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T, TValue>(Expression<Func<T, TValue>> property, Expression<Func<T, TValue>> definition, bool indexed = false) where T : class
    {
        this.Mappings.Computed.Add(ComputedMappingFactory.FromExpression(property, definition, indexed));
        return this;
    }

    /// <summary>AOT-clean overload taking the property name and an explicit setter delegate.</summary>
    public IndexedDbDocumentStoreOptions MapComputedProperty<T, TValue>(string propertyName, Expression<Func<T, TValue>> definition, Action<T, TValue> setter, bool indexed = false) where T : class
    {
        this.Mappings.Computed.Add(ComputedMappingFactory.FromExpression(propertyName, definition, setter, indexed));
        return this;
    }

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
        foreach (var store in this.typeMappings.Values)
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
    IDocumentStoreOptions IDocumentStoreOptions.AddInterceptor(IDocumentInterceptor interceptor)
        => this.AddInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddBulkInterceptor(IDocumentBulkInterceptor interceptor)
        => this.AddBulkInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddQueryFilter<T>(string? name, Expression<Func<T, bool>> predicate)
        => name == null ? this.AddQueryFilter(predicate) : this.AddQueryFilter(name, predicate);
}
