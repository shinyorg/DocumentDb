using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb;

public enum TypeNameResolution
{
    ShortName,
    FullName
}

public class DocumentStoreOptions
{
    readonly Dictionary<string, string> typeMappings = new();
    readonly HashSet<string> mappedTableNames = new(StringComparer.OrdinalIgnoreCase);
    readonly Dictionary<Type, string> idPropertyOverrides = new();
    readonly IdConverterRegistry idConverters = new();
    readonly Dictionary<Type, List<QueryFilter>> queryFilters = new();
    internal readonly Dictionary<Type, VersionMapping> versionMappings = new();
    internal readonly Dictionary<Type, SpatialMapping> spatialMappings = new();
    internal readonly Dictionary<Type, VectorMapping> vectorMappings = new();
    internal readonly Dictionary<Type, TemporalMapping> temporalMappings = new();
    internal readonly List<Func<object, CancellationToken, Task>> beforeInsertHooks = new();

    public required IDatabaseProvider DatabaseProvider { get; set; }
    public TypeNameResolution TypeNameResolution { get; set; } = TypeNameResolution.ShortName;
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// The name of the default shared document table.
    /// Types not explicitly mapped via <see cref="MapTypeToTable{T}"/> are stored here.
    /// Defaults to "documents".
    /// </summary>
    public string TableName { get; set; } = "documents";

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws an
    /// InvalidOperationException if the type cannot be resolved from the configured TypeInfoResolver.
    /// Set to false in AOT deployments to get clear errors instead of hard-to-diagnose trimming failures.
    /// Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>
    /// Optional callback invoked with every SQL statement the store executes.
    /// Useful for debugging and diagnostics.
    /// </summary>
    public Action<string>? Logging { get; set; }

    /// <summary>
    /// When set, enables shared-table multi-tenancy. All queries are filtered by TenantId
    /// and all inserts include the TenantId value. A dedicated TenantId column and index
    /// are created in the table schema automatically.
    /// The function is called on every operation to resolve the current tenant.
    /// </summary>
    public Func<string>? TenantIdAccessor { get; set; }

    /// <summary>
    /// Maps a document type to its own dedicated table.
    /// The table name is auto-derived from the type name using the configured <see cref="TypeNameResolution"/>.
    /// </summary>
    public DocumentStoreOptions MapTypeToTable<T>() where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToTable<T>(typeName);
    }

    /// <summary>
    /// Maps a document type to its own dedicated table with a custom Id property.
    /// The table name is auto-derived from the type name using the configured <see cref="TypeNameResolution"/>.
    /// </summary>
    public DocumentStoreOptions MapTypeToTable<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);
        return this.MapTypeToTable<T>(typeName, idProperty);
    }

    /// <summary>
    /// Maps a document type to a dedicated table with the specified name.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown if another type is already mapped to the same table name.</exception>
    public DocumentStoreOptions MapTypeToTable<T>(string tableName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        var typeName = TypeNameResolver.Resolve(typeof(T), this.TypeNameResolution);

        if (!this.mappedTableNames.Add(tableName))
            throw new ArgumentException($"Table '{tableName}' is already mapped to another type.", nameof(tableName));

        this.typeMappings[typeName] = tableName;
        return this;
    }

    /// <summary>
    /// Maps a document type to a dedicated table with the specified name and a custom Id property.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown if another type is already mapped to the same table name.</exception>
    public DocumentStoreOptions MapTypeToTable<T>(string tableName, Expression<Func<T, object>> idProperty) where T : class
    {
        this.MapTypeToTable<T>(tableName);
        return this.MapIdProperty<T>(idProperty);
    }

    /// <summary>
    /// Overrides the Id property used for a document type. Use this when the document does not
    /// have a property literally named <c>Id</c> (for example, <c>UserId</c>, <c>DeviceKey</c>).
    /// The property type must be <see cref="Guid"/>, <see cref="int"/>, <see cref="long"/>, or
    /// <see cref="string"/>. Can be combined with — and is independent of — <see cref="MapTypeToTable{T}()"/>.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown if the expression is not a simple property access.</exception>
    public DocumentStoreOptions MapIdProperty<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        ArgumentNullException.ThrowIfNull(idProperty);
        this.idPropertyOverrides[typeof(T)] = ExtractPropertyName(idProperty);
        return this;
    }

    /// <summary>
    /// Overrides the Id property used for a document type by name. AOT-safe overload.
    /// </summary>
    public DocumentStoreOptions MapIdProperty<T>(string propertyName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        this.idPropertyOverrides[typeof(T)] = propertyName;
        return this;
    }

    /// <summary>
    /// Registers a converter so a document Id can be a CLR type beyond the built-in
    /// <see cref="Guid"/>, <see cref="int"/>, <see cref="long"/>, and <see cref="string"/> —
    /// for example a <c>Ulid</c> or a strongly-typed wrapper such as <c>record struct OrderId(Guid Value)</c>.
    /// The Id is still stored as a string in every provider; the converter defines how it round-trips.
    /// </summary>
    public DocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.idConverters.Register(converter);
        return this;
    }

    /// <summary>
    /// Registers a custom Id type using inline delegates. <paramref name="isDefault"/> controls when an Id
    /// counts as "unset" (auto-generate on Insert); <paramref name="generate"/> optionally produces new Ids.
    /// </summary>
    public DocumentStoreOptions MapIdType<TId>(
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

    /// <summary>
    /// Switches auto-generation of <see cref="Guid"/> document Ids to <b>version 7</b> (time-ordered,
    /// sortable) GUIDs instead of the default random version 4. No new dependency — uses
    /// <see cref="Guid.CreateVersion7()"/>. Storage format is unchanged, so it is a drop-in for existing
    /// Guid-keyed data. Shorthand for <c>MapIdType(new GuidV7IdConverter())</c>.
    /// </summary>
    public DocumentStoreOptions UseGuidV7Ids() => this.MapIdType(new GuidV7IdConverter());

    internal string ResolveTableName(string typeName)
        => this.typeMappings.TryGetValue(typeName, out var table) ? table : this.TableName;

    internal string? ResolveIdPropertyName(Type type)
        => this.idPropertyOverrides.TryGetValue(type, out var name) ? name : null;

    internal IdConverterRegistry IdConverters => this.idConverters;

    /// <summary>
    /// Registers a global query filter that is AND-applied to every query of type
    /// <typeparamref name="T"/> — including <c>Query&lt;T&gt;()</c>, single-document operations
    /// (<c>Get</c>, <c>Remove</c>, <c>Update</c>, <c>SetProperty</c>, <c>RemoveProperty</c>,
    /// <c>Clear</c>), and bulk operations (<c>ExecuteUpdate</c>, <c>ExecuteDelete</c>).
    /// <para>
    /// <c>Insert</c> and the insert-path of <c>Upsert</c> do not enforce filters — matching
    /// Entity Framework Core semantics. Raw SQL queries are also unaffected. Disable filters on
    /// a per-query basis with <see cref="IDocumentQuery{T}.IgnoreQueryFilters()"/>.
    /// </para>
    /// </summary>
    public DocumentStoreOptions AddQueryFilter<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(null, predicate);
    }

    /// <summary>
    /// Registers a <b>named</b> global query filter. Multiple named filters can be registered for
    /// the same type and individually disabled with
    /// <see cref="IDocumentQuery{T}.IgnoreQueryFilters(string[])"/>.
    /// </summary>
    public DocumentStoreOptions AddQueryFilter<T>(string name, Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(name, predicate);
    }

    DocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        if (!this.queryFilters.TryGetValue(typeof(T), out var list))
            this.queryFilters[typeof(T)] = list = new List<QueryFilter>();
        list.Add(new QueryFilter(name, predicate));
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type)
        => this.queryFilters.TryGetValue(type, out var list)
            ? list
            : Array.Empty<QueryFilter>();

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency.
    /// On insert the version is set to 1. On update the version is checked and incremented.
    /// If the stored version does not match the expected version, a <see cref="ConcurrencyException"/> is thrown.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression; the type is user-constructed and not subject to trimming.")]
    public DocumentStoreOptions MapVersionProperty<T>(Expression<Func<T, int>> property) where T : class
    {
        var body = property.Body;
        if (body is not MemberExpression member)
            throw new ArgumentException(
                "Expression must be a simple property access (e.g., x => x.Version).",
                nameof(property));

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

    /// <summary>
    /// Maps a version property on a document type for optimistic concurrency.
    /// AOT-safe overload that accepts direct accessor delegates.
    /// </summary>
    public DocumentStoreOptions MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(getter);
        ArgumentNullException.ThrowIfNull(setter);

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

    /// <summary>
    /// Enables append-only system-time temporal history for <typeparamref name="T"/>. Every
    /// Insert/Update/Upsert/Remove writes a versioned snapshot to a <c>{table}_history</c> sidecar
    /// table, so the document's state can be read back as of any point in time via
    /// <see cref="DocumentStore.History{T}"/>, <see cref="DocumentStore.AsOf{T}"/>,
    /// <see cref="DocumentStore.Restore{T}"/>, and <see cref="DocumentStore.GetDiffBetween{T}"/>.
    /// <para>
    /// Opt-in and per type — only mapped types incur the extra history write. History tracking is
    /// only supported on providers that report <see cref="IDatabaseProvider.SupportsTemporal"/>.
    /// Bulk <c>Clear</c> does not record per-document history.
    /// </para>
    /// </summary>
    public DocumentStoreOptions MapTemporal<T>(Action<TemporalOptions>? configure = null) where T : class
    {
        var opts = new TemporalOptions();
        configure?.Invoke(opts);
        if (opts.MaxVersions is <= 0)
            throw new ArgumentOutOfRangeException(nameof(configure), "TemporalOptions.MaxVersions must be greater than zero.");

        this.temporalMappings[typeof(T)] = new TemporalMapping
        {
            DocumentType = typeof(T),
            Retention = opts.Retention,
            MaxVersions = opts.MaxVersions,
            CaptureActor = opts.CaptureActor
        };
        return this;
    }

    internal TemporalMapping? ResolveTemporalMapping(Type type)
        => this.temporalMappings.TryGetValue(type, out var mapping) ? mapping : null;

    /// <summary>
    /// Declares that type T has a GeoPoint property to be used for spatial queries.
    /// Only supported by SQLite and CosmosDB providers.
    /// Uses an expression to identify the property name; the accessor is built via PropertyInfo.
    /// For full AOT safety, use the overload accepting a string propertyName and Func delegate.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression; the type is user-constructed and not subject to trimming.")]
    public DocumentStoreOptions MapSpatialProperty<T>(Expression<Func<T, GeoPoint>> property) where T : class
    {
        var body = property.Body;
        if (body is not MemberExpression member)
            throw new ArgumentException(
                "Expression must be a simple property access (e.g., x => x.Location).",
                nameof(property));

        var propertyName = member.Member.Name;
        var propInfo = typeof(T).GetProperty(propertyName)
            ?? throw new ArgumentException($"Property '{propertyName}' not found on type '{typeof(T).Name}'.");

        this.spatialMappings[typeof(T)] = new SpatialMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            JsonPath = null!, // resolved lazily when JsonSerializerOptions are available
            GetGeoPoint = obj => (GeoPoint)propInfo.GetValue(obj)!
        };
        return this;
    }

    /// <summary>
    /// Declares that type T has a GeoPoint property to be used for spatial queries.
    /// AOT-safe overload that accepts a direct accessor delegate.
    /// </summary>
    public DocumentStoreOptions MapSpatialProperty<T>(string propertyName, Func<T, GeoPoint> accessor) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);
        ArgumentNullException.ThrowIfNull(accessor);

        this.spatialMappings[typeof(T)] = new SpatialMapping
        {
            DocumentType = typeof(T),
            PropertyName = propertyName,
            JsonPath = null!, // resolved lazily when JsonSerializerOptions are available
            GetGeoPoint = obj => accessor((T)obj)
        };
        return this;
    }

    internal SpatialMapping? ResolveSpatialMapping(Type type) =>
        this.spatialMappings.TryGetValue(type, out var mapping) ? mapping : null;

    internal void ResolveSpatialJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.spatialMappings.Values)
        {
            if (mapping.JsonPath != null!)
                continue;

            var jsonName = jsonOptions.PropertyNamingPolicy?.ConvertName(mapping.PropertyName) ?? mapping.PropertyName;
            mapping.JsonPath = jsonName;
        }
    }

    /// <summary>
    /// Declares that type T has a <see cref="ReadOnlyMemory{T}"/> embedding property
    /// to be used for ANN vector search. Throws on providers that don't support vectors
    /// (LiteDB, IndexedDB) at registration time.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression; the type is user-constructed and not subject to trimming.")]
    public DocumentStoreOptions MapVectorProperty<T>(
        Expression<Func<T, ReadOnlyMemory<float>>> property,
        int dimensions,
        VectorDistance metric = VectorDistance.Cosine,
        VectorIndexKind indexKind = VectorIndexKind.Hnsw,
        Action<VectorIndexOptions>? configureIndex = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(property);
        if (dimensions <= 0)
            throw new ArgumentOutOfRangeException(nameof(dimensions), "Dimensions must be > 0.");

        if (property.Body is not MemberExpression member)
            throw new ArgumentException(
                "Expression must be a simple property access (e.g., x => x.Embedding).",
                nameof(property));

        var propertyName = member.Member.Name;
        var propInfo = typeof(T).GetProperty(propertyName)
            ?? throw new ArgumentException($"Property '{propertyName}' not found on type '{typeof(T).Name}'.");

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

    /// <summary>
    /// AOT-safe overload of <see cref="MapVectorProperty{T}(Expression{Func{T, ReadOnlyMemory{float}}}, int, VectorDistance, VectorIndexKind, Action{VectorIndexOptions}?)"/>
    /// that accepts direct accessor and setter delegates.
    /// </summary>
    public DocumentStoreOptions MapVectorProperty<T>(
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
        if (dimensions <= 0)
            throw new ArgumentOutOfRangeException(nameof(dimensions), "Dimensions must be > 0.");

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

    internal VectorMapping? ResolveVectorMapping(Type type) =>
        this.vectorMappings.TryGetValue(type, out var mapping) ? mapping : null;

    internal void ResolveVectorJsonPaths(JsonSerializerOptions jsonOptions)
    {
        foreach (var mapping in this.vectorMappings.Values)
        {
            if (mapping.JsonPath != null!)
                continue;

            var jsonName = jsonOptions.PropertyNamingPolicy?.ConvertName(mapping.PropertyName) ?? mapping.PropertyName;
            mapping.JsonPath = jsonName;
        }
    }

    /// <summary>
    /// Registers a callback that runs on every document before <c>Insert</c>, <c>BatchInsert</c>,
    /// and <c>Upsert</c> serialize and persist it. Used by Shiny.DocumentDb.Extensions.AI to
    /// auto-populate vector embeddings, but generally available for any "fill in computed fields"
    /// scenario. Handlers run in registration order.
    /// </summary>
    internal FunctionTranslationRegistry FunctionRegistry { get; } = new();

    /// <summary>
    /// Registers a custom function translation for relational <c>Where</c> predicates. The exemplar
    /// captures the target method (so it stays trim/AOT-safe — no reflection by name); calls to that
    /// method are emitted as <paramref name="sqlFunctionName"/><c>(arg0, …)</c> by the relational
    /// providers. Make sure the function exists (or is registered as a UDF) on the target database.
    /// </summary>
    /// <example>
    /// <code>options.MapFunctionTranslation(() => MyFunctions.Reverse(default!), "REVERSE");</code>
    /// </example>
    public DocumentStoreOptions MapFunctionTranslation(Expression<Func<object?>> exemplar, string sqlFunctionName)
    {
        ArgumentNullException.ThrowIfNull(exemplar);
        ArgumentException.ThrowIfNullOrWhiteSpace(sqlFunctionName);

        var body = exemplar.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;

        if (body is not MethodCallExpression call)
            throw new ArgumentException("Expression must be a method call, e.g. () => MyFunctions.Foo(default!).", nameof(exemplar));

        this.FunctionRegistry.Add(call.Method, sqlFunctionName);
        return this;
    }

    public DocumentStoreOptions OnBeforeInsert<T>(Func<T, CancellationToken, Task> handler) where T : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        this.beforeInsertHooks.Add(async (obj, ct) =>
        {
            if (obj is T typed)
                await handler(typed, ct).ConfigureAwait(false);
        });
        return this;
    }

    internal IReadOnlyList<Func<object, CancellationToken, Task>> ResolveBeforeInsertHooks() => this.beforeInsertHooks;

    // ── Write interceptors ──────────────────────────────────────────────
    internal readonly List<IDocumentInterceptor> interceptors = new();
    internal readonly List<IDocumentBulkInterceptor> bulkInterceptors = new();

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public DocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);
        this.interceptors.Add(interceptor);
        return this;
    }

    /// <summary>Registers a set-based (bulk) write interceptor. Registration order = execution order.</summary>
    public DocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor)
    {
        ArgumentNullException.ThrowIfNull(interceptor);
        this.bulkInterceptors.Add(interceptor);
        return this;
    }

    /// <summary>Registers a before-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public DocumentStoreOptions OnBeforeWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        this.interceptors.Add(new LambdaInterceptor(typeof(T), handler, null));
        return this;
    }

    /// <summary>Registers an after-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public DocumentStoreOptions OnAfterWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        this.interceptors.Add(new LambdaInterceptor(typeof(T), null, handler));
        return this;
    }

    internal IReadOnlyList<IDocumentInterceptor> ResolveInterceptors() => this.interceptors;
    internal IReadOnlyList<IDocumentBulkInterceptor> ResolveBulkInterceptors() => this.bulkInterceptors;

    static string ExtractPropertyName<T>(Expression<Func<T, object>> expression)
    {
        var body = expression.Body;

        // Unwrap Convert (boxing value types to object)
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;

        if (body is MemberExpression member)
            return member.Member.Name;

        throw new ArgumentException(
            "Expression must be a simple property access (e.g., x => x.MyId).",
            nameof(expression));
    }
}
