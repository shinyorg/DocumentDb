using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.RavenDb;

public class RavenDbDocumentStoreOptions
{
    readonly Dictionary<Type, string> idPropertyOverrides = new();
    readonly IdConverterRegistry idConverters = new();
    readonly Dictionary<Type, List<QueryFilter>> queryFilters = new();
    internal readonly Dictionary<Type, VersionMapping> versionMappings = new();

    /// <summary>
    /// A pre-built RavenDB <see cref="global::Raven.Client.Documents.IDocumentStore"/>. When set, the provider
    /// uses it as-is and does not dispose it. When null, one is built from <see cref="Urls"/> +
    /// <see cref="Database"/> (+ optional <see cref="Certificate"/>) and owned/disposed by the provider.
    /// </summary>
    public global::Raven.Client.Documents.IDocumentStore? DocumentStore { get; set; }

    /// <summary>RavenDB server URL(s). Required when <see cref="DocumentStore"/> is not supplied.</summary>
    public string[]? Urls { get; set; }

    /// <summary>Target database. Required when <see cref="DocumentStore"/> is not supplied.</summary>
    public string? Database { get; set; }

    /// <summary>Optional X.509 client certificate for a secured RavenDB cluster.</summary>
    public X509Certificate2? Certificate { get; set; }

    public TypeNameResolution TypeNameResolution { get; set; } = TypeNameResolution.ShortName;
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>
    /// When false, calling a reflection-based overload (without JsonTypeInfo&lt;T&gt;) throws an
    /// InvalidOperationException if the type cannot be resolved from the configured TypeInfoResolver.
    /// Defaults to true.
    /// </summary>
    public bool UseReflectionFallback { get; set; } = true;

    /// <summary>Optional callback invoked with diagnostic messages.</summary>
    public Action<string>? Logging { get; set; }

    // ── Id types ────────────────────────────────────────────────────────
    /// <summary>Registers a converter so a document Id can be a CLR type beyond Guid/int/long/string.</summary>
    public RavenDbDocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.idConverters.Register(converter);
        return this;
    }

    /// <summary>Registers a custom Id type using inline delegates.</summary>
    public RavenDbDocumentStoreOptions MapIdType<TId>(
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

    /// <summary>Overrides the Id property name for a document type (default is <c>Id</c>).</summary>
    public RavenDbDocumentStoreOptions MapIdProperty<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        ArgumentNullException.ThrowIfNull(idProperty);
        this.idPropertyOverrides[typeof(T)] = ExtractPropertyName(idProperty);
        return this;
    }

    internal string? ResolveIdPropertyName(Type type)
        => this.idPropertyOverrides.TryGetValue(type, out var name) ? name : null;

    internal IdConverterRegistry IdConverters => this.idConverters;

    // ── Query filters ───────────────────────────────────────────────────
    /// <summary>Registers a global query filter for <typeparamref name="T"/>.</summary>
    public RavenDbDocumentStoreOptions AddQueryFilter<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(null, predicate);
    }

    /// <summary>Registers a named global query filter for <typeparamref name="T"/>.</summary>
    public RavenDbDocumentStoreOptions AddQueryFilter<T>(string name, Expression<Func<T, bool>> predicate) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(predicate);
        return this.AddQueryFilterInternal<T>(name, predicate);
    }

    RavenDbDocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
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
    public RavenDbDocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor.</summary>
    public RavenDbDocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

    /// <summary>Registers a before-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public RavenDbDocumentStoreOptions OnBeforeWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddBefore<T>(handler); return this; }

    /// <summary>Registers an after-write callback scoped to documents of type <typeparamref name="T"/>.</summary>
    public RavenDbDocumentStoreOptions OnAfterWrite<T>(Func<DocumentWriteContext, CancellationToken, Task> handler) where T : class { this.Interceptors.AddAfter<T>(handler); return this; }

    // ── Optimistic concurrency ──────────────────────────────────────────
    /// <summary>Maps a version property on a document type for optimistic concurrency.</summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Property is resolved by name from a user-provided expression.")]
    public RavenDbDocumentStoreOptions MapVersionProperty<T>(Expression<Func<T, int>> property) where T : class
    {
        var body = property.Body;
        if (body is not MemberExpression member)
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

    /// <summary>Maps a version property on a document type for optimistic concurrency. AOT-safe overload.</summary>
    public RavenDbDocumentStoreOptions MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
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
            "Expression must be a simple property access (e.g., x => x.MyId).",
            nameof(expression));
    }
}
