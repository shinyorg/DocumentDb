using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.RavenDb;

public class RavenDbDocumentStoreOptions : IDocumentStoreOptions
{
    /// <summary>The shared per-type mapping state — see <see cref="DocumentMappingRegistry"/>.</summary>
    internal DocumentMappingRegistry Mappings { get; } = new();


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
        this.Mappings.IdConverters.Register(converter);
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
        this.Mappings.IdConverters.Register(new DelegateIdConverter<TId>(toString, parse, isDefault, generate));
        return this;
    }

    /// <summary>Overrides the Id property name for a document type (default is <c>Id</c>).</summary>
    public RavenDbDocumentStoreOptions MapIdProperty<T>(Expression<Func<T, object>> idProperty) where T : class
    {
        ArgumentNullException.ThrowIfNull(idProperty);
        this.Mappings.MapIdProperty(idProperty);
        return this;
    }

    internal string? ResolveIdPropertyName(Type type) => this.Mappings.ResolveIdPropertyName(type);

    internal IdConverterRegistry IdConverters => this.Mappings.IdConverters;

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
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

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
    public RavenDbDocumentStoreOptions MapVersionProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression<Func<T, int>> property) where T : class
    {
        this.Mappings.MapVersionProperty(property);
        return this;
    }

    /// <summary>Maps a version property on a document type for optimistic concurrency. AOT-safe overload.</summary>
    public RavenDbDocumentStoreOptions MapVersionProperty<T>(string propertyName, Func<T, int> getter, Action<T, int> setter) where T : class
    {
        this.Mappings.MapVersionProperty(propertyName, getter, setter);
        return this;
    }

    // ── Blobs ──────────────────────────────────────────────────────────────

    /// <summary>See <see cref="DocumentStoreOptions.MapBlob{T}(Expression{Func{T, DocumentBlob}}, Action{BlobOptions})"/>.</summary>
    public RavenDbDocumentStoreOptions MapBlob<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression<Func<T, DocumentBlob?>> property, Action<BlobOptions>? configure = null) where T : class
    {
        var o = new BlobOptions();
        configure?.Invoke(o);
        this.AddBlob(BlobMappingFactory.FromExpression(property, o));
        return this;
    }

    /// <summary>See <see cref="DocumentStoreOptions.MapBlobCollection{T}(Expression{Func{T, DocumentBlobCollection}}, Action{BlobOptions})"/>.</summary>
    public RavenDbDocumentStoreOptions MapBlobCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(Expression<Func<T, DocumentBlobCollection?>> property, Action<BlobOptions>? configure = null) where T : class
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
