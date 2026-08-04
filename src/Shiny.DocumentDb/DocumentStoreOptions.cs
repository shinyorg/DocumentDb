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

public class DocumentStoreOptions : IDocumentStoreOptions
{
    // ── IDocumentStoreOptions (explicit — the provider-agnostic slice; the typed overloads below stay fluent) ──
    IDocumentStoreOptions IDocumentStoreOptions.AddInterceptor(IDocumentInterceptor interceptor)
        => this.AddInterceptor(interceptor);

    IDocumentStoreOptions IDocumentStoreOptions.AddBulkInterceptor(IDocumentBulkInterceptor interceptor)
        => this.AddBulkInterceptor(interceptor);
    JsonSerializerOptions? IDocumentStoreOptions.SerializerOptions
    {
        get => this.JsonSerializerOptions;
        set => this.JsonSerializerOptions = value;
    }

    // The relational default carries a reflection resolver unless the store is in strict/AOT mode, so it is
    // built by the store rather than by the interface's provider-agnostic default.
    JsonSerializerOptions IDocumentStoreOptions.EnsureSerializerOptions()
        => this.JsonSerializerOptions ??= DocumentStore.CreateDefaultJsonOptions(this.UseReflectionFallback);

    /// <summary>The per-type mapping state — shared with every other provider's options class.</summary>
    public DocumentMappingRegistry Mappings { get; } = new();

    /// <summary>
    /// What this store's relational backend supports.
    /// <para>
    /// Spatial, vector and full-text read <c>true</c> here even when the engine's own flag is off, because the
    /// relational path <b>degrades</b> rather than fails: mapping a vector on plain SQLite simply skips the
    /// sidecar index until the <c>Shiny.DocumentDb.Sqlite.VectorSupport</c> package is added, and the same
    /// mapping is valid before and after. The validator exists to catch mappings that can <i>never</i> work —
    /// which on the relational family is only temporal history, whose sidecar the engine must actually support.
    /// </para>
    /// </summary>
    DocumentStoreCapabilities IDocumentStoreOptions.Capabilities => new()
    {
        ProviderName = this.DatabaseProvider.GetType().Name.Replace("DatabaseProvider", string.Empty),
        PerTypeStorageName = true,
        Spatial = true,
        Vector = true,
        FullText = true,
        Temporal = this.DatabaseProvider.SupportsTemporal,
        Blobs = true,
        // Alias mode needs no engine support; SupportsComputedColumns only decides whether the value is
        // materialized into a native column, which ResolveComputedColumns already degrades gracefully.
        ComputedProperties = true
    };

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
    /// When true, the store never issues the lazy <c>CREATE TABLE IF NOT EXISTS</c> / index DDL that
    /// otherwise runs once per table on first touch — including on reads. For pointing a store at a
    /// database this process does not own: a read replica, an account without DDL rights, or an admin
    /// tool that has promised not to change anything. The table must already exist; if it does not,
    /// the first query fails with the provider's own "no such table" error rather than creating one.
    /// Defaults to false.
    /// </summary>
    public bool SkipTableInitialization { get; set; }

    /// <summary>
    /// Optional callback invoked with every SQL statement the store executes. Useful for debugging and
    /// diagnostics. This composes with structured logging: when the store is registered via
    /// <c>AddDocumentStore</c> and an <c>ILoggerFactory</c> is in the container, every SQL statement is also
    /// logged through <c>ILogger</c> at <c>Debug</c> under the <c>Shiny.DocumentDb</c> category (control it with
    /// <c>Logging:LogLevel:Shiny.DocumentDb</c>) — this callback still fires either way.
    /// </summary>
    public Action<string>? Logging { get; set; }

    /// <summary>Logical store name, set by the keyed <c>AddDocumentStore(name, …)</c> registrations. When set,
    /// every embedded metric measurement and span from this store is tagged <c>db.namespace</c> so signals from
    /// multiple stores can be told apart. Null for the non-keyed registration.</summary>
    internal string? StoreName { get; set; }

    /// <summary>
    /// When set, enables shared-table multi-tenancy. All queries are filtered by TenantId
    /// and all inserts include the TenantId value. A dedicated TenantId column and index
    /// are created in the table schema automatically.
    /// The function is called on every operation to resolve the current tenant.
    /// </summary>
    public Func<string>? TenantIdAccessor { get; set; }

    /// <summary>
    /// Registers a converter so a document Id can be a CLR type beyond the built-in
    /// <see cref="Guid"/>, <see cref="int"/>, <see cref="long"/>, and <see cref="string"/> —
    /// for example a <c>Ulid</c> or a strongly-typed wrapper such as <c>record struct OrderId(Guid Value)</c>.
    /// The Id is still stored as a string in every provider; the converter defines how it round-trips.
    /// </summary>
    public DocumentStoreOptions MapIdType<TId>(DocumentIdConverter<TId> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        this.Mappings.IdConverters.Register(converter);
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
        this.Mappings.IdConverters.Register(new DelegateIdConverter<TId>(toString, parse, isDefault, generate));
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
        => this.Mappings.ResolveMappedName(typeName, this.TableName);

    /// <summary>Every distinct document table the store writes to: the default shared table plus any
    /// explicitly mapped via <c>MapTypeToTable</c>. Used by the bulk export path.</summary>
    internal IReadOnlyCollection<string> AllDocumentTableNames()
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { this.TableName };
        foreach (var table in this.Mappings.MappedNames)
            set.Add(table);
        return set;
    }

    internal string? ResolveIdPropertyName(Type type) => this.Mappings.ResolveIdPropertyName(type);

    internal IdConverterRegistry IdConverters => this.Mappings.IdConverters;

    DocumentStoreOptions AddQueryFilterInternal<T>(string? name, Expression<Func<T, bool>> predicate) where T : class
    {
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    internal IReadOnlyList<QueryFilter> ResolveQueryFilters(Type type) => this.Mappings.ResolveQueryFilters(type);

    internal VersionMapping? ResolveVersionMapping(Type type) => this.Mappings.ResolveVersionMapping(type);

    internal void ResolveVersionJsonPaths(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveVersionJsonPaths(jsonOptions);

    internal TemporalMapping? ResolveTemporalMapping(Type type) => this.Mappings.ResolveTemporalMapping(type);

    internal SpatialMapping? ResolveSpatialMapping(Type type) => this.Mappings.ResolveSpatialMapping(type);

    internal void ResolveSpatialJsonPaths(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveSpatialJsonPaths(jsonOptions);

    internal VectorMapping? ResolveVectorMapping(Type type) => this.Mappings.ResolveVectorMapping(type);

    internal void ResolveVectorJsonPaths(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveVectorJsonPaths(jsonOptions);

    internal FullTextMapping? ResolveFullTextMapping(Type type) => this.Mappings.ResolveFullTextMapping(type);

    internal void ResolveFullTextJsonPaths(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveFullTextJsonPaths(jsonOptions);

    internal IReadOnlyList<BlobMapping> ResolveBlobMappings(Type type) => this.Mappings.ResolveBlobMappings(type);

    internal bool HasBlobMappings => this.Mappings.BlobMappings.Count > 0;

    internal IReadOnlyList<ComputedMapping> ResolveComputedMappings(Type type) => this.Mappings.ResolveComputedMappings(type);

    /// <summary>A name → mapping lookup (CLR property name, case-insensitive) for the query layer, or null if none.</summary>
    internal IReadOnlyDictionary<string, ComputedMapping>? ResolveComputedLookup(Type type) => this.Mappings.ResolveComputedLookup(type);

    internal void ResolveComputedJsonNames(JsonSerializerOptions jsonOptions) => this.Mappings.ResolveComputedJsonNames(jsonOptions);

    /// <summary>
    /// Decides which materialize-requested computed mappings actually get a native column on this store's
    /// provider. Sets <see cref="ComputedMapping.MaterializedColumn"/> so the query layer references the
    /// column; mappings left null fall back to alias mode. The DDL itself is emitted at table init.
    /// </summary>
    internal void ResolveComputedColumns(IDatabaseProvider provider) => this.Mappings.Computed.ResolveColumns(provider);

    /// <summary>Custom function-name translations for relational <c>Where</c> predicates (see <see cref="MapFunctionTranslation"/>).</summary>
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

    // ── Write interceptors ──────────────────────────────────────────────
    internal InterceptorPipeline Interceptors => this.Mappings.Interceptors;

    /// <summary>Registers a per-document write interceptor. Registration order = execution order.</summary>
    public DocumentStoreOptions AddInterceptor(IDocumentInterceptor interceptor) { this.Interceptors.Add(interceptor); return this; }

    /// <summary>Registers a set-based (bulk) write interceptor. Registration order = execution order.</summary>
    public DocumentStoreOptions AddBulkInterceptor(IDocumentBulkInterceptor interceptor) { this.Interceptors.AddBulk(interceptor); return this; }

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
