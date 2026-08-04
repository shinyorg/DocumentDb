using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// Everything a single document type needs configured, in one block. Obtained from
/// <see cref="ConfigureDocumentExtensions.ConfigureDocument{T}"/>:
/// <code>
/// options.ConfigureDocument&lt;Patient&gt;(cfg =>
/// {
///     cfg.Table = "Patients";
///     cfg.MapIdProperty(x => x.Id);
///     cfg.AddSoftDelete(x => x.IsDeleted);
///     cfg.MapProperty(x => x.Ssn, p => p.Encrypt(EncryptionMode.Deterministic));
/// });
/// </code>
/// <para>
/// Every member writes into the store's shared <see cref="DocumentMappingRegistry"/>, so the builder is one
/// implementation for all providers. Whether a mapped feature is actually supported by the backend you chose
/// is settled by the configuration validation pass when the store is built — you get one aggregated error at
/// startup naming every problem, rather than a throw per call here.
/// </para>
/// <para>
/// Provider packages extend this type with their own vocabulary and features (<c>ToContainer</c> on Cosmos,
/// <c>MapIndexedProperty</c> on DynamoDB), which is why those only appear once you reference the package.
/// </para>
/// </summary>
public sealed class DocumentTypeBuilder<T> where T : class
{
    internal DocumentTypeBuilder(IDocumentStoreOptions options)
    {
        this.Options = options;
        this.TypeName = TypeNameResolver.Resolve(typeof(T), options.TypeNameResolution);
    }

    /// <summary>The store options being configured — the seam extension methods build on.</summary>
    public IDocumentStoreOptions Options { get; }

    /// <summary>The shared per-type mapping state every member here writes into.</summary>
    public DocumentMappingRegistry Mappings => this.Options.Mappings;

    /// <summary>The resolved document type name, per the store's <see cref="TypeNameResolution"/>.</summary>
    public string TypeName { get; }

    /// <summary>
    /// The storage unit this type lives in — a table on the relational providers, a collection on
    /// MongoDB/LiteDB/Firestore, a container on Cosmos, an object store on IndexedDB. Null (the default)
    /// leaves the type in the store's shared default. Providers with no per-type storage unit (Redis,
    /// RavenDB) reject this at build time.
    /// </summary>
    public string? Table
    {
        get => this.Mappings.ResolveMappedName(this.TypeName, null!);
        set
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(value, nameof(this.Table));
            this.Mappings.MapTypeName(this.TypeName, value, nameof(this.Table));
        }
    }

    // ── Identity ────────────────────────────────────────────────────────

    /// <summary>
    /// Overrides the Id property. Use this when the document has no property literally named <c>Id</c>
    /// (for example <c>UserId</c>, <c>DeviceKey</c>). The property must be a <see cref="Guid"/>,
    /// <see cref="int"/>, <see cref="long"/>, <see cref="string"/>, or a type with a registered
    /// <see cref="DocumentIdConverter{TId}"/>.
    /// </summary>
    public DocumentTypeBuilder<T> MapIdProperty(Expression<Func<T, object>> idProperty)
    {
        this.Mappings.MapIdProperty(idProperty);
        return this;
    }

    /// <summary>AOT-safe overload of <see cref="MapIdProperty(Expression{Func{T, object}})"/> taking the property name.</summary>
    public DocumentTypeBuilder<T> MapIdProperty(string propertyName)
    {
        this.Mappings.MapIdProperty<T>(propertyName);
        return this;
    }

    // ── Optimistic concurrency ──────────────────────────────────────────

    /// <summary>
    /// Maps an <c>int</c> version property for optimistic concurrency. Insert seeds version 1; Update checks
    /// and increments it, throwing <see cref="ConcurrencyException"/> when the stored version has moved on.
    /// </summary>
    public DocumentTypeBuilder<T> MapVersionProperty([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Expression<Func<T, int>> property)
    {
        this.Mappings.MapVersionProperty(property);
        return this;
    }

    /// <summary>AOT-safe overload of <see cref="MapVersionProperty(Expression{Func{T, int}})"/> taking explicit accessors.</summary>
    public DocumentTypeBuilder<T> MapVersionProperty(string propertyName, Func<T, int> getter, Action<T, int> setter)
    {
        this.Mappings.MapVersionProperty(propertyName, getter, setter);
        return this;
    }

    // ── Query filters ───────────────────────────────────────────────────

    /// <summary>
    /// Registers a global query filter AND-applied to every read of this type — <c>Query&lt;T&gt;()</c>,
    /// single-document operations (<c>Get</c>, <c>Remove</c>, <c>Update</c>, <c>SetProperty</c>,
    /// <c>RemoveProperty</c>, <c>Clear</c>) and bulk operations (<c>ExecuteUpdate</c>, <c>ExecuteDelete</c>).
    /// <c>Insert</c> and the insert path of <c>Upsert</c> do not enforce filters, matching EF Core. Lift it
    /// per query with <see cref="IDocumentQuery{T}.IgnoreQueryFilters()"/>.
    /// </summary>
    public DocumentTypeBuilder<T> AddQueryFilter(Expression<Func<T, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        this.Mappings.AddQueryFilter(null, predicate);
        return this;
    }

    /// <summary>
    /// Registers a <b>named</b> global query filter, so it can be lifted individually with
    /// <see cref="IDocumentQuery{T}.IgnoreQueryFilters(string[])"/>. Several named filters can coexist.
    /// </summary>
    public DocumentTypeBuilder<T> AddQueryFilter(string name, Expression<Func<T, bool>> predicate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(predicate);
        this.Mappings.AddQueryFilter(name, predicate);
        return this;
    }

    // ── Per-property options ────────────────────────────────────────────

    /// <summary>
    /// Configures a single property — today field-level encryption, and the growth point for future
    /// per-property concerns (provider packages add their own there).
    /// <code>cfg.MapProperty(x => x.Ssn, p => p.Encrypt(EncryptionMode.Deterministic));</code>
    /// </summary>
    public DocumentTypeBuilder<T> MapProperty(Expression<Func<T, object?>> property, Action<DocumentPropertyBuilder<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(property);
        ArgumentNullException.ThrowIfNull(configure);
        configure(new DocumentPropertyBuilder<T>(this.Options, property));
        return this;
    }

    // ── Spatial ─────────────────────────────────────────────────────────

    /// <summary>
    /// Declares the <see cref="GeoPoint"/> property this type is searched by — the input to
    /// <c>NearestPoints</c>, <c>WithinRadius</c>, and the bounding-box queries. One per type.
    /// </summary>
    public DocumentTypeBuilder<T> MapSpatialProperty([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Expression<Func<T, GeoPoint?>> property)
    {
        this.Mappings.MapSpatialProperty(property);
        return this;
    }

    /// <summary>AOT-safe overload taking a direct accessor delegate.</summary>
    public DocumentTypeBuilder<T> MapSpatialProperty(string propertyName, Func<T, GeoPoint?> accessor)
    {
        this.Mappings.MapSpatialProperty(propertyName, accessor);
        return this;
    }

    /// <summary>
    /// Declares the full <see cref="Geometry"/> property (LineString / Polygon / Multi* / collection, or a
    /// point) this type is searched by. One per type — a type has either a point or a geometry mapping.
    /// </summary>
    public DocumentTypeBuilder<T> MapSpatialProperty([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Expression<Func<T, Geometry?>> property)
    {
        this.Mappings.MapSpatialProperty(property);
        return this;
    }

    /// <summary>AOT-safe overload taking a direct accessor delegate.</summary>
    public DocumentTypeBuilder<T> MapSpatialProperty(string propertyName, Func<T, Geometry?> accessor)
    {
        this.Mappings.MapSpatialProperty(propertyName, accessor);
        return this;
    }

    // ── Vector ──────────────────────────────────────────────────────────

    /// <summary>
    /// Declares the <see cref="ReadOnlyMemory{T}"/> embedding property used for ANN vector search. Leave
    /// <paramref name="indexKind"/> null to take the provider's own default (DiskANN on Cosmos, HNSW
    /// elsewhere). One per type.
    /// </summary>
    public DocumentTypeBuilder<T> MapVectorProperty(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Expression<Func<T, ReadOnlyMemory<float>>> property,
        int dimensions,
        VectorDistance metric = VectorDistance.Cosine,
        VectorIndexKind? indexKind = null,
        Action<VectorIndexOptions>? configureIndex = null)
    {
        this.Mappings.MapVectorProperty(property, dimensions, metric, indexKind, configureIndex);
        return this;
    }

    /// <summary>AOT-safe overload taking explicit getter/setter delegates.</summary>
    public DocumentTypeBuilder<T> MapVectorProperty(
        string propertyName,
        Func<T, ReadOnlyMemory<float>> getter,
        Action<T, ReadOnlyMemory<float>> setter,
        int dimensions,
        VectorDistance metric = VectorDistance.Cosine,
        VectorIndexKind? indexKind = null,
        Action<VectorIndexOptions>? configureIndex = null)
    {
        this.Mappings.MapVectorProperty(propertyName, getter, setter, dimensions, metric, indexKind, configureIndex);
        return this;
    }

    // ── Full text ───────────────────────────────────────────────────────

    /// <summary>
    /// Declares a string property searchable via <see cref="IDocumentStore.FullTextSearch{T}"/>. The index is
    /// created at table init; a type must be mapped before it can be searched (there is no ad-hoc full text).
    /// </summary>
    public DocumentTypeBuilder<T> MapFullTextProperty(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Expression<Func<T, string?>> property,
        FullTextLanguage language = FullTextLanguage.English)
    {
        ArgumentNullException.ThrowIfNull(property);
        this.Mappings.MapFullTextProperty([property], language);
        return this;
    }

    /// <summary>Declares several string properties combined into a single full-text index.</summary>
    public DocumentTypeBuilder<T> MapFullTextProperty(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] IReadOnlyList<Expression<Func<T, string?>>> properties,
        FullTextLanguage language = FullTextLanguage.English)
    {
        this.Mappings.MapFullTextProperty(properties, language);
        return this;
    }

    /// <summary>
    /// AOT-safe overload mapping full text to a direct text selector — use it to combine fields or index a
    /// collection of strings (e.g. tags). <paramref name="propertyNames"/> are the JSON paths the database
    /// index covers; <paramref name="textSelector"/> supplies the same text from a live object for the
    /// in-memory fallback providers.
    /// </summary>
    public DocumentTypeBuilder<T> MapFullTextProperty(
        IReadOnlyList<string> propertyNames,
        Func<T, IEnumerable<string?>> textSelector,
        FullTextLanguage language = FullTextLanguage.English)
    {
        this.Mappings.MapFullTextProperty(propertyNames, textSelector, language);
        return this;
    }

    // ── Computed ────────────────────────────────────────────────────────

    /// <summary>
    /// Maps a computed property — a value derived from other fields (<c>o =&gt; o.Quantity * o.UnitPrice</c>)
    /// that is not stored in the document JSON but can be filtered, sorted and projected like a normal one.
    /// The property should be <c>[JsonIgnore]</c> with a setter; pass <paramref name="indexed"/> to request a
    /// materialized (native computed) column where the provider supports it.
    /// </summary>
    public DocumentTypeBuilder<T> MapComputedProperty<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TValue>(
        Expression<Func<T, TValue>> property,
        Expression<Func<T, TValue>> definition,
        bool indexed = false)
    {
        this.Mappings.Computed.Add(ComputedMappingFactory.FromExpression(property, definition, indexed));
        return this;
    }

    /// <summary>
    /// AOT-clean overload taking the property name and an explicit setter, so the read-back path uses no
    /// reflection.
    /// </summary>
    public DocumentTypeBuilder<T> MapComputedProperty<TValue>(
        string propertyName,
        Expression<Func<T, TValue>> definition,
        Action<T, TValue> setter,
        bool indexed = false)
    {
        this.Mappings.Computed.Add(ComputedMappingFactory.FromExpression(propertyName, definition, setter, indexed));
        return this;
    }

    // ── Blobs ───────────────────────────────────────────────────────────

    /// <summary>
    /// Declares a <see cref="DocumentBlob"/> property whose payload lives in the blob sidecar table rather
    /// than the document body. Reads return the metadata with the payload unloaded; fetch it with
    /// <c>LoadBlobs</c>.
    /// </summary>
    public DocumentTypeBuilder<T> MapBlob(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Expression<Func<T, DocumentBlob?>> property,
        Action<BlobOptions>? configure = null)
    {
        this.Mappings.MapBlob(property, configure);
        return this;
    }

    /// <summary>AOT-clean overload taking explicit accessors.</summary>
    public DocumentTypeBuilder<T> MapBlob(
        string propertyName,
        Func<T, DocumentBlob?> getter,
        Action<T, DocumentBlob?> setter,
        Action<BlobOptions>? configure = null)
    {
        this.Mappings.MapBlob(propertyName, getter, setter, configure);
        return this;
    }

    /// <summary>
    /// Declares a property holding several <see cref="DocumentBlob"/> payloads. Each item carries its own
    /// sidecar key, so reordering or inserting into the list does not re-point existing rows.
    /// </summary>
    public DocumentTypeBuilder<T> MapBlobCollection(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Expression<Func<T, DocumentBlobCollection?>> property,
        Action<BlobOptions>? configure = null)
    {
        this.Mappings.MapBlobCollection(property, configure);
        return this;
    }

    /// <summary>AOT-clean overload taking explicit accessors.</summary>
    public DocumentTypeBuilder<T> MapBlobCollection(
        string propertyName,
        Func<T, DocumentBlobCollection?> getter,
        Action<T, DocumentBlobCollection?> setter,
        Action<BlobOptions>? configure = null)
    {
        this.Mappings.MapBlobCollection(propertyName, getter, setter, configure);
        return this;
    }

    // ── Temporal ────────────────────────────────────────────────────────

    /// <summary>
    /// Enables append-only system-time history: every write records a versioned snapshot in a
    /// <c>{table}_history</c> sidecar, readable through <c>History</c>, <c>AsOf</c>, <c>Restore</c> and
    /// <c>GetDiffBetween</c>. Opt-in per type — only mapped types pay for the extra write.
    /// </summary>
    public DocumentTypeBuilder<T> MapTemporal(Action<TemporalOptions>? configure = null)
    {
        this.Mappings.MapTemporal<T>(configure);
        return this;
    }

    // ── Write hooks ─────────────────────────────────────────────────────

    /// <summary>Registers a before-write callback scoped to documents of this type.</summary>
    public DocumentTypeBuilder<T> OnBeforeWrite(Func<DocumentWriteContext, CancellationToken, Task> handler)
    {
        this.Mappings.Interceptors.AddBefore<T>(handler);
        return this;
    }

    /// <summary>Registers an after-write callback scoped to documents of this type.</summary>
    public DocumentTypeBuilder<T> OnAfterWrite(Func<DocumentWriteContext, CancellationToken, Task> handler)
    {
        this.Mappings.Interceptors.AddAfter<T>(handler);
        return this;
    }
}
