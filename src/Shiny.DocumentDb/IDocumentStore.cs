using System.Linq.Expressions;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

public interface IDocumentStore
{
    // ── Late-bound JSON lane (Type + JsonNode) ──────────────────────────
    // Supported on the relational providers (SQLite, SQLCipher, MySQL, SQL Server, PostgreSQL, Oracle,
    // DuckDB) via the core store. Document-native providers (MongoDB, Cosmos, LiteDB, IndexedDB) and the
    // key-partitioned providers (Azure Table, DynamoDB) throw until a later cut. The methods default to
    // NotSupportedException so a provider opts in by overriding, mirroring the spatial/vector/full-text APIs.

    /// <summary>
    /// Writes one or many documents of <paramref name="type"/> from a caller-supplied JSON body, without a
    /// CLR instance. The body is stored AS-IS — property names must match the type's serialized shape. A
    /// <see cref="JsonArray"/> writes every element as a document of <paramref name="type"/> atomically (one
    /// transaction); a <see cref="JsonObject"/> writes a single document; a primitive <see cref="JsonValue"/>
    /// throws <see cref="ArgumentException"/>. Rides the normal write pipeline — tenancy, temporal history,
    /// versioning/CAS, spatial/vector sidecars, JSON interceptors, and change notifications all apply. Every
    /// registered spatial/vector mapping's JSON path must be present (an absent path throws
    /// <see cref="InvalidOperationException"/>; a JSON null is a deliberate "no value"). Returns the number of
    /// documents written.
    /// </summary>
    /// <param name="type">A registered document type. Resolves table/typeName and all mappings.</param>
    /// <param name="document">A <see cref="JsonObject"/> (one doc) or <see cref="JsonArray"/> (many).</param>
    Task<int> Insert(Type type, JsonNode document, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("The late-bound JSON lane (Type + JsonNode) is not supported by this provider.");

    /// <summary>Full-document replace by <paramref name="type"/> + JSON. Every target must already exist
    /// (missing → throws and rolls back the whole call). Same object/array + pipeline semantics as
    /// <see cref="Insert(Type, JsonNode, CancellationToken)"/>.</summary>
    Task<int> Update(Type type, JsonNode document, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("The late-bound JSON lane (Type + JsonNode) is not supported by this provider.");

    /// <summary>Update by <paramref name="type"/> + JSON. When <paramref name="patch"/> is false the body is
    /// replaced wholesale; when true it is RFC 7396 deep-merged into the stored document (so a partial
    /// <c>JsonObject</c> updates only the keys it carries). Every target must already exist.</summary>
    Task<int> Update(Type type, JsonNode document, bool patch, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("The late-bound JSON lane (Type + JsonNode) is not supported by this provider.");

    /// <summary>RFC 7396 JSON Merge Patch upsert by <paramref name="type"/> + JSON (deep-merge if the Id
    /// exists, insert-as-is otherwise). The mapped-property presence check applies only to elements with no
    /// Id (a guaranteed insert). Same object/array + pipeline semantics as
    /// <see cref="Insert(Type, JsonNode, CancellationToken)"/>.</summary>
    Task<int> Upsert(Type type, JsonNode document, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("The late-bound JSON lane (Type + JsonNode) is not supported by this provider.");

    /// <summary>Upsert by <paramref name="type"/> + JSON. When <paramref name="patchIfUpdate"/> is true
    /// (default form) an existing document is RFC 7396 deep-merged; when false its body is replaced wholesale.
    /// Insert-when-absent is identical either way.</summary>
    Task<int> Upsert(Type type, JsonNode document, bool patchIfUpdate, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("The late-bound JSON lane (Type + JsonNode) is not supported by this provider.");

    /// <summary>Reads a single document of <paramref name="type"/> by Id as raw JSON, or null if not found.
    /// Honors tenancy and global query filters exactly like the typed <c>Get&lt;T&gt;</c>.</summary>
    Task<JsonNode?> Get(Type type, object id, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("The late-bound JSON lane (Type + JsonNode) is not supported by this provider.");

    /// <summary>Runs the same string WHERE filter surface as <c>Query&lt;T&gt;(string, …)</c> but returns each
    /// matching document as a <see cref="JsonNode"/> (no deserialize to <c>T</c>). Tenancy applies.</summary>
    Task<IReadOnlyList<JsonNode>> Query(Type type, string whereClause, object? parameters = null, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("The late-bound JSON lane (Type + JsonNode) is not supported by this provider.");

    /// <summary>Streaming form of <see cref="Query(Type, string, object?, CancellationToken)"/>.</summary>
    IAsyncEnumerable<JsonNode> QueryStream(Type type, string whereClause, object? parameters = null, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("The late-bound JSON lane (Type + JsonNode) is not supported by this provider.");

    /// <summary>
    /// Returns a fluent query builder for the specified type.
    /// </summary>
    /// <param name="jsonTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class;

    /// <summary>
    /// Inserts a new document. The document must have a public Id property (Guid, int, long, or string).
    /// For Guid, int, and long, if the Id is the default value it will be auto-generated.
    /// For string, a default (null/empty) Id will throw <see cref="InvalidOperationException"/>.
    /// If a document with the same Id already exists, throws <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <param name="document">The document to insert.</param>
    /// <param name="jsonTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Inserts multiple documents in a single transaction with command reuse for optimal performance.
    /// Auto-generates IDs for Guid, int, and long Id types. String Ids must be pre-set on every document.
    /// If any document fails (e.g. duplicate Id), the entire batch is rolled back.
    /// </summary>
    /// <param name="documents">The documents to insert.</param>
    /// <param name="jsonTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    /// <returns>The number of documents inserted.</returns>
    Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Updates an existing document by replacing it entirely. The document must have a non-default Id.
    /// If the document is not found, throws <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <param name="document">The document to update. Must have a non-default Id.</param>
    /// <param name="jsonTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Upserts a document using RFC 7396 JSON Merge Patch. The document must have a non-default Id.
    /// If the document exists, the patch is deep-merged into the existing JSON; if it doesn't exist, the patch is inserted as-is.
    /// </summary>
    /// <param name="patch">The patch document to merge. Must have a non-default Id.</param>
    /// <param name="jsonTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Updates an existing document (must have a non-default Id; throws if not found). When
    /// <paramref name="patch"/> is <c>false</c> the stored body is replaced wholesale (same as
    /// <see cref="Update{T}(T, JsonTypeInfo{T}, CancellationToken)"/>); when <c>true</c> the document is
    /// RFC 7396 deep-merged into the stored one (null properties on the incoming document are ignored, so only
    /// the present, non-null fields change). For a typed object, a genuine partial update needs the type to
    /// omit unset fields (<c>JsonIgnoreCondition.WhenWritingNull/Default</c>) or the late-bound JSON lane.
    /// </summary>
    /// <remarks>Merge (<paramref name="patch"/> = true) is implemented by the relational providers; other
    /// stores throw <see cref="NotSupportedException"/> for it. Replace (patch = false) works everywhere.</remarks>
    Task Update<T>(T document, bool patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => patch
            ? throw new NotSupportedException("Update(patch: true) — RFC 7396 merge on update — is not supported by this provider. Use a relational provider, or Update with patch: false (wholesale replace).")
            : this.Update(document, jsonTypeInfo, cancellationToken);

    /// <summary>
    /// Inserts the document, or if it already exists, updates it. When <paramref name="patchIfUpdate"/> is
    /// <c>true</c> (the default behaviour of <see cref="Upsert{T}(T, JsonTypeInfo{T}, CancellationToken)"/>)
    /// the existing document is RFC 7396 deep-merged; when <c>false</c> the existing body is replaced
    /// wholesale. Insert-when-absent is identical either way. Must have a non-default Id.
    /// </summary>
    /// <remarks>Wholesale replace-on-update (<paramref name="patchIfUpdate"/> = false) is implemented by the
    /// relational providers; other stores throw <see cref="NotSupportedException"/> for it. The merge default
    /// (patchIfUpdate = true) works everywhere.</remarks>
    Task Upsert<T>(T patch, bool patchIfUpdate, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
        => patchIfUpdate
            ? this.Upsert(patch, jsonTypeInfo, cancellationToken)
            : throw new NotSupportedException("Upsert(patchIfUpdate: false) — wholesale replace on update — is not supported by this provider. Use a relational provider, or Upsert with patchIfUpdate: true (RFC 7396 merge).");

    /// <summary>
    /// Updates a single property on an existing document using json_set.
    /// </summary>
    /// <param name="id">The document ID (Guid, int, long, or string). Throws <see cref="ArgumentException"/> for unsupported types.</param>
    /// <param name="property">Expression selecting the property to set.</param>
    /// <param name="value">The new value.</param>
    /// <param name="jsonTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    /// <returns>True if a document was updated, false if not found.</returns>
    Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Removes a single property from an existing document using json_remove.
    /// </summary>
    /// <param name="id">The document ID (Guid, int, long, or string). Throws <see cref="ArgumentException"/> for unsupported types.</param>
    /// <param name="property">Expression selecting the property to remove.</param>
    /// <param name="jsonTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    /// <returns>True if a document was updated, false if not found.</returns>
    Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Gets a document by ID.
    /// </summary>
    /// <param name="id">The document ID (Guid, int, long, or string). Throws <see cref="ArgumentException"/> for unsupported types.</param>
    /// <param name="jsonTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Compares a modified object against the stored document with the same ID and returns
    /// a <see cref="JsonPatchDocument{T}"/> describing the differences (RFC 6902).
    /// Returns <c>null</c> if no document with the specified ID exists.
    /// </summary>
    /// <param name="id">The document ID (Guid, int, long, or string).</param>
    /// <param name="modified">The modified object to compare against the stored document.</param>
    /// <param name="jsonTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Queries documents using a SQL WHERE clause fragment.
    /// </summary>
    /// <param name="whereClause">SQL WHERE clause, e.g. provider-specific JSON extract syntax</param>
    /// <param name="jsonTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    /// <param name="parameters">Anonymous object or dictionary of parameter values.</param>
    Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Streams documents matching a SQL WHERE clause fragment one-at-a-time.
    /// </summary>
    /// <param name="whereClause">SQL WHERE clause, e.g. provider-specific JSON extract syntax</param>
    /// <param name="jsonTypeInfo">Optional type metadata for AOT-safe serialization. When null, resolved from <see cref="DocumentStoreOptions.JsonSerializerOptions"/> or via reflection.</param>
    /// <param name="parameters">Anonymous object or dictionary of parameter values.</param>
    IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Counts documents of the specified type, with an optional WHERE filter.
    /// </summary>
    Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Removes a document by ID.
    /// </summary>
    /// <param name="id">The document ID (Guid, int, long, or string). Throws <see cref="ArgumentException"/> for unsupported types.</param>
    /// <returns>True if a document was deleted.</returns>
    Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Removes all documents of the specified type.
    /// </summary>
    /// <returns>The number of documents deleted.</returns>
    Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Upserts multiple documents (RFC 7396 JSON Merge Patch) as a single batch. Every patch must have a
    /// non-default Id. The shipped providers apply the batch atomically — a multi-row statement
    /// (SQLite/DuckDB) or bulk write (MongoDB/Cosmos) where supported, otherwise a per-document loop in
    /// one transaction. For versioned types the batch is all-or-nothing: the first version conflict
    /// throws <see cref="ConcurrencyException"/> and the whole batch is rolled back.
    /// </summary>
    /// <returns>The number of documents upserted.</returns>
    /// <remarks>The default interface implementation loops <see cref="Upsert"/> without a shared
    /// transaction; every shipped store overrides it.</remarks>
    async Task<int> BatchUpsert<T>(IEnumerable<T> patches, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var count = 0;
        foreach (var patch in patches)
        {
            await this.Upsert(patch, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            count++;
        }
        return count;
    }

    /// <summary>
    /// Updates multiple existing documents (full-document replace) as a single batch. Every document must
    /// have a non-default Id and must already exist; a missing document throws
    /// <see cref="InvalidOperationException"/> and rolls the whole batch back. For versioned types the
    /// first version conflict throws <see cref="ConcurrencyException"/> (all-or-nothing).
    /// </summary>
    /// <returns>The number of documents updated.</returns>
    /// <remarks>The default interface implementation loops <see cref="Update"/> without a shared
    /// transaction; every shipped store overrides it.</remarks>
    async Task<int> BatchUpdate<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class
    {
        var count = 0;
        foreach (var document in documents)
        {
            await this.Update(document, jsonTypeInfo, cancellationToken).ConfigureAwait(false);
            count++;
        }
        return count;
    }

    /// <summary>
    /// Removes multiple documents by Id as a single batch. The shipped providers apply the batch
    /// atomically — a single delete-by-id-list statement (relational) or bulk delete (MongoDB/Cosmos)
    /// where eligible, otherwise a per-id loop in one transaction. Ids that do not exist are ignored.
    /// </summary>
    /// <param name="ids">The document Ids (Guid, int, long, or string).</param>
    /// <returns>The number of documents actually deleted.</returns>
    /// <remarks>The default interface implementation loops <see cref="Remove"/> without a shared
    /// transaction; every shipped store overrides it.</remarks>
    async Task<int> BatchRemove<T>(IEnumerable<object> ids, CancellationToken cancellationToken = default) where T : class
    {
        var count = 0;
        foreach (var id in ids)
        {
            if (await this.Remove<T>(id, cancellationToken).ConfigureAwait(false))
                count++;
        }
        return count;
    }

    /// <summary>
    /// Creates a <see cref="UnitOfWork"/> that buffers Add/AddRange/Update/Upsert/Remove operations and
    /// applies them atomically in a single transaction when <see cref="UnitOfWork.SaveChanges"/> is called.
    /// This is the only way to group writes into one transaction.
    /// </summary>
    UnitOfWork CreateUnitOfWork();

    /// <summary>
    /// Suppresses every interceptor (per-document and bulk) for the duration of the returned scope, on the
    /// current async flow. Use it to wrap a re-entrant side-effect write made through
    /// <see cref="DocumentWriteContext.Store"/> from inside an interceptor (e.g. an outbox insert) so it does
    /// not recurse back into the interceptor pipeline. Bounded by the caller's async flow — dispose it before
    /// the hook returns.
    /// </summary>
    IDisposable SuppressInterceptors() => DocumentOperationScope.SuppressInterceptors();

    /// <summary>
    /// Returns true if this store supports spatial queries.
    /// </summary>
    bool SupportsSpatial => false;

    /// <summary>
    /// Queries documents within a radius (meters) of a center point, ordered by distance ascending.
    /// </summary>
    /// <param name="center">The center point to search from.</param>
    /// <param name="radiusMeters">Maximum distance in meters.</param>
    /// <param name="filter">Optional additional predicate filter.</param>
    Task<IReadOnlyList<SpatialResult<T>>> WithinRadius<T>(
        GeoPoint center,
        double radiusMeters,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>
    /// Queries documents within a geographic bounding box.
    /// </summary>
    /// <param name="box">The bounding box to search within.</param>
    /// <param name="filter">Optional additional predicate filter.</param>
    Task<IReadOnlyList<T>> WithinBoundingBox<T>(
        GeoBoundingBox box,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>
    /// Returns the nearest documents to a center point, ordered by distance ascending.
    /// </summary>
    /// <param name="center">The center point to search from.</param>
    /// <param name="count">Maximum number of results to return.</param>
    /// <param name="filter">Optional additional predicate filter.</param>
    Task<IReadOnlyList<SpatialResult<T>>> NearestNeighbors<T>(
        GeoPoint center,
        int count,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    // ── Full-geometry topological predicates ──────────────────────────────
    // Each reads "stored-geometry &lt;predicate&gt; query-geometry". When orderByDistanceFrom is supplied,
    // results are ordered by ascending distance from it and SpatialResult.DistanceMeters is populated;
    // otherwise DistanceMeters is 0 and order is unspecified. Requires MapSpatialProperty at startup.

    /// <summary>Documents whose geometry intersects <paramref name="geometry"/>.</summary>
    Task<IReadOnlyList<SpatialResult<T>>> GeoIntersects<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>Documents whose geometry is contained by <paramref name="container"/> (OGC within).</summary>
    Task<IReadOnlyList<SpatialResult<T>>> GeoContainedBy<T>(Geometry container, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>Documents whose geometry contains <paramref name="geometry"/>.</summary>
    Task<IReadOnlyList<SpatialResult<T>>> GeoContains<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>Documents whose geometry is disjoint from <paramref name="geometry"/>. Not index-accelerated (O(n)).</summary>
    Task<IReadOnlyList<SpatialResult<T>>> GeoDisjoint<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>Documents whose geometry touches <paramref name="geometry"/> (boundaries meet, interiors do not).</summary>
    Task<IReadOnlyList<SpatialResult<T>>> GeoTouches<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>Documents whose geometry crosses <paramref name="geometry"/>.</summary>
    Task<IReadOnlyList<SpatialResult<T>>> GeoCrosses<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>Documents whose geometry overlaps <paramref name="geometry"/> (same dimension, partial overlap).</summary>
    Task<IReadOnlyList<SpatialResult<T>>> GeoOverlaps<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>Documents whose geometry equals <paramref name="geometry"/>.</summary>
    Task<IReadOnlyList<SpatialResult<T>>> GeoEquals<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>Documents whose geometry covers <paramref name="geometry"/> (boundary-inclusive contains).</summary>
    Task<IReadOnlyList<SpatialResult<T>>> GeoCovers<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>Documents whose geometry is covered by <paramref name="geometry"/>.</summary>
    Task<IReadOnlyList<SpatialResult<T>>> GeoCoveredBy<T>(Geometry geometry, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>Documents whose geometry lies within <paramref name="meters"/> of <paramref name="geometry"/>.</summary>
    Task<IReadOnlyList<SpatialResult<T>>> GeoWithinDistance<T>(Geometry geometry, double meters, Geometry? orderByDistanceFrom = null, Expression<Func<T, bool>>? filter = null, CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Spatial queries are not supported by this provider.");

    /// <summary>
    /// Returns true if this store supports ANN vector search.
    /// </summary>
    bool SupportsVector => false;

    /// <summary>
    /// Returns the documents whose mapped embedding is nearest to <paramref name="query"/>,
    /// ordered by score ascending for distance metrics (Cosine/Euclidean) and descending for
    /// similarity metrics (DotProduct). Requires <c>MapVectorProperty&lt;T&gt;</c> at startup.
    /// </summary>
    /// <param name="query">The query embedding.</param>
    /// <param name="k">Maximum number of results to return.</param>
    /// <param name="filter">
    /// Optional predicate. Where supported (Cosmos, pgvector, SQL Server, Atlas, DuckDB) it is
    /// applied as a pre-filter inside the ANN search. SQLite post-filters after the candidate
    /// scan; see the design doc for the candidate-multiplier heuristic.
    /// </param>
    Task<IReadOnlyList<VectorResult<T>>> NearestVectors<T>(
        ReadOnlyMemory<float> query,
        int k,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Vector queries are not supported by this provider.");

    /// <summary>
    /// Returns true if this store supports full-text search.
    /// </summary>
    bool SupportsFullText => false;

    /// <summary>
    /// Runs a relevance-ranked full-text search over the property/properties registered with
    /// <c>MapFullTextProperty&lt;T&gt;</c>, returning up to <paramref name="maxResults"/> documents
    /// ordered by relevance descending. <paramref name="searchText"/> is a natural-language query
    /// (terms are OR-combined); the exact operator grammar is provider-specific.
    /// </summary>
    /// <param name="searchText">The text to search for.</param>
    /// <param name="maxResults">Maximum number of results to return, ordered by score descending.</param>
    /// <param name="filter">
    /// Optional predicate applied as a pre-filter alongside the full-text match (e.g. tenant/category
    /// scoping). Pushed into the query where the provider supports it.
    /// </param>
    Task<IReadOnlyList<FullTextResult<T>>> FullTextSearch<T>(
        string searchText,
        int maxResults = 50,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default) where T : class
        => throw new NotSupportedException("Full-text queries are not supported by this provider.");
}
