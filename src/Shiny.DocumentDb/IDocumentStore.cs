using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

public interface IDocumentStore
{
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
    /// When called inside <see cref="RunInTransaction"/>, uses the existing transaction (no nested transaction).
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
    /// Executes multiple operations within a single database transaction.
    /// </summary>
    Task RunInTransaction(Func<IDocumentStore, Task> operation, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a backup of the database to the specified file path.
    /// Not all providers support this operation.
    /// </summary>
    /// <param name="destinationPath">The file path where the backup should be written.</param>
    Task Backup(string destinationPath, CancellationToken cancellationToken = default);

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
}
