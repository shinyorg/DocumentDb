using System.Text.Json.Nodes;

namespace Shiny.DocumentDb;

/// <summary>
/// A JSON view over one collection of documents, addressed either by <b>name</b> (schema-free — no CLR type
/// anywhere) or by <b>CLR type</b> (late-bound — a registered document type, but no CLR instance). Obtain one
/// from <see cref="IDocumentStore.Collection(string, string)"/> or <see cref="IDocumentStore.Collection(Type)"/>.
/// </summary>
/// <remarks>
/// <para>
/// The two keyings are the same lane: the row is identical either way
/// (<c>Id / TypeName / Data / CreatedAt / UpdatedAt</c>), and a CLR type contributes exactly two things on
/// top of a name — the registered mappings plus the write pipeline, and metadata for resolving a path string
/// to a JSON path. Both are optional, so the difference shows up as
/// <see cref="DocumentType"/> being <c>null</c> rather than as a second API.
/// </para>
/// <para>
/// <b>Pipeline features apply when <see cref="DocumentType"/> is non-null.</b> That means tenancy,
/// transactions, paging and backup work on both; while interceptors, temporal history, global query filters,
/// change notifications, versioning/CAS and the spatial/vector/blob sidecars apply only to a type-keyed
/// collection. Bodies are always stored <b>as-is</b> — property names must match the type's serialized shape.
/// </para>
/// </remarks>
public interface IJsonDocumentCollection
{
    /// <summary>The row's <c>TypeName</c> — the supplied collection name, or the type's resolved type name.</summary>
    string Name { get; }

    /// <summary>
    /// The backing CLR type, or <c>null</c> when this collection is schema-free. Everything that resolves a
    /// registration by <see cref="Type"/> — mappings, interceptors, temporal history, global filters, the
    /// change feed — applies only when this is non-null.
    /// </summary>
    Type? DocumentType { get; }

    /// <summary>
    /// The JSON property carrying the document id. Read case-insensitively, written verbatim. Defaults to
    /// <c>"id"</c> for a schema-free collection and to the type's mapped id property for a type-keyed one.
    /// </summary>
    string IdProperty { get; }

    /// <summary>
    /// Inserts one document and returns the stored id. When the id property is absent, JSON-null or empty an
    /// id is generated and stamped into <paramref name="document"/> so the body and the <c>Id</c> column agree
    /// byte-for-byte.
    /// </summary>
    /// <remarks>
    /// Id generation is a property of the target. A <b>type-keyed</b> collection follows the type's declared
    /// id: <c>Guid</c> generates a v4, <c>int</c>/<c>long</c> take the next sequence value, and a
    /// <c>string</c> id refuses to auto-generate. A <b>schema-free</b> collection has no declared id type, so
    /// it always generates a sortable UUIDv7 in <c>"N"</c> form.
    /// </remarks>
    Task<string> Insert(JsonObject document, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts every element of <paramref name="documents"/> atomically (one transaction) and returns the
    /// number written. Every element must be a <see cref="JsonObject"/>.
    /// </summary>
    Task<int> Insert(JsonArray documents, CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates existing documents. When <paramref name="patch"/> is <c>false</c> (the default) the stored body
    /// is replaced wholesale; when <c>true</c> the supplied body is RFC 7396 deep-merged into it, so a partial
    /// object updates only the keys it carries. Accepts a <see cref="JsonObject"/> (one) or
    /// <see cref="JsonArray"/> (many, atomic). Every target must already exist.
    /// </summary>
    Task<int> Update(JsonNode document, bool patch = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Merge-or-insert. When <paramref name="patchIfUpdate"/> is <c>true</c> (the default) an existing document
    /// is RFC 7396 deep-merged; when <c>false</c> its body is replaced wholesale. Insert-when-absent is
    /// identical either way.
    /// </summary>
    Task<int> Upsert(JsonNode document, bool patchIfUpdate = true, CancellationToken cancellationToken = default);

    /// <summary>Upserts many documents in one transaction.</summary>
    Task<int> BatchUpsert(IEnumerable<JsonObject> documents, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads one document by id, or <c>null</c> when it does not exist. On a schema-free collection ids
    /// compare as <b>literal strings</b> — pass back exactly what you stored (a dashed Guid string stored as
    /// such will not be found by passing a <see cref="Guid"/>, which formats as <c>"N"</c>).
    /// </summary>
    Task<JsonObject?> Get(object id, CancellationToken cancellationToken = default);

    /// <summary>Deletes one document by id. Returns whether a row was removed.</summary>
    Task<bool> Remove(object id, CancellationToken cancellationToken = default);

    /// <summary>Deletes many documents by id in one statement. Returns the number removed.</summary>
    Task<int> BatchRemove(IEnumerable<object> ids, CancellationToken cancellationToken = default);

    /// <summary>Deletes every document in this collection. Returns the number removed.</summary>
    Task<int> Clear(CancellationToken cancellationToken = default);

    /// <summary>A fluent string-grammar query returning raw JSON.</summary>
    IJsonDocumentQuery Query();

    /// <summary>
    /// Raw provider-specific SQL <c>WHERE</c> escape hatch — the non-generic twin of
    /// <see cref="IDocumentStore.Query{T}(string, System.Text.Json.Serialization.Metadata.JsonTypeInfo{T}, object?, CancellationToken)"/>.
    /// Tenancy applies. <paramref name="whereClause"/> is your responsibility: it is not parsed, so bind
    /// values through <paramref name="parameters"/> rather than concatenating them in.
    /// </summary>
    Task<IReadOnlyList<JsonNode>> Query(string whereClause, object? parameters = null, CancellationToken cancellationToken = default);

    /// <summary>Streaming form of <see cref="Query(string, object?, CancellationToken)"/>.</summary>
    IAsyncEnumerable<JsonNode> QueryStream(string whereClause, object? parameters = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a JSON expression index over <paramref name="jsonPaths"/>, named exactly as the library names
    /// its own. Paths are validated before reaching the DDL.
    /// </summary>
    Task CreateIndex(CancellationToken cancellationToken = default, params string[] jsonPaths);
}
