using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Everything the JSON lane needs to address one collection of documents. This is the parameter that
/// replaced <c>Type</c> throughout the lane, and it is why one implementation serves both keyings: a name
/// supplies the first three fields and leaves the rest null, a CLR type supplies them all.
/// </summary>
/// <param name="TypeName">The value stored in — and filtered on — the row's <c>TypeName</c> column.</param>
/// <param name="TableName">The physical table.</param>
/// <param name="IdsFactory">
/// How an id is read from, written to, and generated for a document body — resolved lazily, because paths
/// that never touch a JSON body (a typed delete, say) must not fail on a document type whose id the JSON
/// lane cannot express.
/// </param>
/// <param name="DocumentType">
/// The backing CLR type, or <c>null</c> when schema-free. Everything resolved by <see cref="Type"/> —
/// interceptors, temporal history, global query filters, change notifications — is skipped when null.
/// </param>
/// <param name="TypeInfo">
/// Metadata for resolving a path string to a JSON path, or <c>null</c> when schema-free (paths are then
/// taken literally and typed by inference).
/// </param>
sealed record JsonLaneTarget(
    string TypeName,
    string TableName,
    Func<IJsonLaneIds> IdsFactory,
    Type? DocumentType,
    JsonTypeInfo? TypeInfo,
    VersionMapping? Version,
    SpatialMapping? Spatial,
    VectorMapping? Vector)
{
    IJsonLaneIds? ids;

    /// <summary>The id binding, resolved on first use.</summary>
    public IJsonLaneIds Ids => this.ids ??= this.IdsFactory();

    /// <summary>True when this collection carries registered mappings and rides the full write pipeline.</summary>
    public bool IsTyped => this.DocumentType != null;
}

/// <summary>
/// Id handling for a JSON body, abstracted over the two keyings: a type-keyed collection resolves its id
/// member and <see cref="IdKind"/> from the document type, while a schema-free one is told a property name
/// and treats every id as a literal string.
/// </summary>
interface IJsonLaneIds
{
    /// <summary>The JSON property the id is written to.</summary>
    string MemberName { get; }

    /// <summary>The id kind used for store-side generation (sequences). Ignored when <see cref="TryGenerateId"/> answers.</summary>
    IdKind Kind { get; }

    /// <summary>Storage-string form of the document's id, or <c>null</c> when absent or JSON-null.</summary>
    string? ReadStorageId(JsonObject doc);

    /// <summary>True when the id member is absent, JSON-null, or the default value for its kind.</summary>
    bool IsDefaultId(JsonObject doc);

    /// <summary>Converts a caller-supplied id object to the storage-string form the <c>Id</c> column uses.</summary>
    string ResolveId(object id);

    /// <summary>Writes a generated id (in storage-string form) back onto the body.</summary>
    void WriteId(JsonObject doc, string storageId);

    /// <summary>
    /// A locally-generated id, or <c>null</c> to defer to the store's sequence generator for
    /// <see cref="Kind"/>. Throws when this collection refuses to auto-generate (a declared string id).
    /// </summary>
    string? TryGenerateId(string typeName);
}
