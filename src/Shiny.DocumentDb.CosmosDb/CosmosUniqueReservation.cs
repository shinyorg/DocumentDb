using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.CosmosDb;

/// <summary>
/// One claimed unique-index key, stored beside the documents in their container and logical partition so it can be
/// claimed in the same transactional batch as the document write. It carries no <c>data</c>, which is how every
/// document query excludes it (<see cref="CosmosDbDocumentStore.DocumentsOnly"/>).
/// </summary>
internal sealed class CosmosUniqueReservation
{
    const string IdPrefix = "__unique__";

    /// <summary><c>"__unique__{hash}"</c> — the hash covers the type name, the index name and the key values.</summary>
    [JsonPropertyName("id")]
    public string Id { get; set; } = null!;

    /// <summary>The partition key — the owning document's type name.</summary>
    [JsonPropertyName("typeName")]
    public string TypeName { get; set; } = null!;

    /// <summary>The index the key belongs to; its presence marks the item as a reservation.</summary>
    [JsonPropertyName("uniqueIndex")]
    public string UniqueIndex { get; set; } = null!;

    /// <summary>The id of the document holding the key.</summary>
    [JsonPropertyName("ownerId")]
    public string OwnerId { get; set; } = null!;

    /// <summary>Read back from a query projection for If-Match deletes; never written.</summary>
    [JsonPropertyName("_etag")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    [Newtonsoft.Json.JsonProperty("_etag", NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
    public string? ETag { get; set; }

    public static string IdFor(UniqueIndexEntry entry) => IdPrefix + entry.Hash;
}
