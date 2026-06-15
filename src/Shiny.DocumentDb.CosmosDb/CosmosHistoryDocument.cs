using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.CosmosDb;

/// <summary>
/// Internal temporal-history envelope stored in the <c>{container}_history</c> sidecar container
/// (partitioned by <c>/typeName</c>, mirroring the main container). One item per document version.
/// </summary>
internal class CosmosHistoryDocument
{
    /// <summary>Cosmos item id, unique within the partition: <c>"{documentId}__v{version}"</c>.</summary>
    [JsonPropertyName("id")]
    public string Id { get; set; } = null!;

    [JsonPropertyName("documentId")]
    public string DocumentId { get; set; } = null!;

    [JsonPropertyName("typeName")]
    public string TypeName { get; set; } = null!;

    [JsonPropertyName("version")]
    public long Version { get; set; }

    [JsonPropertyName("validFrom")]
    public string ValidFrom { get; set; } = null!;

    [JsonPropertyName("validTo")]
    public string? ValidTo { get; set; }

    [JsonPropertyName("operation")]
    public string Operation { get; set; } = null!;

    [JsonPropertyName("actor")]
    public string? Actor { get; set; }

    /// <summary>The post-image JSON, or null for a <see cref="TemporalOperation.Removed"/> tombstone.</summary>
    [JsonPropertyName("data")]
    [Newtonsoft.Json.JsonConverter(typeof(RawJsonConverter))]
    public string? Data { get; set; }
}
