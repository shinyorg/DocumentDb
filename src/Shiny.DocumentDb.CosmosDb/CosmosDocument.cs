using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.CosmosDb;

/// <summary>
/// Internal envelope document stored in CosmosDB.
/// </summary>
internal class CosmosDocument
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = null!;

    [JsonPropertyName("typeName")]
    public string TypeName { get; set; } = null!;

    // Stored as a nested JSON object (not an escaped string) so Cosmos queries can filter on c.data.* paths.
    [JsonPropertyName("data")]
    [Newtonsoft.Json.JsonConverter(typeof(RawJsonConverter))]
    public string Data { get; set; } = null!;

    [JsonPropertyName("createdAt")]
    public string CreatedAt { get; set; } = null!;

    [JsonPropertyName("updatedAt")]
    public string UpdatedAt { get; set; } = null!;
}
