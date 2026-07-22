using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.CosmosDb;

// One blob payload in the "{container}_blobs" sidecar container, partitioned by /typeName like the main
// container. id = "{docId}__{blobKey}"; the payload is base64 (a Cosmos item is JSON, 2 MB max).
internal class CosmosBlobDocument
{
    [JsonPropertyName("id")] public string Id { get; set; } = null!;
    [JsonPropertyName("typeName")] public string TypeName { get; set; } = null!;
    [JsonPropertyName("docId")] public string DocId { get; set; } = null!;
    [JsonPropertyName("blobKey")] public string BlobKey { get; set; } = null!;
    [JsonPropertyName("data")] public string Data { get; set; } = null!;
    [JsonPropertyName("length")] public long Length { get; set; }
    [JsonPropertyName("contentType")] public string? ContentType { get; set; }
    [JsonPropertyName("fileName")] public string? FileName { get; set; }
    [JsonPropertyName("hash")] public string? Hash { get; set; }
    [JsonPropertyName("createdAt")] public string CreatedAt { get; set; } = null!;
    [JsonPropertyName("updatedAt")] public string UpdatedAt { get; set; } = null!;
}
