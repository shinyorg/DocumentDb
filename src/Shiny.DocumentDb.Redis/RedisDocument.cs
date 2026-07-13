using System.Globalization;
using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.Redis;

/// <summary>
/// The RedisJSON storage envelope for a document. Each document lives at key
/// <c>doc:{typeName}:{id}</c> as a single JSON value:
/// <c>{ id, typeName, data:{…poco…}, createdAt, updatedAt, version? }</c>. The POCO body is nested under
/// <c>data</c> so a per-type RediSearch index can declare schema paths like <c>$.data.status</c> while the
/// bookkeeping fields stay out of the queryable surface. Mirrors the Id/TypeName/Data/CreatedAt/UpdatedAt
/// envelope the SQL and MongoDB providers use so behaviour stays consistent.
/// </summary>
internal static class RedisDocument
{
    public const string Id = "id";
    public const string TypeName = "typeName";
    public const string Data = "data";
    public const string CreatedAt = "createdAt";
    public const string UpdatedAt = "updatedAt";
    public const string Version = "version";

    public static string BuildEnvelope(
        string id,
        string typeName,
        string dataJson,
        DateTime now,
        int? version,
        DateTime? createdAt = null,
        DateTime? updatedAt = null)
    {
        var obj = new JsonObject
        {
            [Id] = id,
            [TypeName] = typeName,
            [Data] = JsonNode.Parse(dataJson),
            [CreatedAt] = (createdAt ?? now).ToString("o", CultureInfo.InvariantCulture),
            [UpdatedAt] = (updatedAt ?? now).ToString("o", CultureInfo.InvariantCulture)
        };
        if (version.HasValue)
            obj[Version] = version.Value;
        return obj.ToJsonString();
    }

    /// <summary>Parses a stored envelope; null when the JSON is missing or malformed. Tolerates the
    /// single-element array form some RedisJSON versions return for a root <c>JSON.GET</c>.</summary>
    public static JsonObject? ParseEnvelope(string? json)
    {
        if (string.IsNullOrEmpty(json))
            return null;
        var node = JsonNode.Parse(json);
        if (node is JsonArray arr)
            node = arr.Count > 0 ? arr[0] : null;
        return node as JsonObject;
    }

    /// <summary>Extracts the raw POCO body JSON from a parsed envelope.</summary>
    public static string? GetDataJson(JsonObject envelope)
        => envelope[Data]?.ToJsonString();

    public static int GetVersion(JsonObject envelope)
        => envelope[Version] is { } v && v.GetValueKind() == System.Text.Json.JsonValueKind.Number
            ? v.GetValue<int>()
            : 0;

    public static DateTime? GetCreatedAt(JsonObject envelope)
        => envelope[CreatedAt] is { } v && DateTime.TryParse(v.GetValue<string>(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dt)
            ? dt
            : null;

    public static DateTime? GetUpdatedAt(JsonObject envelope)
        => envelope[UpdatedAt] is { } v && DateTime.TryParse(v.GetValue<string>(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dt)
            ? dt
            : null;
}

/// <summary>The RediSearch schema role a mapped property plays in a type's index.</summary>
internal enum RedisFieldKind
{
    Tag,
    Numeric,
    Text,
    Geo,
    Vector
}

/// <summary>
/// A single field in a type's RediSearch index. <see cref="JsonPath"/> is the dotted body path
/// (e.g. <c>address.city</c>); <see cref="SchemaPath"/> is the <c>$.data.…</c> JSON path RediSearch indexes;
/// <see cref="Alias"/> is the <c>@alias</c> used in query strings (dots replaced with underscores).
/// </summary>
internal sealed class RedisIndexField
{
    public required string JsonPath { get; init; }
    public required string ClrPath { get; init; }
    public required RedisFieldKind Kind { get; init; }
    public string SchemaPath => $"$.{RedisDocument.Data}.{this.JsonPath}";
    public string Alias => this.JsonPath.Replace('.', '_');
}
