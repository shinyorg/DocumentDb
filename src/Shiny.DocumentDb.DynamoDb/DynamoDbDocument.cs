using System.Globalization;
using Amazon.DynamoDBv2.Model;

namespace Shiny.DocumentDb.DynamoDb;

/// <summary>
/// Attribute-map marshalling for the DynamoDB storage envelope: <c>pk</c> (partition/typeName),
/// <c>sk</c> (sort/id), <c>Data</c> (serialized JSON body), <c>CreatedAt</c>/<c>UpdatedAt</c> (ISO-8601),
/// and a top-level numeric <c>Version</c> when optimistic concurrency is mapped.
/// </summary>
internal static class DynamoDbDocument
{
    public const string Pk = "pk";
    public const string Sk = "sk";
    public const string DataAttr = "Data";
    public const string CreatedAtAttr = "CreatedAt";
    public const string UpdatedAtAttr = "UpdatedAt";
    public const string VersionAttr = "Version";

    public static Dictionary<string, AttributeValue> Build(
        string partitionKey,
        string id,
        string json,
        string createdAt,
        string updatedAt,
        int? version)
    {
        var item = new Dictionary<string, AttributeValue>
        {
            [Pk] = new() { S = partitionKey },
            [Sk] = new() { S = id },
            [DataAttr] = new() { S = json },
            [CreatedAtAttr] = new() { S = createdAt },
            [UpdatedAtAttr] = new() { S = updatedAt }
        };
        if (version.HasValue)
            item[VersionAttr] = new AttributeValue { N = version.Value.ToString(CultureInfo.InvariantCulture) };
        return item;
    }

    public static Dictionary<string, AttributeValue> Key(string partitionKey, string id) => new()
    {
        [Pk] = new AttributeValue { S = partitionKey },
        [Sk] = new AttributeValue { S = id }
    };

    public static string GetData(Dictionary<string, AttributeValue> item) => item[DataAttr].S;

    public static string GetCreatedAt(Dictionary<string, AttributeValue> item)
        => item.TryGetValue(CreatedAtAttr, out var v) ? v.S : item[UpdatedAtAttr].S;

    public static int GetVersion(Dictionary<string, AttributeValue> item)
        => item.TryGetValue(VersionAttr, out var v) && v.N != null
            ? int.Parse(v.N, CultureInfo.InvariantCulture)
            : 0;
}
