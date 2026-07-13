using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Google.Cloud.Firestore;

namespace Shiny.DocumentDb.Firestore;

/// <summary>
/// Converts between the document's JSON body and a Firestore native map (<c>Dictionary&lt;string, object&gt;</c>)
/// so Firestore auto-indexes every field and <c>Where</c>/<c>OrderBy</c> push down. Bookkeeping fields live
/// under a single reserved <c>_meta</c> sub-map (kept out of the user's field namespace and stripped on read).
/// </summary>
internal static class FirestoreDocument
{
    /// <summary>Reserved top-level field carrying the bookkeeping sub-map (typeName, createdAt, updatedAt).</summary>
    public const string MetaField = "_meta";
    public const string MetaTypeName = "typeName";
    public const string MetaCreatedAt = "createdAt";
    public const string MetaUpdatedAt = "updatedAt";

    /// <summary>Builds the Firestore native map from a serialized JSON body plus the bookkeeping metadata.</summary>
    public static Dictionary<string, object?> BuildMap(string json, string typeName, DateTime createdAtUtc, DateTime updatedAtUtc)
    {
        var map = JsonToMap(json);
        map[MetaField] = new Dictionary<string, object?>
        {
            [MetaTypeName] = typeName,
            [MetaCreatedAt] = createdAtUtc.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture),
            [MetaUpdatedAt] = updatedAtUtc.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture)
        };
        return map;
    }

    /// <summary>Reads a stored snapshot's native map back into the document JSON body (reserved fields stripped).</summary>
    public static string MapToJson(IDictionary<string, object> map)
    {
        var obj = new JsonObject();
        foreach (var kv in map)
        {
            if (kv.Key == MetaField)
                continue;
            obj[kv.Key] = ValueToNode(kv.Value);
        }
        return obj.ToJsonString();
    }

    /// <summary>Reads the bookkeeping <c>createdAt</c>/<c>updatedAt</c> from a stored map (null when absent).</summary>
    public static (DateTimeOffset? CreatedAt, DateTimeOffset? UpdatedAt) ReadTimestamps(IDictionary<string, object> map)
    {
        if (!map.TryGetValue(MetaField, out var metaObj) || metaObj is not IDictionary<string, object> meta)
            return (null, null);
        return (ParseTimestamp(meta, MetaCreatedAt), ParseTimestamp(meta, MetaUpdatedAt));
    }

    static DateTimeOffset? ParseTimestamp(IDictionary<string, object> meta, string key)
    {
        if (meta.TryGetValue(key, out var v) && v is string s && DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
            return dto;
        if (meta.TryGetValue(key, out var t) && t is Timestamp ts)
            return ts.ToDateTimeOffset();
        return null;
    }

    // ── JSON → Firestore map ─────────────────────────────────────────────

    static Dictionary<string, object?> JsonToMap(string json)
    {
        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            throw new InvalidOperationException("A Firestore document body must serialize to a JSON object.");
        return (Dictionary<string, object?>)ElementToValue(doc.RootElement)!;
    }

    static object? ElementToValue(JsonElement el)
    {
        switch (el.ValueKind)
        {
            case JsonValueKind.Object:
                var map = new Dictionary<string, object?>();
                foreach (var prop in el.EnumerateObject())
                    map[prop.Name] = ElementToValue(prop.Value);
                return map;

            case JsonValueKind.Array:
                var list = new List<object?>();
                foreach (var item in el.EnumerateArray())
                    list.Add(ElementToValue(item));
                return list;

            case JsonValueKind.String:
                return el.GetString();

            case JsonValueKind.Number:
                return el.TryGetInt64(out var l) ? l : el.GetDouble();

            case JsonValueKind.True:
                return true;

            case JsonValueKind.False:
                return false;

            default:
                return null;
        }
    }

    // ── Firestore map → JSON ─────────────────────────────────────────────

    static JsonNode? ValueToNode(object? value)
    {
        switch (value)
        {
            case null:
                return null;
            case bool b:
                return JsonValue.Create(b);
            case string s:
                return JsonValue.Create(s);
            case long l:
                return JsonValue.Create(l);
            case int i:
                return JsonValue.Create(i);
            case double d:
                return JsonValue.Create(d);
            case float f:
                return JsonValue.Create((double)f);
            case decimal m:
                return JsonValue.Create(m);
            case Timestamp ts:
                return JsonValue.Create(ts.ToDateTimeOffset().UtcDateTime.ToString("o", CultureInfo.InvariantCulture));
            case DateTime dt:
                return JsonValue.Create(dt.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture));
            case DateTimeOffset dto:
                return JsonValue.Create(dto.UtcDateTime.ToString("o", CultureInfo.InvariantCulture));
            case byte[] bytes:
                return JsonValue.Create(Convert.ToBase64String(bytes));
            case IDictionary<string, object> nested:
                var obj = new JsonObject();
                foreach (var kv in nested)
                    obj[kv.Key] = ValueToNode(kv.Value);
                return obj;
            case System.Collections.IEnumerable seq:
                var arr = new JsonArray();
                foreach (var item in seq)
                    arr.Add(ValueToNode(item));
                return arr;
            default:
                return JsonValue.Create(value.ToString());
        }
    }
}
