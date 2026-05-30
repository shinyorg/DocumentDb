using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.Internal;

static class JsonMergePatch
{
    public static string Merge(string targetJson, string patchJson)
    {
        var target = JsonNode.Parse(targetJson)?.AsObject();
        var patch = JsonNode.Parse(patchJson)?.AsObject();

        if (target == null || patch == null)
            return patchJson;

        ApplyPatch(target, patch);
        return target.ToJsonString();
    }

    static void ApplyPatch(JsonObject target, JsonObject patch)
    {
        foreach (var prop in patch)
        {
            if (prop.Value is JsonObject patchObj && target[prop.Key] is JsonObject targetObj)
            {
                ApplyPatch(targetObj, patchObj);
            }
            else
            {
                target[prop.Key] = prop.Value?.DeepClone();
            }
        }
    }

    // Recursively drop null-valued properties from a patch payload BEFORE the merge.
    //
    // Without this, RFC 7396-compliant deep-merge providers (SQLite json_patch, MySQL
    // JSON_MERGE_PATCH, and our own JsonMergePatch.Merge) would interpret a null as
    // "delete this field" and silently wipe nested defaults the caller did not intend
    // to clear. For example, a patch like `new Doc { Inner = new Inner { City = "X" } }`
    // serializes to `{inner: {city:"X", street:null, state:null}}`, and a shallow strip
    // would leave the inner nulls intact — deleting Street/State during merge.
    public static string StripNullsRecursive(string json)
    {
        var node = JsonNode.Parse(json);
        if (node is not JsonObject obj)
            return json;

        StripNullsRecursive(obj);
        return obj.ToJsonString();
    }

    static void StripNullsRecursive(JsonObject obj)
    {
        foreach (var key in obj.Where(kv => kv.Value is null).Select(kv => kv.Key).ToList())
            obj.Remove(key);

        foreach (var kv in obj)
            if (kv.Value is JsonObject child)
                StripNullsRecursive(child);
    }
}
