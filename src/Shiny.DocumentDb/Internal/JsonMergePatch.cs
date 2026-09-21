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

    /// <summary>
    /// Removes an unset embedding from a merge patch. A mapped vector is a <see cref="ReadOnlyMemory{T}"/> — a
    /// non-nullable struct — so an unset one serializes as <c>[]</c> rather than <c>null</c> and sails through
    /// <see cref="StripNullsRecursive(string)"/>. The merge would then overwrite the stored embedding with an
    /// empty array, while the vector-index write (which reads that same emptiness as "not supplied") leaves the
    /// stored vector in place — body and index disagreeing about one write. Dropping it makes "absent" mean the
    /// same thing in both, exactly as a null geometry or blob already does.
    /// </summary>
    /// <param name="vectorJsonPath">The mapped vector's (dotted) JSON path, or null when the type maps none.</param>
    public static string StripUnsetVector(string json, string? vectorJsonPath)
    {
        if (string.IsNullOrEmpty(vectorJsonPath) || JsonNode.Parse(json) is not JsonObject obj)
            return json;

        var segments = vectorJsonPath.Split('.');
        JsonObject? cursor = obj;
        for (var i = 0; i < segments.Length - 1 && cursor != null; i++)
            cursor = cursor[segments[i]] as JsonObject;

        // Only an empty array means "unset". A populated one is a real embedding, and an absent member is
        // already what we want.
        if (cursor?[segments[^1]] is not JsonArray { Count: 0 })
            return json;

        cursor.Remove(segments[^1]);
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
