using System.Text.Json;
using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.Internal;

static class JsonDiff
{
    public static JsonPatchDocument<T> CreatePatch<T>(
        string originalJson,
        string modifiedJson,
        JsonSerializerOptions options) where T : class
    {
        var original = JsonNode.Parse(originalJson)?.AsObject()
            ?? throw new InvalidOperationException("Original document JSON is not a valid object.");
        var modified = JsonNode.Parse(modifiedJson)?.AsObject()
            ?? throw new InvalidOperationException("Modified document JSON is not a valid object.");

        var operations = new List<JsonPatchOperation>();
        BuildDiff(original, modified, "", operations);
        return new JsonPatchDocument<T>(operations, options);
    }

    static void BuildDiff(
        JsonObject original,
        JsonObject modified,
        string prefix,
        List<JsonPatchOperation> operations)
    {
        foreach (var prop in modified)
        {
            var path = prefix + "/" + EscapePointer(prop.Key);
            var hadKey = original.ContainsKey(prop.Key);
            var origValue = original[prop.Key];

            if (!hadKey)
            {
                // A newly-added key — value may legitimately be JSON null (JsonObject stores a null value as a
                // C# null, so this is distinguished from "key absent" via ContainsKey, not a null check).
                operations.Add(JsonPatchOperation.Add(path, ToJsonElement(prop.Value)));
            }
            else if (origValue is null && prop.Value is not null)
            {
                // Key existed with a JSON null and now has a value.
                operations.Add(JsonPatchOperation.Add(path, ToJsonElement(prop.Value)));
            }
            else if (prop.Value is null && origValue is not null)
            {
                operations.Add(JsonPatchOperation.Replace(path, null));
            }
            else if (origValue is not null && prop.Value is not null)
            {
                if (origValue is JsonObject origObj && prop.Value is JsonObject modObj)
                {
                    BuildDiff(origObj, modObj, path, operations);
                }
                else if (!JsonNode.DeepEquals(origValue, prop.Value))
                {
                    operations.Add(JsonPatchOperation.Replace(path, ToJsonElement(prop.Value)));
                }
            }
        }

        foreach (var prop in original)
        {
            if (!modified.ContainsKey(prop.Key))
            {
                operations.Add(JsonPatchOperation.Remove(prefix + "/" + EscapePointer(prop.Key)));
            }
        }
    }

    // RFC 6901: '~' → '~0' and '/' → '~1' (in that order) so a property name containing those characters
    // produces a valid JSON Pointer segment.
    static string EscapePointer(string key) => key.Replace("~", "~0").Replace("/", "~1");

    static JsonElement ToJsonElement(JsonNode? node)
        => node is null
            ? JsonDocument.Parse("null").RootElement.Clone()
            : JsonDocument.Parse(node.ToJsonString()).RootElement.Clone();
}
