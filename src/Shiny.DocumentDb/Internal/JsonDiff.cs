using System.Text.Json;
using System.Text.Json.Nodes;
using SystemTextJsonPatch;
using SystemTextJsonPatch.Operations;

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

        var patch = new JsonPatchDocument<T>();
        patch.Options = options;
        BuildDiff(original, modified, "", patch.Operations);
        return patch;
    }

    static void BuildDiff<T>(
        JsonObject original,
        JsonObject modified,
        string prefix,
        List<Operation<T>> operations) where T : class
    {
        foreach (var prop in modified)
        {
            var path = prefix + "/" + prop.Key;
            var origValue = original[prop.Key];

            if (origValue is null && prop.Value is not null)
            {
                operations.Add(new Operation<T>("add", path, null, ToJsonElement(prop.Value)));
            }
            else if (prop.Value is null && origValue is not null)
            {
                operations.Add(new Operation<T>("replace", path, null, null));
            }
            else if (origValue is not null && prop.Value is not null)
            {
                if (origValue is JsonObject origObj && prop.Value is JsonObject modObj)
                {
                    BuildDiff(origObj, modObj, path, operations);
                }
                else if (!JsonNode.DeepEquals(origValue, prop.Value))
                {
                    operations.Add(new Operation<T>("replace", path, null, ToJsonElement(prop.Value)));
                }
            }
        }

        foreach (var prop in original)
        {
            if (!modified.ContainsKey(prop.Key))
            {
                operations.Add(new Operation<T>("remove", prefix + "/" + prop.Key, null));
            }
        }
    }

    static JsonElement ToJsonElement(JsonNode node)
        => JsonDocument.Parse(node.ToJsonString()).RootElement.Clone();
}
