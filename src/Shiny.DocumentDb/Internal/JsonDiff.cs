using System.Diagnostics.CodeAnalysis;
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
        // Every provider's GetDiff / GetDiffBetween funnels through here, so the encryption normalization
        // lives here rather than in each of them.
        if (EncryptionRegistry.For(typeof(T)).Count > 0)
        {
            var plaintext = EncryptionRegistry.PlaintextView(options);
            originalJson = Decrypt<T>(originalJson, options, plaintext);
            modifiedJson = Decrypt<T>(modifiedJson, options, plaintext);
            options = plaintext;
        }

        var original = JsonNode.Parse(originalJson)?.AsObject()
            ?? throw new InvalidOperationException("Original document JSON is not a valid object.");
        var modified = JsonNode.Parse(modifiedJson)?.AsObject()
            ?? throw new InvalidOperationException("Modified document JSON is not a valid object.");

        var operations = new List<JsonPatchOperation>();
        BuildDiff(original, modified, "", operations);
        return new JsonPatchDocument<T>(operations, options);
    }

    /// <summary>
    /// Rewrites one side of the diff into its plaintext view. Both sides arrive as ciphertext — the stored body
    /// holds envelopes, and the "modified" side was serialized through the same encrypting options — and under
    /// randomized encryption the same plaintext encrypts to a different envelope every time, so comparing them
    /// reports every mapped property as changed on every call.
    /// </summary>
    /// <remarks>
    /// The round trip through <typeparamref name="T"/> is deliberate: it reuses the very converters that
    /// produced the envelopes rather than re-deriving the codec here, and it only runs for a type that actually
    /// maps encryption.
    /// </remarks>
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Only reached when the type maps encrypted properties, which requires a TypeInfoResolver on these options "
                      + "(EncryptionRegistry.InstallModifier throws without one) — so this never falls back to reflection.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Same encryption-configured path; see the IL2026 justification.")]
    static string Decrypt<T>(string json, JsonSerializerOptions encrypting, JsonSerializerOptions plaintext) where T : class
    {
        var document = JsonSerializer.Deserialize<T>(json, encrypting)
            ?? throw new InvalidOperationException($"Failed to materialize a '{typeof(T).Name}' document for diffing.");
        return JsonSerializer.Serialize(document, plaintext);
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
