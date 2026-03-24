using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb;

public sealed class JsonPatchDocument<T> where T : class
{
    readonly JsonSerializerOptions? options;

    public IReadOnlyList<JsonPatchOperation> Operations { get; }

    public JsonPatchDocument(IReadOnlyList<JsonPatchOperation> operations, JsonSerializerOptions? options = null)
    {
        Operations = operations;
        this.options = options;
    }

    public T ApplyTo(T target, JsonTypeInfo<T> typeInfo)
    {
        var node = JsonSerializer.SerializeToNode(target, typeInfo)
            ?? throw new InvalidOperationException("Serialization produced null.");
        ApplyOperations(node);
        return JsonSerializer.Deserialize(node, typeInfo)
            ?? throw new InvalidOperationException("Deserialization produced null.");
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Reflection path only used when JsonTypeInfo is not provided.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Reflection path only used when JsonTypeInfo is not provided.")]
    public T ApplyTo(T target, JsonSerializerOptions? options = null)
    {
        var opts = options ?? this.options;
        var node = JsonSerializer.SerializeToNode(target, opts)
            ?? throw new InvalidOperationException("Serialization produced null.");
        ApplyOperations(node);
        return JsonSerializer.Deserialize<T>(node, opts!)
            ?? throw new InvalidOperationException("Deserialization produced null.");
    }

    void ApplyOperations(JsonNode root)
    {
        foreach (var op in Operations)
            ApplyOp(root, op);
    }

    static void ApplyOp(JsonNode root, JsonPatchOperation op)
    {
        var segments = ParsePath(op.Path);
        switch (op.Op)
        {
            case "add":
            case "replace":
                SetValue(root, segments, op.Value);
                break;
            case "remove":
                RemoveValue(root, segments);
                break;
            case "copy":
                var sourceVal = GetValue(root, ParsePath(op.From!));
                SetValue(root, segments, sourceVal != null
                    ? JsonDocument.Parse(sourceVal.ToJsonString()).RootElement.Clone()
                    : null);
                break;
            case "move":
                var moveVal = GetValue(root, ParsePath(op.From!));
                RemoveValue(root, ParsePath(op.From!));
                SetValue(root, segments, moveVal != null
                    ? JsonDocument.Parse(moveVal.ToJsonString()).RootElement.Clone()
                    : null);
                break;
            case "test":
                var actual = GetValue(root, segments);
                var expected = op.Value.HasValue
                    ? JsonNode.Parse(op.Value.Value.GetRawText())
                    : null;
                if (!JsonNode.DeepEquals(actual, expected))
                    throw new InvalidOperationException($"Test operation failed for path '{op.Path}'.");
                break;
            default:
                throw new NotSupportedException($"Unsupported patch operation: {op.Op}");
        }
    }

    static string[] ParsePath(string path)
    {
        if (string.IsNullOrEmpty(path) || path == "/")
            return [];

        // RFC 6901: skip leading '/'
        return path[1..].Split('/');
    }

    static JsonNode? GetValue(JsonNode root, string[] segments)
    {
        var current = root;
        foreach (var seg in segments)
        {
            if (current is JsonObject obj)
                current = obj[seg];
            else if (current is JsonArray arr && int.TryParse(seg, out var idx))
                current = arr[idx];
            else
                return null;
        }
        return current;
    }

    static void SetValue(JsonNode root, string[] segments, JsonElement? value)
    {
        if (segments.Length == 0)
            throw new InvalidOperationException("Cannot set the root node.");

        var parent = NavigateToParent(root, segments);
        var key = segments[^1];
        var nodeValue = value.HasValue ? JsonNode.Parse(value.Value.GetRawText()) : null;

        if (parent is JsonObject obj)
        {
            obj[key] = nodeValue;
        }
        else if (parent is JsonArray arr && int.TryParse(key, out var idx))
        {
            if (idx == arr.Count)
                arr.Add(nodeValue);
            else
                arr[idx] = nodeValue;
        }
    }

    static void RemoveValue(JsonNode root, string[] segments)
    {
        if (segments.Length == 0)
            throw new InvalidOperationException("Cannot remove the root node.");

        var parent = NavigateToParent(root, segments);
        var key = segments[^1];

        if (parent is JsonObject obj)
            obj.Remove(key);
        else if (parent is JsonArray arr && int.TryParse(key, out var idx))
            arr.RemoveAt(idx);
    }

    static JsonNode NavigateToParent(JsonNode root, string[] segments)
    {
        var current = root;
        for (var i = 0; i < segments.Length - 1; i++)
        {
            var seg = segments[i];
            if (current is JsonObject obj)
                current = obj[seg] ?? throw new InvalidOperationException($"Path segment '{seg}' not found.");
            else if (current is JsonArray arr && int.TryParse(seg, out var idx))
                current = arr[idx] ?? throw new InvalidOperationException($"Array index '{idx}' not found.");
            else
                throw new InvalidOperationException($"Cannot navigate path segment '{seg}'.");
        }
        return current;
    }
}
