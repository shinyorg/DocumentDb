using System.Text.Json;
using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Models;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Compares two document bodies and reports what changed, path by path.
/// </summary>
/// <remarks>
/// The library has its own diff (<c>JsonDiff</c>, behind <c>GetDiffBetween&lt;T&gt;</c>), but it is
/// internal, generic over a CLR document type, and produces an RFC 6902 patch - a thing you apply,
/// not a thing you read. This produces a flat, sorted list of differences meant to be looked at.
/// </remarks>
public static class JsonComparer
{
    /// <summary>How deep a nested object or array is walked before its value is compared whole.</summary>
    public const int MaxDepth = 12;

    /// <summary>Longest rendered value shown for one path before it is cut short.</summary>
    public const int MaxValueLength = 400;

    /// <summary>
    /// Compares two document bodies. Either side may be null - a Removed tombstone has no body -
    /// which reads as everything on the other side being added or removed.
    /// </summary>
    public static IReadOnlyList<JsonChange> Compare(string? before, string? after)
    {
        var left = Parse(before);
        var right = Parse(after);

        var changes = new List<JsonChange>();
        Walk(left, right, path: "", changes, depth: 0, before is not null, after is not null);

        return [.. changes.OrderBy(x => x.Path, StringComparer.OrdinalIgnoreCase)];
    }

    /// <summary>
    /// Compares one pair of nodes. <paramref name="leftPresent"/> / <paramref name="rightPresent"/>
    /// carry whether the key existed at all, which a null node cannot: <c>JsonObject</c> stores a JSON
    /// null as a C# null, so "set to null" and "key removed" are otherwise indistinguishable.
    /// </summary>
    static void Walk(
        JsonNode? left,
        JsonNode? right,
        string path,
        List<JsonChange> changes,
        int depth,
        bool leftPresent,
        bool rightPresent)
    {
        if (leftPresent == rightPresent && AreEqual(left, right))
            return;

        // Past the depth limit, or where the two sides are not the same shape, the pair is reported as
        // one change rather than descending into a structure that no longer lines up.
        if (depth >= MaxDepth || !SameShape(left, right))
        {
            changes.Add(Change(path, left, right, leftPresent, rightPresent));
            return;
        }

        switch (left)
        {
            case JsonObject leftObject when right is JsonObject rightObject:
                foreach (var key in Keys(leftObject, rightObject))
                {
                    var hasLeft = leftObject.ContainsKey(key);
                    var hasRight = rightObject.ContainsKey(key);
                    var childPath = path.Length == 0 ? key : $"{path}.{key}";

                    if (!hasLeft || !hasRight)
                    {
                        // Key present on one side only: report the whole subtree, not its innards.
                        changes.Add(Change(
                            childPath,
                            hasLeft ? leftObject[key] : null,
                            hasRight ? rightObject[key] : null,
                            hasLeft,
                            hasRight));

                        continue;
                    }

                    Walk(leftObject[key], rightObject[key], childPath, changes, depth + 1, true, true);
                }

                break;

            case JsonArray leftArray when right is JsonArray rightArray:
                // Positional comparison. An insert at the front therefore reads as "everything after it
                // changed", which is honest about what the stored document actually looks like.
                for (var i = 0; i < Math.Max(leftArray.Count, rightArray.Count); i++)
                {
                    var childPath = $"{path}[{i}]";
                    if (i >= leftArray.Count)
                        changes.Add(Change(childPath, null, rightArray[i], false, true));
                    else if (i >= rightArray.Count)
                        changes.Add(Change(childPath, leftArray[i], null, true, false));
                    else
                        Walk(leftArray[i], rightArray[i], childPath, changes, depth + 1, true, true);
                }

                break;

            default:
                changes.Add(Change(path, left, right, leftPresent, rightPresent));
                break;
        }
    }

    static JsonChange Change(string path, JsonNode? left, JsonNode? right, bool leftPresent, bool rightPresent)
    {
        var kind = (leftPresent, rightPresent) switch
        {
            (false, true) => JsonChangeKind.Added,
            (true, false) => JsonChangeKind.Removed,
            _ => JsonChangeKind.Modified
        };

        return new JsonChange(
            path.Length == 0 ? "(document)" : path,
            kind,
            leftPresent ? Render(left) : null,
            rightPresent ? Render(right) : null);
    }

    /// <summary>The value as a person should read it: scalars bare, structures as compact JSON.</summary>
    static string Render(JsonNode? node)
    {
        if (node is null)
            return "null";

        var text = node switch
        {
            JsonValue value when value.GetValueKind() == JsonValueKind.String => value.GetValue<string>(),
            _ => node.ToJsonString(Compact)
        };

        return text.Length > MaxValueLength ? text[..MaxValueLength] + "…" : text;
    }

    static IEnumerable<string> Keys(JsonObject left, JsonObject right)
        => left.Select(x => x.Key)
            .Concat(right.Select(x => x.Key))
            .Distinct(StringComparer.Ordinal);

    /// <summary>Both sides are objects, both arrays, or both something else.</summary>
    static bool SameShape(JsonNode? left, JsonNode? right)
        => (left is JsonObject && right is JsonObject) || (left is JsonArray && right is JsonArray);

    static bool AreEqual(JsonNode? left, JsonNode? right)
    {
        if (left is null || right is null)
            return left is null && right is null;

        // Serialized comparison rather than JsonNode.DeepEquals: it treats 1 and 1.0 as different, which
        // is the right answer for a tool showing you what is actually stored in the column.
        return string.Equals(left.ToJsonString(Compact), right.ToJsonString(Compact), StringComparison.Ordinal);
    }

    static JsonNode? Parse(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return null;

        try
        {
            return JsonNode.Parse(json);
        }
        catch (JsonException)
        {
            // A body that will not parse still has to compare against something; treat it as a scalar
            // string so the versions show as "modified" rather than blowing up the page.
            return JsonValue.Create(json);
        }
    }

    static readonly JsonSerializerOptions Compact = new()
    {
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };
}
