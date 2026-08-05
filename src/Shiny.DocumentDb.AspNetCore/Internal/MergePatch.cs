using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.AspNetCore.Internal;

/// <summary>
/// RFC 7396 JSON Merge Patch, applied to a document the endpoint already read.
/// </summary>
/// <remarks>
/// The core store has its own merge, but it strips nulls first — it must, because a serialized CLR document
/// carries a null for every unset member, and treating those as deletions would wipe fields nobody touched.
/// An HTTP PATCH body is not a serialized document: every member in it was written by the caller, so an
/// explicit <c>null</c> means <b>remove</b>, exactly as RFC 7396 says. That difference is why this lives here
/// rather than being delegated.
/// </remarks>
static class MergePatch
{
    public static void Apply(JsonObject target, JsonObject patch)
    {
        foreach (var member in patch.ToList())
        {
            if (member.Value is null)
            {
                target.Remove(member.Key);
            }
            else if (member.Value is JsonObject nested && target[member.Key] is JsonObject existing)
            {
                Apply(existing, nested);
            }
            else
            {
                target[member.Key] = member.Value.DeepClone();
            }
        }
    }
}
