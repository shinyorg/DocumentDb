using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.Internal;

/// <summary>Shared bits of the raw JSON terminals, so the node lane parses (and fails) the same way everywhere.</summary>
static class RawJson
{
    public static JsonObject ParseObject(string raw, Type documentType)
        => JsonNode.Parse(raw) as JsonObject
            ?? throw new InvalidOperationException(
                $"The stored body for '{documentType.Name}' is not a JSON object. Read it with the raw terminals " +
                "(ToRawJsonAsyncEnumerable / FirstOrDefaultRawJson) instead.");
}
