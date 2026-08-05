using System.Text.Json;
using System.Text.Json.Nodes;
using Shiny.DocumentDb;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Recognises Shiny.DocumentDb field-level encryption envelopes inside a stored document body.
/// </summary>
/// <remarks>
/// <para>
/// The admin tool writes raw JSON over ADO and never goes through the library, so nothing here decrypts and
/// nothing here encrypts. What it does is know an envelope when it sees one — which is enough to render it
/// honestly, to report which keys a type's documents are under, and to refuse a save that would replace one
/// with clear text.
/// </para>
/// <para>
/// The shape test itself is <see cref="DocumentEncryptionFormat"/>, the library's own public contract, so this
/// cannot drift from what the store writes. In particular <c>SecretProtector</c>'s unrelated <c>enc:v1:</c>
/// profile-secret format is <b>not</b> an envelope: its version segment does not parse as an integer.
/// </para>
/// </remarks>
public static class EncryptedFields
{
    /// <summary>True when this JSON value is an encryption envelope. String values only.</summary>
    public static bool TryRead(JsonNode? node, out EncryptedValueInfo info)
    {
        info = default;

        return node is JsonValue value
               && value.GetValueKind() == JsonValueKind.String
               && DocumentEncryptionFormat.TryParse(value.GetValue<string>(), out info);
    }

    /// <summary>
    /// The encrypted paths in a body, dotted, one level deep — the same walk the Structure tab does, so the
    /// paths line up with the fields it lists.
    /// </summary>
    public static IReadOnlyList<string> PathsIn(JsonNode? body)
    {
        if (body is not JsonObject obj)
            return [];

        var paths = new List<string>();
        Walk(obj, prefix: "", paths);
        return paths;
    }

    static void Walk(JsonObject obj, string prefix, List<string> into)
    {
        foreach (var (key, value) in obj)
        {
            var path = prefix.Length == 0 ? key : $"{prefix}.{key}";

            if (TryRead(value, out _))
                into.Add(path);
            else if (value is JsonObject nested && prefix.Length == 0)
                Walk(nested, path, into);
        }
    }

    /// <summary>
    /// Reads a dotted path out of a body, one level deep — the lookup the downgrade diff and the inventory
    /// both need, and the one <c>DocumentRow.Read</c> performs against a parsed row.
    /// </summary>
    public static JsonNode? Read(JsonNode? body, string path)
    {
        var node = body;
        foreach (var segment in path.Split('.', StringSplitOptions.RemoveEmptyEntries))
        {
            if (node is not JsonObject obj || !obj.TryGetPropertyValue(segment, out var next))
                return null;

            node = next;
        }

        return node;
    }
}
