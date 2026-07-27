using System.Globalization;
using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Id handling for a schema-free collection: the id lives at a caller-named JSON property and is always a
/// literal string at the storage layer.
/// </summary>
/// <remarks>
/// <para>
/// Read <b>case-insensitively</b> (<c>id</c> / <c>Id</c> / <c>ID</c> all match) because a schema-free caller
/// has no metadata telling them which casing the writer used, but written back <b>verbatim</b> under the
/// configured name.
/// </para>
/// <para>
/// Auto-generation is a sortable <b>UUIDv7</b> in <c>"N"</c> form. This deliberately differs from the
/// type-keyed lane, where a declared <c>Guid</c> id generates a v4 and a declared <c>string</c> id refuses to
/// generate at all — a collection with no declared id type has nothing to honour, and a time-ordered string
/// is the only default that keeps inserts index-friendly.
/// </para>
/// <para>
/// The documented consequence of "ids are literal strings" is that you get back exactly what you stored:
/// store a dashed Guid string and <c>Get(someGuid)</c> misses, because a <see cref="Guid"/> formats to
/// <c>"N"</c>.
/// </para>
/// </remarks>
sealed class DynamicIdBinding : IJsonLaneIds
{
    public DynamicIdBinding(string idProperty)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(idProperty);
        this.MemberName = idProperty;
    }

    public string MemberName { get; }

    // Never consulted: TryGenerateId always answers, so the store's sequence path is not reached.
    public IdKind Kind => IdKind.String;

    public string? ReadStorageId(JsonObject doc)
    {
        var node = this.Find(doc);
        if (node is null)
            return null;

        return node.GetValueKind() switch
        {
            System.Text.Json.JsonValueKind.String => node.GetValue<string>(),
            System.Text.Json.JsonValueKind.Number => node.ToJsonString(),
            _ => throw new ArgumentException(
                $"The id property '{this.MemberName}' must be a string or a number, but was {node.GetValueKind()}.")
        };
    }

    public bool IsDefaultId(JsonObject doc)
    {
        var node = this.Find(doc);
        return node is null || (node.GetValueKind() == System.Text.Json.JsonValueKind.String && node.GetValue<string>().Length == 0);
    }

    public string ResolveId(object id) => id switch
    {
        string s => s,
        Guid g => g.ToString("N"),
        int i => i.ToString(CultureInfo.InvariantCulture),
        long l => l.ToString(CultureInfo.InvariantCulture),
        _ => throw new ArgumentException(
            $"A schema-free collection's id must be a string, Guid, int or long, but was '{id.GetType().Name}'. " +
            "Ids compare as literal strings — pass back what you stored.", nameof(id))
    };

    public void WriteId(JsonObject doc, string storageId)
    {
        // Written under the configured name verbatim, and any differently-cased duplicate is dropped so the
        // body cannot end up carrying two ids.
        this.RemoveOtherCasings(doc);
        doc[this.MemberName] = JsonValue.Create(storageId);
    }

    public string? TryGenerateId(string typeName) => Guid.CreateVersion7().ToString("N");

    JsonNode? Find(JsonObject doc)
    {
        if (doc.TryGetPropertyValue(this.MemberName, out var node))
            return node;

        foreach (var kvp in doc)
        {
            if (string.Equals(kvp.Key, this.MemberName, StringComparison.OrdinalIgnoreCase))
                return kvp.Value;
        }
        return null;
    }

    void RemoveOtherCasings(JsonObject doc)
    {
        List<string>? stale = null;
        foreach (var kvp in doc)
        {
            if (!string.Equals(kvp.Key, this.MemberName, StringComparison.Ordinal)
                && string.Equals(kvp.Key, this.MemberName, StringComparison.OrdinalIgnoreCase))
            {
                (stale ??= []).Add(kvp.Key);
            }
        }
        if (stale != null)
            foreach (var key in stale)
                doc.Remove(key);
    }
}
