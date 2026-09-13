using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Shiny.DocumentDb;

/// <summary>
/// The key a document occupies in a unique index — computed from the document's stored JSON so every provider
/// that enforces uniqueness itself (rather than through a native database index) agrees on exactly the same
/// rules: scoped to the type and the index, excluded when the filter rejects the document or any key part is
/// <c>null</c>/missing, and compared exactly as stored.
/// </summary>
public static class UniqueIndexKeys
{
    const char Separator = '';

    /// <summary>
    /// The entries <paramref name="document"/> occupies across <paramref name="indexes"/> — one per index whose
    /// filter it passes and whose every key part holds a non-null value.
    /// </summary>
    /// <param name="indexes">The type's unique indexes (<see cref="DocumentMappingRegistry.ResolveUniqueIndexes"/>).</param>
    /// <param name="typeName">The stored type name, so identical values on different types never collide.</param>
    /// <param name="document">The document as written — the filter is evaluated against it.</param>
    /// <param name="json">The document's JSON exactly as it will be stored.</param>
    /// <param name="jsonOptions">The store's serializer options, used to resolve each key part's JSON path.</param>
    public static IReadOnlyList<UniqueIndexEntry> Compute(
        IReadOnlyList<UniqueIndexMapping> indexes,
        string typeName,
        object document,
        string json,
        JsonSerializerOptions jsonOptions)
    {
        ArgumentNullException.ThrowIfNull(document);
        return Compute(indexes, typeName, json, () => document, jsonOptions);
    }

    /// <summary>
    /// As <see cref="Compute(IReadOnlyList{UniqueIndexMapping}, string, object, string, JsonSerializerOptions)"/>,
    /// for a caller that only holds the JSON: <paramref name="document"/> is invoked (at most once) only when an
    /// index has a filter to evaluate.
    /// </summary>
    public static IReadOnlyList<UniqueIndexEntry> Compute(
        IReadOnlyList<UniqueIndexMapping> indexes,
        string typeName,
        string json,
        Func<object> document,
        JsonSerializerOptions jsonOptions)
    {
        ArgumentNullException.ThrowIfNull(indexes);
        ArgumentNullException.ThrowIfNull(typeName);
        ArgumentNullException.ThrowIfNull(json);
        ArgumentNullException.ThrowIfNull(document);
        ArgumentNullException.ThrowIfNull(jsonOptions);

        if (indexes.Count == 0)
            return [];

        object? materialized = null;
        var entries = new List<UniqueIndexEntry>(indexes.Count);
        using var parsed = JsonDocument.Parse(json);
        foreach (var index in indexes)
        {
            if (index.Filter != null && !index.AppliesTo(materialized ??= document()))
                continue;

            var key = BuildKey(typeName, index, parsed.RootElement, jsonOptions);
            if (key != null)
                entries.Add(new UniqueIndexEntry(index, key));
        }
        return entries;
    }

    /// <summary>
    /// What changed between the entries a document held before a write and the ones it holds after:
    /// <c>Added</c> must be claimed, <c>Removed</c> released. An entry present on both sides is neither.
    /// </summary>
    public static (IReadOnlyList<UniqueIndexEntry> Added, IReadOnlyList<UniqueIndexEntry> Removed) Diff(
        IReadOnlyList<UniqueIndexEntry> before,
        IReadOnlyList<UniqueIndexEntry> after)
    {
        ArgumentNullException.ThrowIfNull(before);
        ArgumentNullException.ThrowIfNull(after);

        var beforeKeys = before.Select(e => e.Key).ToHashSet(StringComparer.Ordinal);
        var afterKeys = after.Select(e => e.Key).ToHashSet(StringComparer.Ordinal);
        return (
            after.Where(e => !beforeKeys.Contains(e.Key)).ToList(),
            before.Where(e => !afterKeys.Contains(e.Key)).ToList()
        );
    }

    /// <summary>A lower-case hex SHA-256 of <paramref name="key"/> — 64 characters, safe as a key on any backend.</summary>
    public static string HashKey(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(key))).ToLowerInvariant();
    }

    static string? BuildKey(string typeName, UniqueIndexMapping index, JsonElement root, JsonSerializerOptions jsonOptions)
    {
        var sb = new StringBuilder(typeName).Append(Separator).Append(index.Name);
        foreach (var path in index.GetJsonPaths(jsonOptions))
        {
            if (!TryNavigate(root, path, out var value))
                return null;

            sb.Append(Separator);
            switch (value.ValueKind)
            {
                case JsonValueKind.String:
                    sb.Append('s').Append(value.GetString());
                    break;
                case JsonValueKind.Number:
                    sb.Append('n').Append(CanonicalNumber(value));
                    break;
                case JsonValueKind.True or JsonValueKind.False:
                    sb.Append('b').Append(value.GetRawText());
                    break;
                case JsonValueKind.Object or JsonValueKind.Array:
                    sb.Append('j').Append(value.GetRawText());
                    break;
                default:
                    return null;
            }
        }
        return sb.ToString();
    }

    // Equal numbers must share a key however they are spelled: 1.50, 1.5 and 15e-1 are one value, and a backend that
    // re-formats a stored number (RedisJSON, Firestore) must not move a document off the key it was reserved under.
    // Dividing by a one with a high scale is the decimal idiom for dropping trailing zeros.
    static string CanonicalNumber(JsonElement value)
        => value.TryGetDecimal(out var number)
            ? (number / 1.000000000000000000000000000000000m).ToString(CultureInfo.InvariantCulture)
            : value.GetDouble().ToString("R", CultureInfo.InvariantCulture);

    static bool TryNavigate(JsonElement root, string path, out JsonElement value)
    {
        value = root;
        foreach (var segment in path.Split('.'))
        {
            if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty(segment, out value))
                return false;
        }
        return true;
    }
}

/// <summary>One unique-index entry a document occupies — see <see cref="UniqueIndexKeys"/>.</summary>
/// <param name="Mapping">The index the entry belongs to.</param>
/// <param name="Key">The canonical key: type name, index name and each key value, separator-joined.</param>
public sealed record UniqueIndexEntry(UniqueIndexMapping Mapping, string Key)
{
    /// <summary><see cref="UniqueIndexKeys.HashKey"/> of <see cref="Key"/> — use it where a backend limits key length or characters.</summary>
    public string Hash { get; } = UniqueIndexKeys.HashKey(Key);
}
