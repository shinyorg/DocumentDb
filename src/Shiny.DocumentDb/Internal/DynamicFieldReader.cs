using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Reads a dot-separated JSON path out of a <see cref="JsonNode"/> and unwraps the leaf to a requested CLR
/// type. This is the in-memory twin of the relational <c>json_extract</c>: the in-memory providers
/// (LiteDB, IndexedDB) and every client-side filter path evaluate a schema-free predicate by walking the
/// document with this rather than by reflecting over a CLR type that does not exist.
/// </summary>
static class DynamicFieldReader
{
    /// <summary>
    /// Returns the value at <paramref name="jsonPath"/> coerced to <paramref name="clrType"/>, or
    /// <c>null</c> when the path is absent, JSON-null, or not convertible.
    /// </summary>
    public static object? Read(JsonNode? root, string jsonPath, Type clrType)
    {
        var node = Resolve(root, jsonPath);
        return node is null ? null : Unwrap(node, clrType);
    }

    static JsonNode? Resolve(JsonNode? root, string jsonPath)
    {
        var current = root;
        var start = 0;
        while (current is not null)
        {
            var dot = jsonPath.IndexOf('.', start);
            var segment = dot < 0 ? jsonPath[start..] : jsonPath[start..dot];

            if (current is not JsonObject obj || !TryGetProperty(obj, segment, out current))
                return null;

            if (dot < 0)
                return current;
            start = dot + 1;
        }
        return null;
    }

    // JSON property names are case-sensitive, but a schema-free caller has no metadata telling them whether
    // the writer used a naming policy — so fall back to a case-insensitive scan, matching how the id property
    // is read.
    static bool TryGetProperty(JsonObject obj, string name, out JsonNode? value)
    {
        if (obj.TryGetPropertyValue(name, out value))
            return true;

        foreach (var kvp in obj)
        {
            if (string.Equals(kvp.Key, name, StringComparison.OrdinalIgnoreCase))
            {
                value = kvp.Value;
                return true;
            }
        }

        value = null;
        return false;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2067",
        Justification = "Target types are the closed set produced by the dynamic field binder (string, numeric, bool, DateTime, Guid).")]
    static object? Unwrap(JsonNode node, Type clrType)
    {
        if (node is not JsonValue value)
            return null; // an object/array leaf has no scalar CLR projection

        var target = Nullable.GetUnderlyingType(clrType) ?? clrType;

        // Go through the JSON text so "100" and 100 both reach a numeric target — the schema-free lane cannot
        // assume a writer stored a number as a number, and this mirrors json_extract handing back text.
        var raw = value.GetValueKind() switch
        {
            JsonValueKind.String => value.GetValue<string>(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null or JsonValueKind.Undefined => null,
            _ => value.ToJsonString()
        };
        if (raw is null)
            return null;

        try
        {
            if (target == typeof(string)) return raw;
            if (target == typeof(bool)) return bool.Parse(raw);
            if (target == typeof(Guid)) return Guid.Parse(raw);
            if (target == typeof(DateTime)) return DateTime.Parse(raw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            if (target == typeof(DateTimeOffset)) return DateTimeOffset.Parse(raw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            if (target == typeof(int)) return int.Parse(raw, CultureInfo.InvariantCulture);
            if (target == typeof(long)) return long.Parse(raw, CultureInfo.InvariantCulture);
            if (target == typeof(short)) return short.Parse(raw, CultureInfo.InvariantCulture);
            if (target == typeof(byte)) return byte.Parse(raw, CultureInfo.InvariantCulture);
            if (target == typeof(double)) return double.Parse(raw, CultureInfo.InvariantCulture);
            if (target == typeof(float)) return float.Parse(raw, CultureInfo.InvariantCulture);
            if (target == typeof(decimal)) return decimal.Parse(raw, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException or OverflowException)
        {
            // A value that does not fit the inferred type is "no match", not an error — the same thing the
            // relational lane does when a CAST yields NULL.
            return null;
        }

        return raw;
    }
}
