using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Encodes / decodes the opaque continuation token used by cursor (keyset) pagination. The token is
/// <c>base64url(UTF-8 JSON)</c> (no padding): small, self-describing, and forward-compatible via the
/// <c>v</c> field. It carries the trailing row's typed sort-key values plus a shape hash of the query's
/// ordering so a cursor reused against a different <c>OrderBy</c> fails loudly instead of returning garbage.
/// <para>
/// Opacity, not security: the payload exposes the sort-key values (e.g. a timestamp). Fine for the common
/// case; a future <c>ICursorProtector</c> can encrypt/sign it.
/// </para>
/// </summary>
static class CursorCodec
{
    const int Version = 1;

    /// <summary>Builds the shape hash for a keyset cursor from a normalized ordering spec.</summary>
    public static string ComputeShapeHash(string spec)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes("v1|" + spec));
        return Convert.ToHexStringLower(bytes)[..16];
    }

    /// <summary>
    /// Encodes a keyset continuation token from the trailing row's sort-key + Id values (in key order).
    /// </summary>
    public static string EncodeKeyset(string shapeHash, IReadOnlyList<object?> values)
    {
        var keys = new JsonArray();
        foreach (var value in values)
            keys.Add((JsonNode)EncodeValue(value));

        var payload = new JsonObject
        {
            ["v"] = Version,
            ["t"] = "k",
            ["h"] = shapeHash,
            ["k"] = keys
        };

        var json = payload.ToJsonString();
        return Base64UrlEncode(Encoding.UTF8.GetBytes(json));
    }

    /// <summary>
    /// Decodes a keyset continuation token, validating its version and shape hash. Returns the sort-key +
    /// Id values (in key order) reconstructed as CLR objects ready to bind as query parameters.
    /// </summary>
    public static IReadOnlyList<object?> DecodeKeyset(string cursor, string expectedShapeHash)
    {
        JsonObject payload;
        try
        {
            var json = Encoding.UTF8.GetString(Base64UrlDecode(cursor));
            payload = JsonNode.Parse(json) as JsonObject
                ?? throw new InvalidOperationException("Cursor payload is not a JSON object.");
        }
        catch (Exception ex) when (ex is not InvalidOperationException)
        {
            throw new InvalidOperationException("The cursor is malformed or corrupt.", ex);
        }

        var version = payload["v"]?.GetValue<int>() ?? 0;
        if (version != Version)
            throw new InvalidOperationException(
                $"Unsupported cursor version '{version}'. This cursor was produced by a different library version.");

        var type = payload["t"]?.GetValue<string>();
        if (type != "k")
            throw new InvalidOperationException($"Cursor is not a keyset cursor (type '{type}').");

        var hash = payload["h"]?.GetValue<string>();
        if (!string.Equals(hash, expectedShapeHash, StringComparison.Ordinal))
            throw new InvalidOperationException(
                "The cursor does not match this query. A cursor is only valid for the exact ordering that " +
                "produced it — the OrderBy (and filters) must be identical across page calls.");

        if (payload["k"] is not JsonArray keys)
            throw new InvalidOperationException("The cursor is malformed or corrupt.");

        var values = new object?[keys.Count];
        try
        {
            for (var i = 0; i < keys.Count; i++)
                values[i] = DecodeValue(keys[i] as JsonObject
                    ?? throw new InvalidOperationException("The cursor is malformed or corrupt."));
        }
        catch (Exception ex) when (ex is not InvalidOperationException)
        {
            // A structurally-valid token whose value payload is corrupt (e.g. a non-Guid where a Guid tag
            // claims one) must surface as the documented InvalidOperationException, not a raw FormatException.
            throw new InvalidOperationException("The cursor is malformed or corrupt.", ex);
        }
        return values;
    }

    /// <summary>
    /// Coerces a live CLR value to the canonical type <see cref="DecodeKeyset"/> reconstructs (integral →
    /// <see cref="long"/>, floating → <see cref="double"/>, enum → its underlying integral, all others
    /// unchanged). The in-memory pager normalizes both a candidate row's key values and the decoded cursor
    /// values through this so they compare as the same runtime type.
    /// </summary>
    public static object? NormalizeForCompare(object? value)
    {
        if (value is Enum e)
            value = Convert.ChangeType(e, Enum.GetUnderlyingType(e.GetType()), CultureInfo.InvariantCulture);

        return value switch
        {
            null => null,
            byte or sbyte or short or ushort or int or uint or long or ulong => Convert.ToInt64(value, CultureInfo.InvariantCulture),
            float or double => Convert.ToDouble(value, CultureInfo.InvariantCulture),
            _ => value
        };
    }

    static JsonObject EncodeValue(object? value)
    {
        // Enums bind as their underlying numeric value (matching the JSON representation), so normalize first.
        if (value is Enum e)
            value = Convert.ChangeType(e, Enum.GetUnderlyingType(e.GetType()), CultureInfo.InvariantCulture);

        return value switch
        {
            null => new JsonObject { ["t"] = "0" },
            string s => new JsonObject { ["t"] = "s", ["v"] = s },
            bool b => new JsonObject { ["t"] = "b", ["v"] = b },
            Guid g => new JsonObject { ["t"] = "g", ["v"] = g.ToString() },
            DateTime dt => new JsonObject { ["t"] = "dt", ["v"] = dt.ToString("O", CultureInfo.InvariantCulture) },
            DateTimeOffset dto => new JsonObject { ["t"] = "do", ["v"] = dto.ToString("O", CultureInfo.InvariantCulture) },
            decimal m => new JsonObject { ["t"] = "m", ["v"] = m.ToString(CultureInfo.InvariantCulture) },
            float or double => new JsonObject { ["t"] = "f", ["v"] = Convert.ToDouble(value, CultureInfo.InvariantCulture).ToString("R", CultureInfo.InvariantCulture) },
            byte or sbyte or short or ushort or int or uint or long or ulong =>
                new JsonObject { ["t"] = "i", ["v"] = Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture) },
            _ => throw new NotSupportedException(
                $"Sort key of type '{value.GetType().Name}' cannot be used with cursor pagination. " +
                "Order by a string, number, bool, Guid, or date/time property.")
        };
    }

    static object? DecodeValue(JsonObject node)
    {
        var tag = node["t"]?.GetValue<string>();
        var raw = node["v"]?.GetValue<string>();
        return tag switch
        {
            "0" => null,
            "s" => raw,
            "b" => bool.Parse(raw!),
            "g" => Guid.Parse(raw!),
            "dt" => DateTime.Parse(raw!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            "do" => DateTimeOffset.Parse(raw!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            "m" => decimal.Parse(raw!, CultureInfo.InvariantCulture),
            "f" => double.Parse(raw!, CultureInfo.InvariantCulture),
            "i" => long.Parse(raw!, CultureInfo.InvariantCulture),
            _ => throw new InvalidOperationException($"Unknown cursor value type '{tag}'.")
        };
    }

    static string Base64UrlEncode(byte[] bytes)
        => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    static byte[] Base64UrlDecode(string value)
    {
        var s = value.Replace('-', '+').Replace('_', '/');
        switch (s.Length % 4)
        {
            case 2: s += "=="; break;
            case 3: s += "="; break;
        }
        return Convert.FromBase64String(s);
    }
}
