using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Determines how an enum is stored in the JSON document under a given <see cref="JsonSerializerOptions"/> —
/// numeric (the default) or a string (when a <c>JsonStringEnumConverter</c>, a source-generated
/// <c>JsonStringEnumConverter&lt;T&gt;</c>, or a <c>[JsonConverter]</c> that emits strings is in effect).
/// <para>
/// The relational query pipeline binds enum comparisons as their underlying numeric value by default (matching
/// a compiler-lifted <c>== EnumValue</c> constant). When the effective converter serializes the enum as a
/// string instead, the stored JSON holds the member name, so a numeric bind matches nothing. This helper lets
/// the lowerer bind the same string the write path persisted, and extract the field as text rather than casting
/// it to an integer.
/// </para>
/// </summary>
static class EnumJsonStorage
{
    // Per-options cache: enum type → serializes-as-string. Keyed weakly on the options so it can't keep a
    // store's JsonSerializerOptions alive; each store typically has a single options instance.
    static readonly ConditionalWeakTable<JsonSerializerOptions, ConcurrentDictionary<Type, bool>> Cache = new();

    /// <summary>
    /// True when <paramref name="type"/> (or its nullable underlying) is an enum that serializes as a JSON
    /// string under <paramref name="options"/>. False for non-enums and numeric-stored enums.
    /// </summary>
    public static bool SerializesAsString(Type type, JsonSerializerOptions options)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;
        if (!t.IsEnum)
            return false;

        var perOptions = Cache.GetValue(options, static _ => new ConcurrentDictionary<Type, bool>());
        return perOptions.GetOrAdd(t, static (enumType, opts) => Probe(enumType, opts), options);
    }

    /// <summary>
    /// When <paramref name="value"/>'s enum type is string-stored, outputs the exact JSON string the write path
    /// would persist (honoring the converter's naming policy). Returns false for numeric-stored enums, so the
    /// caller keeps binding the numeric value.
    /// </summary>
    public static bool TryGetJsonString(Enum value, JsonSerializerOptions options, out string result)
    {
        result = "";
        if (!SerializesAsString(value.GetType(), options))
            return false;

        if (TrySerialize(value, value.GetType(), options, out var json) && json.Length >= 2 && json[0] == '"')
        {
            // Read the JSON string back to its unescaped CLR string — dependency-free, AOT-clean.
            try
            {
                var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
                if (reader.Read() && reader.TokenType == JsonTokenType.String)
                {
                    result = reader.GetString() ?? "";
                    return true;
                }
            }
            catch (JsonException)
            {
            }
        }
        return false;
    }

    static bool Probe(Type enumType, JsonSerializerOptions options)
    {
        // Serialize a defined member (converters emit undefined values inconsistently) and check whether the
        // JSON is a quoted string. Any failure is treated as numeric — the safe, pre-existing behavior.
        var sample = FirstDefinedValue(enumType);
        if (sample == null)
            return false;

        return TrySerialize(sample, enumType, options, out var json) && json.Length >= 1 && json[0] == '"';
    }

    [UnconditionalSuppressMessage("Trimming", "IL2072",
        Justification = "enumType is a user enum reached from a query expression; enum types are value types and are not trimmed.")]
    static object? FirstDefinedValue(Type enumType)
    {
        // GetValuesAsUnderlyingType (rather than GetValues) keeps this AOT-clean: it returns the underlying
        // primitive array, so the runtime never has to construct an array of the enum type itself.
        var values = Enum.GetValuesAsUnderlyingType(enumType);
        return values.Length > 0 ? Enum.ToObject(enumType, values.GetValue(0)!) : null;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Enum-to-JSON probe for query normalization; enums are value types (never trimmed) and any failure falls back to numeric binding.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "Enum-to-JSON probe for query normalization; any failure falls back to numeric binding.")]
    static bool TrySerialize(object value, Type type, JsonSerializerOptions options, out string json)
    {
        json = "";
        try
        {
            json = JsonSerializer.Serialize(value, type, options);
            return true;
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or InvalidOperationException)
        {
            return false;
        }
    }
}
