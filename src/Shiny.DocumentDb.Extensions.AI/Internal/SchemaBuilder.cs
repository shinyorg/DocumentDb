using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Extensions.AI.Internal;

/// <summary>
/// Builds JSON Schema fragments from STJ <see cref="JsonTypeInfo"/> metadata and
/// <c>[Description]</c>/<c>[DisplayName]</c> attributes.
/// </summary>
static class SchemaBuilder
{
    /// <summary>
    /// Produces the LLM-visible field list for a document type, honoring allow/ignore lists
    /// and pulling descriptions from attributes (with caller overrides taking precedence).
    /// </summary>
    public static IReadOnlyList<DocumentField> BuildFields<T>(DocumentAITypeRegistration<T> reg) where T : class
    {
        var allowed = reg.AllowedProperties is { Count: > 0 } a ? new HashSet<string>(a, StringComparer.Ordinal) : null;
        var ignored = reg.IgnoredProperties is { Count: > 0 } i ? new HashSet<string>(i, StringComparer.Ordinal) : null;

        var fields = new List<DocumentField>();
        foreach (var prop in reg.JsonTypeInfo.Properties)
        {
            if (allowed != null && !allowed.Contains(prop.Name))
                continue;
            if (ignored != null && ignored.Contains(prop.Name))
                continue;

            string? description = null;
            PropertyInfo? clrProperty = null;
            if (prop.AttributeProvider is MemberInfo mi)
            {
                if (reg.PropertyDescriptionOverrides.TryGetValue(prop.Name, out var overridden))
                    description = overridden;
                else
                    description = ReadMemberDescription(mi);
                clrProperty = mi as PropertyInfo;
            }
            else if (reg.PropertyDescriptionOverrides.TryGetValue(prop.Name, out var overridden))
            {
                description = overridden;
            }

            fields.Add(new DocumentField(
                JsonName: prop.Name,
                ClrType: prop.PropertyType,
                ClrProperty: clrProperty,
                Description: description));
        }
        return fields;
    }

    public static string? ResolveTypeDescription<T>(DocumentAITypeRegistration<T> reg) where T : class
    {
        if (!string.IsNullOrWhiteSpace(reg.TypeDescriptionOverride))
            return reg.TypeDescriptionOverride;
        return ReadTypeDescription(reg.JsonTypeInfo.Type);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2070",
        Justification = "DescriptionAttribute and DisplayNameAttribute are sealed framework types preserved by the runtime.")]
    static string? ReadTypeDescription(Type type)
    {
        var desc = type.GetCustomAttribute<DescriptionAttribute>(inherit: false)?.Description;
        if (!string.IsNullOrWhiteSpace(desc))
            return desc;
        var displayName = type.GetCustomAttribute<DisplayNameAttribute>(inherit: false)?.DisplayName;
        return string.IsNullOrWhiteSpace(displayName) ? null : displayName;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2070",
        Justification = "DescriptionAttribute and DisplayNameAttribute are sealed framework types preserved by the runtime.")]
    static string? ReadMemberDescription(MemberInfo member)
    {
        var desc = member.GetCustomAttribute<DescriptionAttribute>(inherit: false)?.Description;
        if (!string.IsNullOrWhiteSpace(desc))
            return desc;
        var displayName = member.GetCustomAttribute<DisplayNameAttribute>(inherit: false)?.DisplayName;
        return string.IsNullOrWhiteSpace(displayName) ? null : displayName;
    }

    /// <summary>
    /// Returns the JSON-Schema "type" string (or list) for a CLR type. Booleans,
    /// numbers, strings, and dates are mapped explicitly; everything else degrades to "string".
    /// </summary>
    public static (string SchemaType, string? Format, bool Nullable) ClassifyJsonSchemaType(Type clrType)
    {
        var nullable = false;
        var underlying = Nullable.GetUnderlyingType(clrType);
        if (underlying != null)
        {
            nullable = true;
            clrType = underlying;
        }
        else if (!clrType.IsValueType)
        {
            nullable = true;
        }

        if (clrType == typeof(bool))    return ("boolean", null, nullable);
        if (clrType == typeof(string))  return ("string",  null, nullable);
        if (clrType == typeof(Guid))    return ("string",  "uuid", nullable);
        if (clrType == typeof(DateTime) || clrType == typeof(DateTimeOffset))
                                        return ("string",  "date-time", nullable);
        if (clrType == typeof(DateOnly))return ("string",  "date", nullable);
        if (clrType == typeof(TimeOnly) || clrType == typeof(TimeSpan))
                                        return ("string",  "time", nullable);

        if (clrType == typeof(byte) || clrType == typeof(sbyte) ||
            clrType == typeof(short) || clrType == typeof(ushort) ||
            clrType == typeof(int) || clrType == typeof(uint) ||
            clrType == typeof(long) || clrType == typeof(ulong))
            return ("integer", null, nullable);

        if (clrType == typeof(float) || clrType == typeof(double) || clrType == typeof(decimal))
            return ("number", null, nullable);

        // Default: opaque value, treat as string for the LLM. Property is still queryable
        // by equality/non-null on the SQL side as it's persisted as JSON.
        return ("string", null, nullable);
    }

    /// <summary>
    /// Builds a JSON Schema describing the structured filter expression accepted by query/count/aggregate.
    /// </summary>
    public static JsonObject BuildFilterSchema(IReadOnlyList<DocumentField> fields)
    {
        var fieldEnum = new JsonArray();
        foreach (var f in fields)
            fieldEnum.Add(f.JsonName);

        var fieldsDescription = new System.Text.StringBuilder();
        fieldsDescription.Append("Allowed fields: ");
        for (var i = 0; i < fields.Count; i++)
        {
            if (i > 0) fieldsDescription.Append(", ");
            fieldsDescription.Append(fields[i].JsonName);
            if (!string.IsNullOrWhiteSpace(fields[i].Description))
                fieldsDescription.Append(" (").Append(fields[i].Description).Append(')');
        }

        // Recursive schema via $defs + $ref.
        var filterDef = new JsonObject
        {
            ["type"] = "object",
            ["description"] =
                "Structured filter. Supply EITHER a leaf comparison ({field, op, value}) " +
                "OR a logical combinator ({and:[...]}, {or:[...]}, {not:{...}}). " +
                fieldsDescription.ToString(),
            ["properties"] = new JsonObject
            {
                ["field"] = new JsonObject
                {
                    ["type"] = "string",
                    ["enum"] = fieldEnum.DeepClone(),
                    ["description"] = "Field name for a leaf comparison."
                },
                ["op"] = new JsonObject
                {
                    ["type"] = "string",
                    ["enum"] = new JsonArray("eq", "ne", "gt", "gte", "lt", "lte", "contains", "startsWith", "in"),
                    ["description"] = "Comparison operator. 'in' takes an array value; 'contains'/'startsWith' require string fields."
                },
                ["value"] = new JsonObject
                {
                    ["description"] = "Comparison value. Type must be compatible with the field. Use an array for 'in'."
                },
                ["and"] = new JsonObject
                {
                    ["type"] = "array",
                    ["items"] = new JsonObject { ["$ref"] = "#/$defs/filter" },
                    ["description"] = "All sub-filters must match."
                },
                ["or"] = new JsonObject
                {
                    ["type"] = "array",
                    ["items"] = new JsonObject { ["$ref"] = "#/$defs/filter" },
                    ["description"] = "At least one sub-filter must match."
                },
                ["not"] = new JsonObject
                {
                    ["$ref"] = "#/$defs/filter",
                    ["description"] = "Negates the inner filter."
                }
            }
        };

        var schema = new JsonObject
        {
            ["$defs"] = new JsonObject { ["filter"] = filterDef },
            ["$ref"] = "#/$defs/filter"
        };
        return schema;
    }

    /// <summary>
    /// Builds the JSON Schema "properties" entry for a single document field.
    /// </summary>
    public static JsonObject BuildFieldPropertySchema(DocumentField field)
    {
        var (type, format, nullable) = ClassifyJsonSchemaType(field.ClrType);
        var node = new JsonObject();

        if (nullable)
            node["type"] = new JsonArray(type, "null");
        else
            node["type"] = type;

        if (format != null)
            node["format"] = format;
        if (!string.IsNullOrWhiteSpace(field.Description))
            node["description"] = field.Description;
        return node;
    }

    /// <summary>
    /// Serializes a <see cref="JsonObject"/> schema to a <see cref="JsonElement"/> for
    /// AIFunction.JsonSchema consumption.
    /// </summary>
    public static JsonElement ToJsonElement(JsonObject node)
    {
        // JsonNode -> JsonElement via a temp UTF-8 buffer. Avoids JsonSerializer reflection paths.
        var json = node.ToJsonString();
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }
}

readonly record struct DocumentField(string JsonName, Type ClrType, PropertyInfo? ClrProperty, string? Description);
