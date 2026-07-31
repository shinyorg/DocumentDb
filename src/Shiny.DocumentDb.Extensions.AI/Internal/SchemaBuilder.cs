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
    /// <param name="fields">
    /// The LLM-visible fields, or <c>null</c> for an open (schema-free) collection that declared no fields —
    /// then the field is a free-form dot path and each leaf may carry its own <c>type</c> hint.
    /// </param>
    public static JsonObject BuildFilterSchema(IReadOnlyList<DocumentField>? fields)
    {
        // Recursive schema via $defs + $ref.
        var filterDef = new JsonObject
        {
            ["type"] = "object",
            ["description"] =
                "Structured filter. Supply EITHER a leaf comparison ({field, op, value}) " +
                "OR a logical combinator ({and:[...]}, {or:[...]}, {not:{...}}). " +
                DescribeFields(fields),
            ["properties"] = new JsonObject
            {
                ["field"] = BuildFieldSelectorSchema(fields, "Field name for a leaf comparison.", nullable: false),
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

        // Open collections have no declared leaf types, so a leaf carries its own optional hint. Without one
        // the type is inferred from the supplied value, which is right for everything except a comparison
        // whose value is a string but whose stored field is not.
        if (fields is null)
            ((JsonObject)filterDef["properties"]!)["type"] = BuildTypeHintSchema(
                "Optional type of the stored field. Defaults to the type of 'value'.");

        var schema = new JsonObject
        {
            ["$defs"] = new JsonObject { ["filter"] = filterDef },
            ["$ref"] = "#/$defs/filter"
        };
        return schema;
    }

    /// <summary>The <c>get_by_id</c> / <c>delete</c> schema — one required string id.</summary>
    public static JsonElement BuildIdSchema(string description)
    {
        var node = new JsonObject
        {
            ["type"] = "object",
            ["properties"] = new JsonObject
            {
                ["id"] = new JsonObject
                {
                    ["type"] = "string",
                    ["description"] = description
                }
            },
            ["required"] = new JsonArray("id")
        };
        return ToJsonElement(node);
    }

    /// <summary>The <c>query</c> schema — filter, sort and paging, shared by both lanes.</summary>
    public static JsonElement BuildQuerySchema(IReadOnlyList<DocumentField>? fields, int maxPageSize)
    {
        var properties = new JsonObject
        {
            ["filter"] = BuildFilterSchema(fields),
            ["orderBy"] = BuildFieldSelectorSchema(fields, "Optional field name to sort by.", nullable: true),
            ["orderDirection"] = new JsonObject
            {
                ["type"] = "string",
                ["enum"] = new JsonArray("asc", "desc"),
                ["description"] = "Sort direction. Defaults to 'asc'."
            },
            ["limit"] = new JsonObject
            {
                ["type"] = "integer",
                ["minimum"] = 1,
                ["maximum"] = maxPageSize,
                ["description"] = $"Maximum number of documents to return (1-{maxPageSize}). Defaults to 50."
            },
            ["offset"] = new JsonObject
            {
                ["type"] = "integer",
                ["minimum"] = 0,
                ["description"] = "Number of documents to skip. Defaults to 0."
            }
        };

        // Nothing pins the type of a sort key on an open collection, and an unhinted path sorts
        // lexicographically on every provider but SQLite — so the model can say what it is sorting.
        if (fields is null)
            properties["orderByType"] = BuildTypeHintSchema(
                "Optional type of the 'orderBy' field. Numeric sorts need 'number' — an unhinted path sorts as text.");

        return ToJsonElement(new JsonObject
        {
            ["type"] = "object",
            ["properties"] = properties
        });
    }

    /// <summary>The <c>count</c> schema — an optional filter.</summary>
    public static JsonElement BuildCountSchema(IReadOnlyList<DocumentField>? fields)
        => ToJsonElement(new JsonObject
        {
            ["type"] = "object",
            ["properties"] = new JsonObject { ["filter"] = BuildFilterSchema(fields) }
        });

    /// <summary>The <c>aggregate</c> schema — function, field and an optional filter.</summary>
    public static JsonElement BuildAggregateSchema(IReadOnlyList<DocumentField>? fields)
        => ToJsonElement(new JsonObject
        {
            ["type"] = "object",
            ["properties"] = new JsonObject
            {
                ["function"] = new JsonObject
                {
                    ["type"] = "string",
                    ["enum"] = new JsonArray("count", "sum", "min", "max", "avg"),
                    ["description"] = "Aggregate function. 'count' ignores 'field'; sum/min/max/avg require a numeric 'field'."
                },
                ["field"] = BuildFieldSelectorSchema(
                    fields, "Numeric field to aggregate. Required for sum/min/max/avg.", nullable: true),
                ["filter"] = BuildFilterSchema(fields)
            },
            ["required"] = new JsonArray("function")
        });

    /// <summary>The <c>insert</c> / <c>update</c> schema — one document object.</summary>
    public static JsonElement BuildDocumentSchema(IReadOnlyList<DocumentField>? fields, string? typeDescription)
    {
        JsonObject docSchema;
        if (fields is null)
        {
            // No declared shape to describe — the body is whatever the caller's collection holds.
            docSchema = new JsonObject { ["type"] = "object" };
        }
        else
        {
            var properties = new JsonObject();
            foreach (var f in fields)
                properties[f.JsonName] = BuildFieldPropertySchema(f);
            docSchema = new JsonObject
            {
                ["type"] = "object",
                ["properties"] = properties
            };
        }

        if (!string.IsNullOrWhiteSpace(typeDescription))
            docSchema["description"] = typeDescription;

        return ToJsonElement(new JsonObject
        {
            ["type"] = "object",
            ["properties"] = new JsonObject { ["document"] = docSchema },
            ["required"] = new JsonArray("document")
        });
    }

    /// <summary>
    /// A slot that names a field: an enum of the declared fields, or a free-form dot path when the
    /// collection declared none.
    /// </summary>
    static JsonObject BuildFieldSelectorSchema(IReadOnlyList<DocumentField>? fields, string description, bool nullable)
    {
        if (fields is null)
        {
            return new JsonObject
            {
                ["type"] = nullable ? new JsonArray("string", "null") : "string",
                ["description"] =
                    description +
                    " Dot-separated path into the document (e.g. 'customer.name'); letters, digits and " +
                    "underscores only."
            };
        }

        var values = new JsonArray();
        if (nullable)
            values.Add((JsonNode?)null);
        foreach (var f in fields)
            values.Add((JsonNode)JsonValue.Create(f.JsonName));

        return new JsonObject
        {
            ["type"] = nullable ? new JsonArray("string", "null") : "string",
            ["enum"] = values,
            ["description"] = description
        };
    }

    static JsonObject BuildTypeHintSchema(string description) => new()
    {
        ["type"] = new JsonArray("string", "null"),
        ["enum"] = new JsonArray(null, "string", "number", "int", "long", "double", "decimal", "bool", "date", "guid"),
        ["description"] = description
    };

    static string DescribeFields(IReadOnlyList<DocumentField>? fields)
    {
        if (fields is null)
            return "Any field in the document may be named.";

        var sb = new System.Text.StringBuilder("Allowed fields: ");
        for (var i = 0; i < fields.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append(fields[i].JsonName);
            if (!string.IsNullOrWhiteSpace(fields[i].Description))
                sb.Append(" (").Append(fields[i].Description).Append(')');
        }
        return sb.ToString();
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
