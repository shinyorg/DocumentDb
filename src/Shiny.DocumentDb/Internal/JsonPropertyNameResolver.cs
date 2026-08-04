using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

static class JsonPropertyNameResolver
{
    /// <summary>
    /// Resolves the stored JSON name for a mapped CLR property. Prefers the resolved
    /// <see cref="JsonTypeInfo"/> so source-generated (metadata-mode) contexts and
    /// <c>[JsonPropertyName]</c> are honored — those bake the property name at generation time and
    /// ignore the runtime naming policy. Falls back to the naming policy when the type has no
    /// resolvable metadata.
    /// </summary>
    public static string ResolveJsonName(JsonSerializerOptions jsonOptions, Type documentType, string propertyName)
    {
        try
        {
            var typeInfo = jsonOptions.GetTypeInfo(documentType);
            return ResolveProperty(typeInfo, propertyName).JsonName;
        }
        catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException)
        {
            // GetTypeInfo throws InvalidOperationException when no TypeInfoResolver is configured, and
            // NotSupportedException when a resolver exists but doesn't carry this type (e.g. a source-gen
            // context that omits it). In both cases there's no metadata to read — fall back to the raw
            // naming policy.
            return jsonOptions.PropertyNamingPolicy?.ConvertName(propertyName) ?? propertyName;
        }
    }

    public static (string JsonName, Type PropertyType) ResolveProperty(JsonTypeInfo typeInfo, string clrPropertyName)
    {
        foreach (var prop in typeInfo.Properties)
        {
            if (prop.AttributeProvider is MemberInfo member && member.Name == clrPropertyName)
                return (prop.Name, prop.PropertyType);
        }

        // Fallback: apply naming policy to the CLR name
        var jsonName = typeInfo.Options.PropertyNamingPolicy?.ConvertName(clrPropertyName) ?? clrPropertyName;
        return (jsonName, typeof(object));
    }

    public static string BuildJsonPath(JsonSerializerOptions options, JsonTypeInfo rootTypeInfo, List<string> clrPropertyChain)
    {
        var segments = new List<string>(clrPropertyChain.Count);
        var currentTypeInfo = rootTypeInfo;

        for (var i = 0; i < clrPropertyChain.Count; i++)
        {
            var (jsonName, propertyType) = ResolveProperty(currentTypeInfo, clrPropertyChain[i]);
            segments.Add(jsonName);

            if (i < clrPropertyChain.Count - 1)
            {
                currentTypeInfo = options.GetTypeInfo(propertyType);
            }
        }

        return string.Join('.', segments);
    }
}
