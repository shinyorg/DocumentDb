using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

static class JsonPropertyNameResolver
{
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
