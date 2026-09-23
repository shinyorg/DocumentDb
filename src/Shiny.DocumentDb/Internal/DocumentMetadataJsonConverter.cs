using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Serializes a <see cref="DocumentMetadata"/> outside the store (an API response, a cache entry) as
/// <c>{ "createdAt": …, "updatedAt": …, "tenantId": … }</c> (<c>tenantId</c> only when set). The store itself never persists it — the body is stripped before
/// it is written — so this only decides what the object looks like everywhere else.
/// </summary>
/// <remarks>
/// Public with a public parameterless constructor because <c>DocumentSerialization.Generated</c> emits
/// <c>new DocumentMetadataJsonConverter()</c> into the consuming assembly's metadata resolver, and a
/// <c>JsonSerializerContext</c> honours the type-level attribute without the user registering the type.
/// </remarks>
public sealed class DocumentMetadataJsonConverter : JsonConverter<DocumentMetadata>
{
    const string CreatedAtName = "CreatedAt";
    const string UpdatedAtName = "UpdatedAt";
    const string TenantIdName = "TenantId";

    public override DocumentMetadata? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return null;

        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected StartObject for DocumentMetadata.");

        var metadata = new DocumentMetadata();
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType == JsonTokenType.PropertyName)
            {
                var name = reader.GetString()!;
                reader.Read();

                if (reader.TokenType == JsonTokenType.String && Match(name, CreatedAtName))
                    metadata.CreatedAt = reader.GetDateTimeOffset();
                else if (reader.TokenType == JsonTokenType.String && Match(name, UpdatedAtName))
                    metadata.UpdatedAt = reader.GetDateTimeOffset();
                else if (reader.TokenType == JsonTokenType.String && Match(name, TenantIdName))
                    metadata.TenantId = reader.GetString();
                else
                    reader.Skip();
            }
        }
        return metadata;
    }

    public override void Write(Utf8JsonWriter writer, DocumentMetadata value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString(Name(CreatedAtName, options), value.CreatedAt);
        writer.WriteString(Name(UpdatedAtName, options), value.UpdatedAt);
        if (value.TenantId != null)
            writer.WriteString(Name(TenantIdName, options), value.TenantId);
        writer.WriteEndObject();
    }

    static string Name(string clrName, JsonSerializerOptions options)
        => options.PropertyNamingPolicy?.ConvertName(clrName) ?? clrName;

    static bool Match(string name, string clrName)
        => string.Equals(name, clrName, StringComparison.OrdinalIgnoreCase);
}
