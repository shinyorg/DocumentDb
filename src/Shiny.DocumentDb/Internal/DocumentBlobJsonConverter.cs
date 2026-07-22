using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Persists a <see cref="DocumentBlob"/> as <b>metadata only</b> — the payload lives in the sidecar table and
/// must never reach the document body.
/// </summary>
/// <remarks>
/// Public (and with a public parameterless constructor) because <c>DocumentSerialization.Generated</c> emits
/// <c>new DocumentBlobJsonConverter()</c> into the consuming assembly's metadata resolver.
/// <para>
/// Member names are written through <see cref="JsonSerializerOptions.PropertyNamingPolicy"/> so the names the
/// query translator computes for <c>x.Pdf.Length</c> match the names actually stored. Reads are
/// case-insensitive so a body written under one policy still loads under another.
/// </para>
/// </remarks>
public sealed class DocumentBlobJsonConverter : JsonConverter<DocumentBlob>
{
    const string KeyName = "Key";
    const string LengthName = "Length";
    const string ContentTypeName = "ContentType";
    const string FileNameName = "FileName";
    const string HashName = "Hash";
    const string CreatedAtName = "CreatedAt";
    const string UpdatedAtName = "UpdatedAt";

    public override DocumentBlob? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return null;

        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected StartObject for DocumentBlob.");

        var blob = new DocumentBlob();

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName)
                continue;

            var name = reader.GetString()!;
            reader.Read();

            if (Match(name, KeyName))
                blob.Key = reader.GetString() ?? "";
            else if (Match(name, LengthName))
                blob.Length = reader.GetInt64();
            else if (Match(name, ContentTypeName))
                blob.ContentType = reader.GetString();
            else if (Match(name, FileNameName))
                blob.FileName = reader.GetString();
            else if (Match(name, HashName))
                blob.Hash = reader.GetString();
            else if (Match(name, CreatedAtName))
                blob.CreatedAt = reader.GetDateTimeOffset();
            else if (Match(name, UpdatedAtName))
                blob.UpdatedAt = reader.GetDateTimeOffset();
            else
                reader.Skip();
        }
        return blob;
    }

    public override void Write(Utf8JsonWriter writer, DocumentBlob value, JsonSerializerOptions options)
    {
        var policy = options.PropertyNamingPolicy;

        writer.WriteStartObject();
        writer.WriteString(Name(policy, KeyName), value.Key);
        writer.WriteNumber(Name(policy, LengthName), value.Length);

        WriteOptionalString(writer, Name(policy, ContentTypeName), value.ContentType, options);
        WriteOptionalString(writer, Name(policy, FileNameName), value.FileName, options);
        WriteOptionalString(writer, Name(policy, HashName), value.Hash, options);

        writer.WriteString(Name(policy, CreatedAtName), value.CreatedAt);
        writer.WriteString(Name(policy, UpdatedAtName), value.UpdatedAt);
        writer.WriteEndObject();
    }

    static void WriteOptionalString(Utf8JsonWriter writer, string name, string? value, JsonSerializerOptions options)
    {
        if (value != null)
            writer.WriteString(name, value);
        else if (options.DefaultIgnoreCondition != JsonIgnoreCondition.WhenWritingNull)
            writer.WriteNull(name);
    }

    static string Name(JsonNamingPolicy? policy, string clrName)
        => policy == null ? clrName : policy.ConvertName(clrName);

    static bool Match(string actual, string clrName)
        => string.Equals(actual, clrName, StringComparison.OrdinalIgnoreCase);
}
