using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Serializes a <see cref="DocumentBlobCollection"/> as a JSON array of blob <b>metadata</b> — each element
/// goes through <see cref="DocumentBlobJsonConverter"/>, so payloads never reach the document body.
/// </summary>
/// <remarks>
/// Public with a public parameterless constructor because <c>DocumentSerialization.Generated</c> emits
/// <c>new DocumentBlobCollectionJsonConverter()</c> into the consuming assembly's metadata resolver — where,
/// without the type-level converter, a non-<c>List&lt;T&gt;</c> collection would otherwise be rejected outright.
/// </remarks>
public sealed class DocumentBlobCollectionJsonConverter : JsonConverter<DocumentBlobCollection>
{
    public override DocumentBlobCollection? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return null;
        if (reader.TokenType != JsonTokenType.StartArray)
            throw new JsonException("Expected StartArray for DocumentBlobCollection.");

        var collection = new DocumentBlobCollection();
        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
        {
            var blob = JsonSerializer.Deserialize(ref reader, DocumentDbJsonContext.Default.DocumentBlob);
            if (blob != null)
                collection.Add(blob);
        }
        return collection;
    }

    public override void Write(Utf8JsonWriter writer, DocumentBlobCollection value, JsonSerializerOptions options)
    {
        writer.WriteStartArray();
        foreach (var blob in value)
            JsonSerializer.Serialize(writer, blob, DocumentDbJsonContext.Default.DocumentBlob);
        writer.WriteEndArray();
    }
}
