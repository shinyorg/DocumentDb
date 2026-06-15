using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Shiny.DocumentDb.CosmosDb;

/// <summary>
/// Stores a property that already holds a JSON string as a nested JSON <b>object</b> in Cosmos (not an
/// escaped string), so server-side queries can navigate into it (<c>c.data.field</c>). The Cosmos SDK
/// serializes the envelope with Newtonsoft, so this is a Newtonsoft converter: on write it emits the
/// string verbatim as raw JSON; on read it reconstitutes the nested object back into its JSON string, so
/// every consumer that treats the value as a string keeps working.
/// </summary>
internal sealed class RawJsonConverter : JsonConverter<string>
{
    public override void WriteJson(JsonWriter writer, string? value, JsonSerializer serializer)
    {
        if (string.IsNullOrEmpty(value))
            writer.WriteNull();
        else
            writer.WriteRawValue(value);
    }

    public override string? ReadJson(JsonReader reader, Type objectType, string? existingValue, bool hasExistingValue, JsonSerializer serializer)
    {
        if (reader.TokenType == JsonToken.Null)
            return null;

        var token = JToken.Load(reader);
        return token.ToString(Formatting.None);
    }
}
