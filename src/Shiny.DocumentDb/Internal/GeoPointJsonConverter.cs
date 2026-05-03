using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Serializes GeoPoint as GeoJSON: {"type":"Point","coordinates":[longitude,latitude]}
/// </summary>
sealed class GeoPointJsonConverter : JsonConverter<GeoPoint>
{
    public override GeoPoint Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected StartObject for GeoPoint.");

        double lat = 0, lng = 0;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return new GeoPoint(lat, lng);

            if (reader.TokenType != JsonTokenType.PropertyName)
                continue;

            var propertyName = reader.GetString();
            reader.Read();

            if (string.Equals(propertyName, "coordinates", StringComparison.OrdinalIgnoreCase))
            {
                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException("Expected array for coordinates.");

                reader.Read();
                lng = reader.GetDouble();
                reader.Read();
                lat = reader.GetDouble();
                reader.Read(); // EndArray
            }
        }

        throw new JsonException("Unexpected end of JSON for GeoPoint.");
    }

    public override void Write(Utf8JsonWriter writer, GeoPoint value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("type", "Point");
        writer.WriteStartArray("coordinates");
        writer.WriteNumberValue(value.Longitude);
        writer.WriteNumberValue(value.Latitude);
        writer.WriteEndArray();
        writer.WriteEndObject();
    }
}
