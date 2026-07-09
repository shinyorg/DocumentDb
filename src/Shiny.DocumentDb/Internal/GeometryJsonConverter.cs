using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Polymorphic GeoJSON (de)serializer for the <see cref="Geometry"/> hierarchy. Reads/writes the standard
/// GeoJSON shape — <c>{"type":"...","coordinates":[...]}</c> (or <c>"geometries"</c> for collections) — with
/// coordinates in <c>[longitude, latitude]</c> order. Polygon rings are winding-normalized on write
/// (exterior CCW, holes CW) so native 2dsphere/Cosmos indexes accept every polygon.
/// </summary>
sealed class GeometryJsonConverter : JsonConverter<Geometry>
{
    public override Geometry Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        return ReadGeometry(doc.RootElement);
    }

    static Geometry ReadGeometry(JsonElement el)
    {
        if (el.ValueKind != JsonValueKind.Object || !el.TryGetProperty("type", out var typeEl))
            throw new JsonException("Expected a GeoJSON geometry object with a 'type' property.");

        var type = typeEl.GetString();
        switch (type)
        {
            case "Point":
                return new GeoPointGeometry(ReadCoordinate(el.GetProperty("coordinates")));
            case "LineString":
                return new GeoLineString(ReadCoordinates(el.GetProperty("coordinates")));
            case "Polygon":
                return ReadPolygon(el.GetProperty("coordinates"));
            case "MultiPoint":
                return new GeoMultiPoint(ReadCoordinates(el.GetProperty("coordinates")));
            case "MultiLineString":
            {
                var coords = el.GetProperty("coordinates");
                var lines = new List<GeoLineString>(coords.GetArrayLength());
                foreach (var line in coords.EnumerateArray())
                    lines.Add(new GeoLineString(ReadCoordinates(line)));
                return new GeoMultiLineString(lines);
            }
            case "MultiPolygon":
            {
                var coords = el.GetProperty("coordinates");
                var polygons = new List<GeoPolygon>(coords.GetArrayLength());
                foreach (var poly in coords.EnumerateArray())
                    polygons.Add(ReadPolygon(poly));
                return new GeoMultiPolygon(polygons);
            }
            case "GeometryCollection":
            {
                var geoms = el.GetProperty("geometries");
                var list = new List<Geometry>(geoms.GetArrayLength());
                foreach (var g in geoms.EnumerateArray())
                    list.Add(ReadGeometry(g));
                return new GeoGeometryCollection(list);
            }
            default:
                throw new JsonException($"Unsupported GeoJSON geometry type '{type}'.");
        }
    }

    static GeoPolygon ReadPolygon(JsonElement rings)
    {
        var ringArrays = new List<IReadOnlyList<GeoPoint>>(rings.GetArrayLength());
        foreach (var ring in rings.EnumerateArray())
            ringArrays.Add(ReadCoordinates(ring));

        if (ringArrays.Count == 0)
            throw new JsonException("Polygon requires at least an exterior ring.");

        var holes = ringArrays.Count > 1
            ? ringArrays.GetRange(1, ringArrays.Count - 1)
            : (IReadOnlyList<IReadOnlyList<GeoPoint>>)Array.Empty<IReadOnlyList<GeoPoint>>();
        return new GeoPolygon(ringArrays[0], holes);
    }

    static IReadOnlyList<GeoPoint> ReadCoordinates(JsonElement arr)
    {
        var list = new List<GeoPoint>(arr.GetArrayLength());
        foreach (var c in arr.EnumerateArray())
            list.Add(ReadCoordinate(c));
        return list;
    }

    static GeoPoint ReadCoordinate(JsonElement arr)
    {
        var lng = arr[0].GetDouble();
        var lat = arr[1].GetDouble();
        return new GeoPoint(lat, lng);
    }

    public override void Write(Utf8JsonWriter writer, Geometry value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case GeoPointGeometry p:
                writer.WriteStartObject();
                writer.WriteString("type", "Point");
                WriteCoordinate(writer, "coordinates", p.Point);
                writer.WriteEndObject();
                break;
            case GeoLineString ls:
                WriteSimple(writer, "LineString", ls.Coordinates);
                break;
            case GeoPolygon pg:
                writer.WriteStartObject();
                writer.WriteString("type", "Polygon");
                writer.WritePropertyName("coordinates");
                WritePolygonRings(writer, pg);
                writer.WriteEndObject();
                break;
            case GeoMultiPoint mp:
                WriteSimple(writer, "MultiPoint", mp.Points);
                break;
            case GeoMultiLineString ml:
                writer.WriteStartObject();
                writer.WriteString("type", "MultiLineString");
                writer.WriteStartArray("coordinates");
                for (var i = 0; i < ml.LineStrings.Count; i++)
                    WriteRing(writer, ml.LineStrings[i].Coordinates);
                writer.WriteEndArray();
                writer.WriteEndObject();
                break;
            case GeoMultiPolygon mpg:
                writer.WriteStartObject();
                writer.WriteString("type", "MultiPolygon");
                writer.WriteStartArray("coordinates");
                for (var i = 0; i < mpg.Polygons.Count; i++)
                    WritePolygonRings(writer, mpg.Polygons[i]);
                writer.WriteEndArray();
                writer.WriteEndObject();
                break;
            case GeoGeometryCollection gc:
                writer.WriteStartObject();
                writer.WriteString("type", "GeometryCollection");
                writer.WriteStartArray("geometries");
                for (var i = 0; i < gc.Geometries.Count; i++)
                    this.Write(writer, gc.Geometries[i], options);
                writer.WriteEndArray();
                writer.WriteEndObject();
                break;
            default:
                throw new JsonException($"Cannot serialize geometry type {value.GetType().Name}.");
        }
    }

    static void WriteSimple(Utf8JsonWriter writer, string type, IReadOnlyList<GeoPoint> coords)
    {
        writer.WriteStartObject();
        writer.WriteString("type", type);
        writer.WritePropertyName("coordinates");
        WriteRing(writer, coords);
        writer.WriteEndObject();
    }

    static void WritePolygonRings(Utf8JsonWriter writer, GeoPolygon pg)
    {
        // Winding-normalize on write: exterior CCW, holes CW.
        var repaired = (GeoPolygon)GeometryValidation.MakeValid(pg);
        writer.WriteStartArray();
        WriteRing(writer, repaired.ExteriorRing);
        for (var i = 0; i < repaired.InteriorRings.Count; i++)
            WriteRing(writer, repaired.InteriorRings[i]);
        writer.WriteEndArray();
    }

    static void WriteRing(Utf8JsonWriter writer, IReadOnlyList<GeoPoint> coords)
    {
        writer.WriteStartArray();
        for (var i = 0; i < coords.Count; i++)
            WriteCoordinateArray(writer, coords[i]);
        writer.WriteEndArray();
    }

    static void WriteCoordinate(Utf8JsonWriter writer, string propertyName, GeoPoint p)
    {
        writer.WritePropertyName(propertyName);
        WriteCoordinateArray(writer, p);
    }

    static void WriteCoordinateArray(Utf8JsonWriter writer, GeoPoint p)
    {
        writer.WriteStartArray();
        writer.WriteNumberValue(p.Longitude);
        writer.WriteNumberValue(p.Latitude);
        writer.WriteEndArray();
    }
}
