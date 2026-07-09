using System.Text.Json;

namespace Shiny.DocumentDb.Internal.Spatial;

/// <summary>Serializes a query geometry to GeoJSON for binding as a spatial-predicate parameter.</summary>
static class SpatialJson
{
    static readonly JsonSerializerOptions Options = new();

    // Geometry carries [JsonConverter(GeometryJsonConverter)]; serializing at the base type keeps it polymorphic.
    public static string ToGeoJson(Geometry geometry) => JsonSerializer.Serialize(geometry, typeof(Geometry), Options);
}
