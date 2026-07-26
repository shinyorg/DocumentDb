using System.Text.Json;
using System.Text.Json.Nodes;

namespace Shiny.DocumentDb.Internal.Spatial;

/// <summary>
/// The single GeoJSON (de)serialization point for <see cref="Geometry"/> — binding a query geometry into a
/// spatial predicate parameter, and reading one back out of a stored document body.
/// </summary>
/// <remarks>
/// Everything routes through <c>DocumentDbJsonContext</c> rather than ambient <c>JsonSerializerOptions</c>,
/// which is what keeps the spatial paths free of trim/AOT warnings: <see cref="Geometry"/> carries a
/// type-level <c>[JsonConverter]</c>, so the generated contract is a converter lookup and the caller's naming
/// policy or resolver has nothing to contribute to a shape GeoJSON already fixes. Public (like
/// <c>GeometryJsonConverter</c>) so provider packages outside the <c>InternalsVisibleTo</c> set can reach it.
/// </remarks>
public static class SpatialJson
{
    /// <summary>Serializes a geometry to a GeoJSON string.</summary>
    public static string ToGeoJson(Geometry geometry)
        => JsonSerializer.Serialize(geometry, DocumentDbJsonContext.Default.Geometry);

    /// <summary>Deserializes a GeoJSON string back to a geometry.</summary>
    public static Geometry? FromGeoJson(string json)
        => JsonSerializer.Deserialize(json, DocumentDbJsonContext.Default.Geometry);

    /// <summary>Deserializes a geometry out of an already-parsed document body member.</summary>
    public static Geometry? FromNode(JsonNode node)
        => node.Deserialize(DocumentDbJsonContext.Default.Geometry);
}
