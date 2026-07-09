namespace Shiny.DocumentDb;

/// <summary>
/// OGC geometry type discriminator, matching GeoJSON type names.
/// </summary>
public enum GeoGeometryType
{
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
    GeometryCollection = 7
}
