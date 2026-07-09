using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb.Internal.Spatial;

/// <summary>
/// Ray-casting point-in-polygon test, hole-aware. Coordinates are planar (X = Longitude, Y = Latitude).
/// Ported from Shiny.Spatial.
/// </summary>
static class PointInPolygon
{
    public static bool Contains(GeoPolygon polygon, GeoPoint point)
    {
        if (!polygon.GetEnvelope().Contains(point))
            return false;

        if (!IsInsideRing(polygon.ExteriorRing, point))
            return false;

        for (var i = 0; i < polygon.InteriorRings.Count; i++)
        {
            if (IsInsideRing(polygon.InteriorRings[i], point))
                return false;
        }
        return true;
    }

    public static bool IsInsideRing(IReadOnlyList<GeoPoint> ring, GeoPoint point)
    {
        var inside = false;
        var count = ring.Count;

        for (int i = 0, j = count - 1; i < count; j = i++)
        {
            double xi = ring[i].Longitude, yi = ring[i].Latitude;
            double xj = ring[j].Longitude, yj = ring[j].Latitude;

            if (((yi > point.Latitude) != (yj > point.Latitude)) &&
                (point.Longitude < (xj - xi) * (point.Latitude - yi) / (yj - yi) + xi))
            {
                inside = !inside;
            }
        }
        return inside;
    }
}
