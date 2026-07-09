namespace Shiny.DocumentDb.Internal.Spatial;

/// <summary>
/// Segment-vs-segment intersection test (cross-product orientation + collinear on-segment check).
/// Coordinates are treated planar: X = Longitude, Y = Latitude. Ported from Shiny.Spatial.
/// </summary>
static class SegmentIntersection
{
    public static bool Intersects(GeoPoint a1, GeoPoint a2, GeoPoint b1, GeoPoint b2)
    {
        var d1 = CrossProduct(b1, b2, a1);
        var d2 = CrossProduct(b1, b2, a2);
        var d3 = CrossProduct(a1, a2, b1);
        var d4 = CrossProduct(a1, a2, b2);

        if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
            ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)))
            return true;

        if (d1 == 0 && OnSegment(b1, b2, a1)) return true;
        if (d2 == 0 && OnSegment(b1, b2, a2)) return true;
        if (d3 == 0 && OnSegment(a1, a2, b1)) return true;
        if (d4 == 0 && OnSegment(a1, a2, b2)) return true;

        return false;
    }

    static double CrossProduct(GeoPoint o, GeoPoint a, GeoPoint b)
        => (a.Longitude - o.Longitude) * (b.Latitude - o.Latitude) -
           (a.Latitude - o.Latitude) * (b.Longitude - o.Longitude);

    static bool OnSegment(GeoPoint p, GeoPoint q, GeoPoint r)
        => r.Longitude <= Math.Max(p.Longitude, q.Longitude) && r.Longitude >= Math.Min(p.Longitude, q.Longitude) &&
           r.Latitude <= Math.Max(p.Latitude, q.Latitude) && r.Latitude >= Math.Min(p.Latitude, q.Latitude);
}
