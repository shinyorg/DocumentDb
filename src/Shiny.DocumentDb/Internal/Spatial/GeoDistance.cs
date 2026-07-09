namespace Shiny.DocumentDb.Internal.Spatial;

/// <summary>
/// Great-circle distance from a point (or geometry) to a geometry, in meters. Point-to-geometry is exact to
/// the nearest edge (0 when the point is inside a polygon); geometry-to-geometry is a vertex-sampled
/// approximation adequate for distance ordering. Planar nearest-point projection + Haversine.
/// </summary>
static class GeoDistance
{
    public static double PointToGeometry(GeoPoint p, Geometry g)
    {
        switch (g)
        {
            case GeoPointGeometry pt:
                return GeoMath.HaversineDistance(p, pt.Point);
            case GeoLineString ls:
                return MinToRing(p, ls.Coordinates);
            case GeoPolygon pg:
                if (PointInPolygon.Contains(pg, p))
                    return 0;
                var d = MinToRing(p, pg.ExteriorRing);
                for (var i = 0; i < pg.InteriorRings.Count; i++)
                    d = Math.Min(d, MinToRing(p, pg.InteriorRings[i]));
                return d;
            case GeoMultiPoint mp:
                return MinOverPoints(p, mp.Points);
            case GeoMultiLineString ml:
                return MinOver(ml.LineStrings, l => PointToGeometry(p, l));
            case GeoMultiPolygon mpg:
                return MinOver(mpg.Polygons, pg => PointToGeometry(p, pg));
            case GeoGeometryCollection gc:
                return MinOver(gc.Geometries, inner => PointToGeometry(p, inner));
            default:
                return double.PositiveInfinity;
        }
    }

    public static double Between(Geometry a, Geometry b)
    {
        if (GeometryRelate.Intersects(a, b))
            return 0;

        var min = double.PositiveInfinity;
        foreach (var p in Coordinates(a))
            min = Math.Min(min, PointToGeometry(p, b));
        foreach (var p in Coordinates(b))
            min = Math.Min(min, PointToGeometry(p, a));
        return min;
    }

    static double MinToRing(GeoPoint p, IReadOnlyList<GeoPoint> ring)
    {
        var min = double.PositiveInfinity;
        for (var i = 0; i < ring.Count - 1; i++)
            min = Math.Min(min, GeoMath.HaversineDistance(p, NearestOnSegment(p, ring[i], ring[i + 1])));
        if (ring.Count == 1)
            min = GeoMath.HaversineDistance(p, ring[0]);
        return min;
    }

    static double MinOverPoints(GeoPoint p, IReadOnlyList<GeoPoint> pts)
    {
        var min = double.PositiveInfinity;
        for (var i = 0; i < pts.Count; i++)
            min = Math.Min(min, GeoMath.HaversineDistance(p, pts[i]));
        return min;
    }

    static double MinOver<T>(IReadOnlyList<T> items, Func<T, double> selector)
    {
        var min = double.PositiveInfinity;
        for (var i = 0; i < items.Count; i++)
            min = Math.Min(min, selector(items[i]));
        return min;
    }

    static GeoPoint NearestOnSegment(GeoPoint p, GeoPoint a, GeoPoint b)
    {
        double dx = b.Longitude - a.Longitude, dy = b.Latitude - a.Latitude;
        var lenSq = dx * dx + dy * dy;
        if (lenSq == 0)
            return a;

        var t = ((p.Longitude - a.Longitude) * dx + (p.Latitude - a.Latitude) * dy) / lenSq;
        t = Math.Max(0, Math.Min(1, t));
        return new GeoPoint(a.Latitude + t * dy, a.Longitude + t * dx);
    }

    static IEnumerable<GeoPoint> Coordinates(Geometry g)
    {
        switch (g)
        {
            case GeoPointGeometry p: yield return p.Point; break;
            case GeoLineString ls: foreach (var c in ls.Coordinates) yield return c; break;
            case GeoPolygon pg:
                foreach (var c in pg.ExteriorRing) yield return c;
                for (var i = 0; i < pg.InteriorRings.Count; i++)
                    foreach (var c in pg.InteriorRings[i]) yield return c;
                break;
            case GeoMultiPoint mp: foreach (var c in mp.Points) yield return c; break;
            case GeoMultiLineString ml:
                foreach (var l in ml.LineStrings) foreach (var c in Coordinates(l)) yield return c;
                break;
            case GeoMultiPolygon mpg:
                foreach (var pg in mpg.Polygons) foreach (var c in Coordinates(pg)) yield return c;
                break;
            case GeoGeometryCollection gc:
                foreach (var inner in gc.Geometries) foreach (var c in Coordinates(inner)) yield return c;
                break;
        }
    }
}
