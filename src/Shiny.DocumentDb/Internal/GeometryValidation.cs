using Shiny.DocumentDb.Internal.Spatial;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Lightweight geometry validity + best-effort repair. Not full OGC validity (no noding/polygonization).
/// </summary>
static class GeometryValidation
{
    public static bool IsValid(Geometry g)
    {
        switch (g)
        {
            case GeoPointGeometry p:
                return IsFinite(p.Point);
            case GeoLineString ls:
                return ls.Coordinates.Count >= 2 && AllFinite(ls.Coordinates) && !AllEqual(ls.Coordinates);
            case GeoPolygon pg:
                return IsValidRing(pg.ExteriorRing) && AllRingsValid(pg.InteriorRings) && IsSimpleRing(pg.ExteriorRing);
            case GeoMultiPoint mp:
                return AllFinite(mp.Points);
            case GeoMultiLineString ml:
                return All(ml.LineStrings, IsValid);
            case GeoMultiPolygon mpg:
                return All(mpg.Polygons, IsValid);
            case GeoGeometryCollection gc:
                return All(gc.Geometries, IsValid);
            default:
                return false;
        }
    }

    public static bool IsSimple(Geometry g) => g switch
    {
        GeoLineString ls => IsSimpleRing(ls.Coordinates),
        GeoPolygon pg => IsSimpleRing(pg.ExteriorRing),
        GeoMultiLineString ml => All(ml.LineStrings, IsSimple),
        GeoMultiPolygon mpg => All(mpg.Polygons, IsSimple),
        GeoGeometryCollection gc => All(gc.Geometries, IsSimple),
        _ => true
    };

    public static Geometry MakeValid(Geometry g)
    {
        switch (g)
        {
            case GeoLineString ls:
                return new GeoLineString(Dedupe(ls.Coordinates));
            case GeoPolygon pg:
                return RepairPolygon(pg);
            case GeoMultiLineString ml:
                return new GeoMultiLineString(Map(ml.LineStrings, l => (GeoLineString)MakeValid(l)));
            case GeoMultiPolygon mpg:
                return new GeoMultiPolygon(Map(mpg.Polygons, p => (GeoPolygon)MakeValid(p)));
            case GeoGeometryCollection gc:
                return new GeoGeometryCollection(Map(gc.Geometries, MakeValid));
            default:
                return g;
        }
    }

    static GeoPolygon RepairPolygon(GeoPolygon pg)
    {
        var exterior = NormalizeRing(pg.ExteriorRing, wantCounterClockwise: true);
        var holes = new List<IReadOnlyList<GeoPoint>>(pg.InteriorRings.Count);
        for (var i = 0; i < pg.InteriorRings.Count; i++)
        {
            var hole = NormalizeRing(pg.InteriorRings[i], wantCounterClockwise: false);
            if (hole.Count >= 4)
                holes.Add(hole);
        }
        return new GeoPolygon(exterior, holes);
    }

    // Dedupe consecutive points, close the ring, and orient it to the requested winding.
    static IReadOnlyList<GeoPoint> NormalizeRing(IReadOnlyList<GeoPoint> ring, bool wantCounterClockwise)
    {
        var pts = Dedupe(ring);
        if (pts.Count > 0 && pts[0] != pts[^1])
            pts.Add(pts[0]);

        if (pts.Count >= 4 && IsCounterClockwise(pts) != wantCounterClockwise)
            pts.Reverse();

        return pts;
    }

    static List<GeoPoint> Dedupe(IReadOnlyList<GeoPoint> pts)
    {
        var result = new List<GeoPoint>(pts.Count);
        for (var i = 0; i < pts.Count; i++)
        {
            if (result.Count == 0 || result[^1] != pts[i])
                result.Add(pts[i]);
        }
        return result;
    }

    // Signed planar area (shoelace); positive => counter-clockwise in (lng, lat) space.
    static bool IsCounterClockwise(IReadOnlyList<GeoPoint> ring)
    {
        var sum = 0.0;
        for (var i = 0; i < ring.Count - 1; i++)
        {
            var a = ring[i];
            var b = ring[i + 1];
            sum += (b.Longitude - a.Longitude) * (b.Latitude + a.Latitude);
        }
        return sum < 0;
    }

    static bool IsValidRing(IReadOnlyList<GeoPoint> ring)
        => ring.Count >= 4 && AllFinite(ring) && ring[0] == ring[^1];

    static bool AllRingsValid(IReadOnlyList<IReadOnlyList<GeoPoint>> rings)
    {
        for (var i = 0; i < rings.Count; i++)
        {
            if (!IsValidRing(rings[i]))
                return false;
        }
        return true;
    }

    // No non-adjacent edge crossings.
    static bool IsSimpleRing(IReadOnlyList<GeoPoint> ring)
    {
        var n = ring.Count;
        for (var i = 0; i < n - 1; i++)
        {
            for (var j = i + 1; j < n - 1; j++)
            {
                if (j == i || j == i + 1 || i == j + 1)
                    continue;
                // Skip the shared-endpoint case of the closing edge meeting the first edge.
                if (i == 0 && j == n - 2 && ring[0] == ring[^1])
                    continue;
                if (SegmentIntersection.Intersects(ring[i], ring[i + 1], ring[j], ring[j + 1]))
                    return false;
            }
        }
        return true;
    }

    static bool IsFinite(GeoPoint p)
        => double.IsFinite(p.Latitude) && double.IsFinite(p.Longitude) &&
           p.Latitude is >= -90 and <= 90 && p.Longitude is >= -180 and <= 180;

    static bool AllFinite(IReadOnlyList<GeoPoint> pts)
    {
        for (var i = 0; i < pts.Count; i++)
        {
            if (!IsFinite(pts[i]))
                return false;
        }
        return true;
    }

    static bool AllEqual(IReadOnlyList<GeoPoint> pts)
    {
        for (var i = 1; i < pts.Count; i++)
        {
            if (pts[i] != pts[0])
                return false;
        }
        return true;
    }

    static bool All<T>(IReadOnlyList<T> items, Func<T, bool> predicate)
    {
        for (var i = 0; i < items.Count; i++)
        {
            if (!predicate(items[i]))
                return false;
        }
        return true;
    }

    static IReadOnlyList<TOut> Map<TIn, TOut>(IReadOnlyList<TIn> items, Func<TIn, TOut> selector)
    {
        var result = new List<TOut>(items.Count);
        for (var i = 0; i < items.Count; i++)
            result.Add(selector(items[i]));
        return result;
    }
}
