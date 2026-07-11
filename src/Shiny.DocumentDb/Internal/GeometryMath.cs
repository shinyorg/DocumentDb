namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Measurement + envelope helpers for the geometry model. Areas/lengths are geodesic approximations
/// (spherical earth) consistent with the Haversine distance primitive; see the spatial docs.
/// </summary>
static class GeometryMath
{
    // Mean earth radius for lengths (matches GeoMath.HaversineDistance) and the standard geodesic
    // radius for the spherical polygon-area approximation.
    const double AreaEarthRadiusMeters = 6_378_137.0;

    public static GeoBoundingBox Envelope(IReadOnlyList<GeoPoint> pts)
    {
        if (pts.Count == 0)
            return new GeoBoundingBox(0, 0, 0, 0);

        double minLat = double.MaxValue, minLng = double.MaxValue, maxLat = double.MinValue, maxLng = double.MinValue;
        for (var i = 0; i < pts.Count; i++)
        {
            var p = pts[i];
            if (p.Latitude < minLat) minLat = p.Latitude;
            if (p.Latitude > maxLat) maxLat = p.Latitude;
            if (p.Longitude < minLng) minLng = p.Longitude;
            if (p.Longitude > maxLng) maxLng = p.Longitude;
        }
        return WidenIfAntimeridian(minLat, minLng, maxLat, maxLng);
    }

    // When the longitude span exceeds 180°, the geometry almost certainly wraps the antimeridian (170° and
    // −170° are 20° apart across ±180, not 340°). The naive min/max box is then the wrong-way envelope that
    // *excludes* the true region near ±180, so a bbox prune would drop valid matches. Widen to the full
    // longitude range — a safe over-approximation; the exact in-process refine still filters the candidates.
    static GeoBoundingBox WidenIfAntimeridian(double minLat, double minLng, double maxLat, double maxLng)
        => maxLng - minLng > 180.0
            ? new GeoBoundingBox(minLat, -180.0, maxLat, 180.0)
            : new GeoBoundingBox(minLat, minLng, maxLat, maxLng);

    public static GeoBoundingBox Envelope<T>(IReadOnlyList<T> geometries) where T : Geometry
        => EnvelopeOfGeometries(geometries);

    public static GeoBoundingBox EnvelopeOfGeometries<T>(IReadOnlyList<T> geometries) where T : Geometry
    {
        if (geometries.Count == 0)
            return new GeoBoundingBox(0, 0, 0, 0);

        double minLat = double.MaxValue, minLng = double.MaxValue, maxLat = double.MinValue, maxLng = double.MinValue;
        for (var i = 0; i < geometries.Count; i++)
        {
            var e = geometries[i].GetEnvelope();
            if (e.MinLatitude < minLat) minLat = e.MinLatitude;
            if (e.MaxLatitude > maxLat) maxLat = e.MaxLatitude;
            if (e.MinLongitude < minLng) minLng = e.MinLongitude;
            if (e.MaxLongitude > maxLng) maxLng = e.MaxLongitude;
        }
        return WidenIfAntimeridian(minLat, minLng, maxLat, maxLng);
    }

    public static int NumGeometries(Geometry g) => g switch
    {
        GeoMultiPoint mp => mp.Points.Count,
        GeoMultiLineString ml => ml.LineStrings.Count,
        GeoMultiPolygon mpg => mpg.Polygons.Count,
        GeoGeometryCollection gc => gc.Geometries.Count,
        _ => 1
    };

    public static int NumPoints(Geometry g) => g switch
    {
        GeoPointGeometry => 1,
        GeoLineString ls => ls.Coordinates.Count,
        GeoPolygon pg => RingPointCount(pg),
        GeoMultiPoint mp => mp.Points.Count,
        GeoMultiLineString ml => Sum(ml.LineStrings, NumPoints),
        GeoMultiPolygon mpg => Sum(mpg.Polygons, NumPoints),
        GeoGeometryCollection gc => Sum(gc.Geometries, NumPoints),
        _ => 0
    };

    public static double Length(Geometry g) => g switch
    {
        GeoLineString ls => RingLength(ls.Coordinates),
        GeoPolygon pg => RingLength(pg.ExteriorRing),
        GeoMultiLineString ml => Sum(ml.LineStrings, Length),
        GeoMultiPolygon mpg => Sum(mpg.Polygons, Length),
        GeoGeometryCollection gc => Sum(gc.Geometries, Length),
        _ => 0.0
    };

    public static double Area(Geometry g) => g switch
    {
        GeoPolygon pg => PolygonArea(pg),
        GeoMultiPolygon mpg => Sum(mpg.Polygons, Area),
        GeoGeometryCollection gc => Sum(gc.Geometries, Area),
        _ => 0.0
    };

    public static GeoPoint Centroid(Geometry g) => g switch
    {
        GeoPointGeometry p => p.Point,
        GeoPolygon pg => PolygonCentroid(pg),
        _ => AverageCoordinate(g)
    };

    static int RingPointCount(GeoPolygon pg)
    {
        var n = pg.ExteriorRing.Count;
        for (var i = 0; i < pg.InteriorRings.Count; i++)
            n += pg.InteriorRings[i].Count;
        return n;
    }

    static double RingLength(IReadOnlyList<GeoPoint> ring)
    {
        var total = 0.0;
        for (var i = 0; i < ring.Count - 1; i++)
            total += GeoMath.HaversineDistance(ring[i], ring[i + 1]);
        return total;
    }

    static double PolygonArea(GeoPolygon pg)
    {
        var area = Math.Abs(RingArea(pg.ExteriorRing));
        for (var i = 0; i < pg.InteriorRings.Count; i++)
            area -= Math.Abs(RingArea(pg.InteriorRings[i]));
        return area < 0 ? 0 : area;
    }

    // Spherical polygon area approximation (signed). Standard formula used by geojson-area et al.
    static double RingArea(IReadOnlyList<GeoPoint> ring)
    {
        var n = ring.Count;
        if (n < 3)
            return 0.0;

        var sum = 0.0;
        for (var i = 0; i < n; i++)
        {
            var p1 = ring[i];
            var p2 = ring[(i + 1) % n];
            sum += ToRadians(p2.Longitude - p1.Longitude) *
                   (2 + Math.Sin(ToRadians(p1.Latitude)) + Math.Sin(ToRadians(p2.Latitude)));
        }
        return sum * AreaEarthRadiusMeters * AreaEarthRadiusMeters / 2.0;
    }

    static GeoPoint PolygonCentroid(GeoPolygon pg)
    {
        // Planar shoelace centroid over the exterior ring; falls back to vertex average for degenerate rings.
        var ring = pg.ExteriorRing;
        double a = 0, cx = 0, cy = 0;
        for (var i = 0; i < ring.Count - 1; i++)
        {
            double x0 = ring[i].Longitude, y0 = ring[i].Latitude;
            double x1 = ring[i + 1].Longitude, y1 = ring[i + 1].Latitude;
            var cross = x0 * y1 - x1 * y0;
            a += cross;
            cx += (x0 + x1) * cross;
            cy += (y0 + y1) * cross;
        }
        if (a == 0)
            return AverageRing(ring);

        a *= 0.5;
        return new GeoPoint(cy / (6 * a), cx / (6 * a));
    }

    static GeoPoint AverageCoordinate(Geometry g)
    {
        double latSum = 0, lngSum = 0;
        var count = 0;
        Accumulate(g, ref latSum, ref lngSum, ref count);
        return count == 0 ? new GeoPoint(0, 0) : new GeoPoint(latSum / count, lngSum / count);
    }

    static void Accumulate(Geometry g, ref double latSum, ref double lngSum, ref int count)
    {
        switch (g)
        {
            case GeoPointGeometry p:
                latSum += p.Point.Latitude; lngSum += p.Point.Longitude; count++;
                break;
            case GeoLineString ls:
                AccumulatePoints(ls.Coordinates, ref latSum, ref lngSum, ref count);
                break;
            case GeoPolygon pg:
                AccumulatePoints(pg.ExteriorRing, ref latSum, ref lngSum, ref count);
                break;
            case GeoMultiPoint mp:
                AccumulatePoints(mp.Points, ref latSum, ref lngSum, ref count);
                break;
            case GeoMultiLineString ml:
                for (var i = 0; i < ml.LineStrings.Count; i++)
                    Accumulate(ml.LineStrings[i], ref latSum, ref lngSum, ref count);
                break;
            case GeoMultiPolygon mpg:
                for (var i = 0; i < mpg.Polygons.Count; i++)
                    Accumulate(mpg.Polygons[i], ref latSum, ref lngSum, ref count);
                break;
            case GeoGeometryCollection gc:
                for (var i = 0; i < gc.Geometries.Count; i++)
                    Accumulate(gc.Geometries[i], ref latSum, ref lngSum, ref count);
                break;
        }
    }

    static void AccumulatePoints(IReadOnlyList<GeoPoint> pts, ref double latSum, ref double lngSum, ref int count)
    {
        for (var i = 0; i < pts.Count; i++)
        {
            latSum += pts[i].Latitude;
            lngSum += pts[i].Longitude;
            count++;
        }
    }

    static GeoPoint AverageRing(IReadOnlyList<GeoPoint> ring)
    {
        double lat = 0, lng = 0;
        var count = 0;
        AccumulatePoints(ring, ref lat, ref lng, ref count);
        return count == 0 ? new GeoPoint(0, 0) : new GeoPoint(lat / count, lng / count);
    }

    static double Sum<T>(IReadOnlyList<T> items, Func<T, double> selector)
    {
        var total = 0.0;
        for (var i = 0; i < items.Count; i++)
            total += selector(items[i]);
        return total;
    }

    static int Sum<T>(IReadOnlyList<T> items, Func<T, int> selector)
    {
        var total = 0;
        for (var i = 0; i < items.Count; i++)
            total += selector(items[i]);
        return total;
    }

    static double ToRadians(double degrees) => degrees * (Math.PI / 180.0);
}
