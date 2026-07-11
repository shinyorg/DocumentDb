namespace Shiny.DocumentDb.Internal.Spatial;

/// <summary>
/// Boolean-fact relate engine over the geometry model. Rather than materializing the full DE-9IM string,
/// it computes the facts the OGC predicates actually need — do the geometries intersect at all, do their
/// interiors intersect, and does one cover the other — from the ported point-in-polygon / segment-
/// intersection primitives. The named predicates in <see cref="SpatialPredicates"/> are expressed as
/// combinations of these facts.
///
/// Approximations (planar, coordinate-precision sensitive; see spatial docs): polygon-covers-polygon uses a
/// vertex-containment test, and line/line "overlaps" vs "crosses" is distinguished by whether the shared
/// intersection is collinear. Correct for the documented predicate cases.
/// </summary>
static class GeometryRelate
{
    const double Epsilon = 1e-9;

    /// <summary>Do the two geometries intersect at all (interior or boundary)?</summary>
    public static bool Intersects(Geometry a, Geometry b)
    {
        if (!a.GetEnvelope().Intersects(b.GetEnvelope()))
            return false;

        foreach (var pa in Primitives(a))
        foreach (var pb in Primitives(b))
        {
            if (PrimitiveIntersects(pa, pb))
                return true;
        }
        return false;
    }

    /// <summary>Do the interiors of the two geometries intersect (more than a boundary touch)?</summary>
    public static bool InteriorIntersects(Geometry a, Geometry b)
    {
        if (!a.GetEnvelope().Intersects(b.GetEnvelope()))
            return false;

        foreach (var pa in Primitives(a))
        foreach (var pb in Primitives(b))
        {
            if (PrimitiveInteriorIntersects(pa, pb))
                return true;
        }
        return false;
    }

    /// <summary>Does <paramref name="cover"/> cover every part of <paramref name="inner"/> (boundary allowed)?</summary>
    public static bool Covers(Geometry cover, Geometry inner)
    {
        if (!cover.GetEnvelope().Intersects(inner.GetEnvelope()))
            return false;

        foreach (var pi in Primitives(inner))
        {
            if (!PrimitiveCoveredBy(pi, cover))
                return false;
        }
        return true;
    }

    public static bool GeometryEquals(Geometry a, Geometry b)
        => Covers(a, b) && Covers(b, a);

    public static int Dimension(Geometry g)
    {
        var dim = -1;
        foreach (var p in Primitives(g))
            dim = Math.Max(dim, p.Dimension);
        return dim;
    }

    /// <summary>Do any line components of the two geometries share a collinear, overlapping segment?</summary>
    public static bool CollinearOverlap(Geometry a, Geometry b)
    {
        foreach (var pa in Primitives(a))
        {
            if (pa.Dimension != 1) continue;
            foreach (var pb in Primitives(b))
            {
                if (pb.Dimension != 1) continue;
                if (SegmentsCollinearOverlap(pa.Line!, pb.Line!))
                    return true;
            }
        }
        return false;
    }

    static bool SegmentsCollinearOverlap(IReadOnlyList<GeoPoint> a, IReadOnlyList<GeoPoint> b)
    {
        for (var i = 0; i < a.Count - 1; i++)
        for (var j = 0; j < b.Count - 1; j++)
        {
            var a1 = a[i]; var a2 = a[i + 1]; var b1 = b[j]; var b2 = b[j + 1];
            if (Math.Abs(Cross(a1, a2, b1)) > Epsilon || Math.Abs(Cross(a1, a2, b2)) > Epsilon)
                continue; // not collinear
            // Project onto the dominant axis and test for >point overlap.
            var horizontal = Math.Abs(a2.Longitude - a1.Longitude) >= Math.Abs(a2.Latitude - a1.Latitude);
            double aMin, aMax, bMin, bMax;
            if (horizontal)
            {
                aMin = Math.Min(a1.Longitude, a2.Longitude); aMax = Math.Max(a1.Longitude, a2.Longitude);
                bMin = Math.Min(b1.Longitude, b2.Longitude); bMax = Math.Max(b1.Longitude, b2.Longitude);
            }
            else
            {
                aMin = Math.Min(a1.Latitude, a2.Latitude); aMax = Math.Max(a1.Latitude, a2.Latitude);
                bMin = Math.Min(b1.Latitude, b2.Latitude); bMax = Math.Max(b1.Latitude, b2.Latitude);
            }
            if (Math.Min(aMax, bMax) - Math.Max(aMin, bMin) > Epsilon)
                return true;
        }
        return false;
    }

    // ---- primitive decomposition ------------------------------------------------------------------

    internal readonly struct Primitive
    {
        public readonly int Dimension;                 // 0 point, 1 line, 2 area
        public readonly GeoPoint Point;
        public readonly IReadOnlyList<GeoPoint>? Line; // for dim 1
        public readonly GeoPolygon? Polygon;           // for dim 2

        Primitive(int dim, GeoPoint point, IReadOnlyList<GeoPoint>? line, GeoPolygon? polygon)
        {
            this.Dimension = dim; this.Point = point; this.Line = line; this.Polygon = polygon;
        }

        public static Primitive Pt(GeoPoint p) => new(0, p, null, null);
        public static Primitive Ln(IReadOnlyList<GeoPoint> l) => new(1, default, l, null);
        public static Primitive Pg(GeoPolygon pg) => new(2, default, null, pg);
    }

    static IEnumerable<Primitive> Primitives(Geometry g)
    {
        switch (g)
        {
            case GeoPointGeometry p:
                yield return Primitive.Pt(p.Point);
                break;
            case GeoLineString ls:
                yield return Primitive.Ln(ls.Coordinates);
                break;
            case GeoPolygon pg:
                yield return Primitive.Pg(pg);
                break;
            case GeoMultiPoint mp:
                foreach (var pt in mp.Points) yield return Primitive.Pt(pt);
                break;
            case GeoMultiLineString ml:
                foreach (var l in ml.LineStrings) yield return Primitive.Ln(l.Coordinates);
                break;
            case GeoMultiPolygon mpg:
                foreach (var pg in mpg.Polygons) yield return Primitive.Pg(pg);
                break;
            case GeoGeometryCollection gc:
                foreach (var inner in gc.Geometries)
                    foreach (var prim in Primitives(inner))
                        yield return prim;
                break;
        }
    }

    // ---- primitive-level relations ----------------------------------------------------------------

    static bool PrimitiveIntersects(Primitive a, Primitive b) => (a.Dimension, b.Dimension) switch
    {
        (0, 0) => PointsEqual(a.Point, b.Point),
        (0, 1) => PointOnLine(a.Point, b.Line!),
        (1, 0) => PointOnLine(b.Point, a.Line!),
        (0, 2) => PointCoveredByPolygon(a.Point, b.Polygon!),
        (2, 0) => PointCoveredByPolygon(b.Point, a.Polygon!),
        (1, 1) => LinesIntersect(a.Line!, b.Line!),
        (1, 2) => LinePolygonIntersects(a.Line!, b.Polygon!),
        (2, 1) => LinePolygonIntersects(b.Line!, a.Polygon!),
        (2, 2) => PolygonsIntersect(a.Polygon!, b.Polygon!),
        _ => false
    };

    static bool PrimitiveInteriorIntersects(Primitive a, Primitive b) => (a.Dimension, b.Dimension) switch
    {
        (0, 0) => PointsEqual(a.Point, b.Point),
        (0, 1) => PointOnLineInterior(a.Point, b.Line!),
        (1, 0) => PointOnLineInterior(b.Point, a.Line!),
        (0, 2) => PointInPolygonStrict(a.Point, b.Polygon!),
        (2, 0) => PointInPolygonStrict(b.Point, a.Polygon!),
        (1, 1) => LinesInteriorIntersect(a.Line!, b.Line!),
        (1, 2) => LineEntersPolygon(a.Line!, b.Polygon!),
        (2, 1) => LineEntersPolygon(b.Line!, a.Polygon!),
        (2, 2) => PolygonsInteriorIntersect(a.Polygon!, b.Polygon!),
        _ => false
    };

    static bool PrimitiveCoveredBy(Primitive inner, Geometry cover)
    {
        switch (inner.Dimension)
        {
            case 0:
                return PointCoveredBy(inner.Point, cover);
            case 1:
                // Every vertex covered and every segment midpoint covered (approximation of segment coverage).
                for (var i = 0; i < inner.Line!.Count; i++)
                    if (!PointCoveredBy(inner.Line[i], cover)) return false;
                for (var i = 0; i < inner.Line.Count - 1; i++)
                    if (!PointCoveredBy(Midpoint(inner.Line[i], inner.Line[i + 1]), cover)) return false;
                return true;
            default:
                // Polygon covered by a single covering polygon: every exterior-ring vertex of the inner must
                // be covered AND no inner edge may cross the cover's boundary. The vertex test alone is a
                // false positive for a concave cover whose notch (or a hole) cuts across an inner edge between
                // two covered vertices — the edge-crossing test rules that out.
                var ring = inner.Polygon!.ExteriorRing;
                foreach (var cp in Primitives(cover))
                {
                    if (cp.Dimension != 2) continue;
                    var coverPg = cp.Polygon!;
                    var all = true;
                    for (var i = 0; i < ring.Count && all; i++)
                        all = PointCoveredByPolygon(ring[i], coverPg);
                    if (!all)
                        continue;
                    // Only a *proper* (transversal) crossing means an inner edge leaves the cover — a shared or
                    // collinear boundary edge is fine, since Covers is boundary-inclusive.
                    if (RingProperlyCrosses(ring, coverPg.ExteriorRing))
                        continue;
                    var crossesHole = false;
                    for (var h = 0; h < coverPg.InteriorRings.Count && !crossesHole; h++)
                        crossesHole = RingProperlyCrosses(ring, coverPg.InteriorRings[h]);
                    if (!crossesHole)
                        return true;
                }
                return false;
        }
    }

    static bool PointCoveredBy(GeoPoint p, Geometry cover)
    {
        foreach (var cp in Primitives(cover))
        {
            var covered = cp.Dimension switch
            {
                0 => PointsEqual(p, cp.Point),
                1 => PointOnLine(p, cp.Line!),
                2 => PointCoveredByPolygon(p, cp.Polygon!),
                _ => false
            };
            if (covered) return true;
        }
        return false;
    }

    // ---- geometric primitives ---------------------------------------------------------------------

    static bool PointsEqual(GeoPoint a, GeoPoint b)
        => Math.Abs(a.Latitude - b.Latitude) < Epsilon && Math.Abs(a.Longitude - b.Longitude) < Epsilon;

    static bool PointOnLine(GeoPoint p, IReadOnlyList<GeoPoint> line)
    {
        for (var i = 0; i < line.Count - 1; i++)
            if (DistanceToSegmentDegrees(p, line[i], line[i + 1]) < Epsilon) return true;
        return false;
    }

    static bool PointOnLineInterior(GeoPoint p, IReadOnlyList<GeoPoint> line)
    {
        // On the line but not exactly an endpoint.
        if (PointsEqual(p, line[0]) || PointsEqual(p, line[^1]))
            return false;
        return PointOnLine(p, line);
    }

    static bool PointCoveredByPolygon(GeoPoint p, GeoPolygon pg)
        => PointInPolygon.Contains(pg, p) || PointOnRing(p, pg.ExteriorRing) || HolesBoundary(p, pg);

    static bool PointInPolygonStrict(GeoPoint p, GeoPolygon pg)
        => PointInPolygon.Contains(pg, p) && !PointOnRing(p, pg.ExteriorRing) && !HolesBoundary(p, pg);

    static bool HolesBoundary(GeoPoint p, GeoPolygon pg)
    {
        for (var i = 0; i < pg.InteriorRings.Count; i++)
            if (PointOnRing(p, pg.InteriorRings[i])) return true;
        return false;
    }

    static bool PointOnRing(GeoPoint p, IReadOnlyList<GeoPoint> ring)
    {
        for (var i = 0; i < ring.Count - 1; i++)
            if (DistanceToSegmentDegrees(p, ring[i], ring[i + 1]) < Epsilon) return true;
        return false;
    }

    static bool LinesIntersect(IReadOnlyList<GeoPoint> a, IReadOnlyList<GeoPoint> b)
    {
        for (var i = 0; i < a.Count - 1; i++)
            for (var j = 0; j < b.Count - 1; j++)
                if (SegmentIntersection.Intersects(a[i], a[i + 1], b[j], b[j + 1])) return true;
        return false;
    }

    static bool LinesInteriorIntersect(IReadOnlyList<GeoPoint> a, IReadOnlyList<GeoPoint> b)
    {
        // Any crossing that is not solely a shared endpoint touch.
        for (var i = 0; i < a.Count - 1; i++)
        for (var j = 0; j < b.Count - 1; j++)
        {
            if (!SegmentIntersection.Intersects(a[i], a[i + 1], b[j], b[j + 1]))
                continue;
            if (SharesOnlyEndpoint(a[i], a[i + 1], b[j], b[j + 1]))
                continue;
            return true;
        }
        return false;
    }

    static bool SharesOnlyEndpoint(GeoPoint a1, GeoPoint a2, GeoPoint b1, GeoPoint b2)
    {
        var shared = (PointsEqual(a1, b1) ? 1 : 0) + (PointsEqual(a1, b2) ? 1 : 0)
                   + (PointsEqual(a2, b1) ? 1 : 0) + (PointsEqual(a2, b2) ? 1 : 0);
        if (shared == 0)
            return false;
        // If they share an endpoint, only a "touch" if neither segment's interior crosses the other.
        return !SegmentCrossesInterior(a1, a2, b1, b2);
    }

    // True when any edge of ring a properly (transversally) crosses an edge of ring b — i.e. passes from one
    // side to the other. Shared/collinear boundary edges and endpoint touches do NOT count.
    static bool RingProperlyCrosses(IReadOnlyList<GeoPoint> a, IReadOnlyList<GeoPoint> b)
    {
        for (var i = 0; i < a.Count - 1; i++)
            for (var j = 0; j < b.Count - 1; j++)
                if (SegmentCrossesInterior(a[i], a[i + 1], b[j], b[j + 1]))
                    return true;
        return false;
    }

    static bool SegmentCrossesInterior(GeoPoint a1, GeoPoint a2, GeoPoint b1, GeoPoint b2)
    {
        double d1 = Cross(b1, b2, a1), d2 = Cross(b1, b2, a2), d3 = Cross(a1, a2, b1), d4 = Cross(a1, a2, b2);
        return ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0));
    }

    static bool LinePolygonIntersects(IReadOnlyList<GeoPoint> line, GeoPolygon pg)
    {
        for (var i = 0; i < line.Count; i++)
            if (PointCoveredByPolygon(line[i], pg)) return true;
        for (var i = 0; i < line.Count - 1; i++)
            if (SegmentIntersectsRing(line[i], line[i + 1], pg.ExteriorRing)) return true;
        return false;
    }

    static bool LineEntersPolygon(IReadOnlyList<GeoPoint> line, GeoPolygon pg)
    {
        for (var i = 0; i < line.Count; i++)
            if (PointInPolygonStrict(line[i], pg)) return true;
        // A segment midpoint strictly inside means the line enters the interior.
        for (var i = 0; i < line.Count - 1; i++)
            if (PointInPolygonStrict(Midpoint(line[i], line[i + 1]), pg)) return true;
        return false;
    }

    static bool PolygonsIntersect(GeoPolygon a, GeoPolygon b)
    {
        for (var i = 0; i < a.ExteriorRing.Count; i++)
            if (PointCoveredByPolygon(a.ExteriorRing[i], b)) return true;
        for (var i = 0; i < b.ExteriorRing.Count; i++)
            if (PointCoveredByPolygon(b.ExteriorRing[i], a)) return true;
        for (var i = 0; i < a.ExteriorRing.Count - 1; i++)
            if (SegmentIntersectsRing(a.ExteriorRing[i], a.ExteriorRing[i + 1], b.ExteriorRing)) return true;
        return false;
    }

    static bool PolygonsInteriorIntersect(GeoPolygon a, GeoPolygon b)
    {
        for (var i = 0; i < a.ExteriorRing.Count; i++)
            if (PointInPolygonStrict(a.ExteriorRing[i], b)) return true;
        for (var i = 0; i < b.ExteriorRing.Count; i++)
            if (PointInPolygonStrict(b.ExteriorRing[i], a)) return true;
        // Edge crossings imply overlapping interiors.
        for (var i = 0; i < a.ExteriorRing.Count - 1; i++)
        for (var j = 0; j < b.ExteriorRing.Count - 1; j++)
            if (SegmentCrossesInterior(a.ExteriorRing[i], a.ExteriorRing[i + 1], b.ExteriorRing[j], b.ExteriorRing[j + 1]))
                return true;
        return false;
    }

    static bool SegmentIntersectsRing(GeoPoint a, GeoPoint b, IReadOnlyList<GeoPoint> ring)
    {
        for (var i = 0; i < ring.Count - 1; i++)
            if (SegmentIntersection.Intersects(a, b, ring[i], ring[i + 1])) return true;
        return false;
    }

    static double Cross(GeoPoint o, GeoPoint a, GeoPoint b)
        => (a.Longitude - o.Longitude) * (b.Latitude - o.Latitude) -
           (a.Latitude - o.Latitude) * (b.Longitude - o.Longitude);

    static GeoPoint Midpoint(GeoPoint a, GeoPoint b)
        => new((a.Latitude + b.Latitude) / 2, (a.Longitude + b.Longitude) / 2);

    static double DistanceToSegmentDegrees(GeoPoint p, GeoPoint a, GeoPoint b)
    {
        double dx = b.Longitude - a.Longitude, dy = b.Latitude - a.Latitude;
        var lenSq = dx * dx + dy * dy;
        if (lenSq == 0)
            return Hypot(p.Longitude - a.Longitude, p.Latitude - a.Latitude);

        var t = ((p.Longitude - a.Longitude) * dx + (p.Latitude - a.Latitude) * dy) / lenSq;
        t = Math.Max(0, Math.Min(1, t));
        return Hypot(p.Longitude - (a.Longitude + t * dx), p.Latitude - (a.Latitude + t * dy));
    }

    static double Hypot(double x, double y) => Math.Sqrt(x * x + y * y);
}
