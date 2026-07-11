using Shiny.DocumentDb.Internal.Spatial;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Phase 2 coverage: the DE-9IM-derived predicate family over the geometry model (pure functions).
/// </summary>
public class SpatialPredicateTests
{
    static GeoPolygon Square(double minLng, double minLat, double maxLng, double maxLat) =>
        new(new GeoPoint[]
        {
            new(minLat, minLng), new(minLat, maxLng), new(maxLat, maxLng), new(maxLat, minLng), new(minLat, minLng)
        });

    static GeoLineString Line(params (double lat, double lng)[] pts)
    {
        var coords = new GeoPoint[pts.Length];
        for (var i = 0; i < pts.Length; i++)
            coords[i] = new GeoPoint(pts[i].lat, pts[i].lng);
        return new GeoLineString(coords);
    }

    readonly GeoPolygon unit = Square(0, 0, 1, 1);

    [Fact]
    public void Point_In_Polygon_Intersects_And_Contains()
    {
        Geometry inside = new GeoPoint(0.5, 0.5);
        Assert.True(SpatialPredicates.Intersects(this.unit, inside));
        Assert.True(SpatialPredicates.Contains(this.unit, inside));
        Assert.True(SpatialPredicates.Within(inside, this.unit));
        Assert.False(SpatialPredicates.Disjoint(this.unit, inside));

        Geometry outside = new GeoPoint(5, 5);
        Assert.False(SpatialPredicates.Intersects(this.unit, outside));
        Assert.True(SpatialPredicates.Disjoint(this.unit, outside));
    }

    [Fact]
    public void Disjoint_Squares()
    {
        var far = Square(10, 10, 11, 11);
        Assert.True(SpatialPredicates.Disjoint(this.unit, far));
        Assert.False(SpatialPredicates.Intersects(this.unit, far));
    }

    [Fact]
    public void Overlapping_Squares()
    {
        var over = Square(0.5, 0.5, 1.5, 1.5);
        Assert.True(SpatialPredicates.Intersects(this.unit, over));
        Assert.True(SpatialPredicates.Overlaps(this.unit, over));
        Assert.False(SpatialPredicates.Contains(this.unit, over));
        Assert.False(SpatialPredicates.Touches(this.unit, over));
    }

    [Fact]
    public void Touching_Squares_Share_Edge()
    {
        var right = Square(1, 0, 2, 1); // shares the x=1 edge with unit
        Assert.True(SpatialPredicates.Intersects(this.unit, right));
        Assert.True(SpatialPredicates.Touches(this.unit, right));
        Assert.False(SpatialPredicates.Overlaps(this.unit, right));
    }

    [Fact]
    public void Contains_And_Covers_Inner_Square()
    {
        var inner = Square(0.25, 0.25, 0.75, 0.75);
        Assert.True(SpatialPredicates.Contains(this.unit, inner));
        Assert.True(SpatialPredicates.Covers(this.unit, inner));
        Assert.True(SpatialPredicates.Within(inner, this.unit));
        Assert.True(SpatialPredicates.CoveredBy(inner, this.unit));
    }

    [Fact]
    public void Covers_ConcaveNotch_CuttingInnerEdge_IsNotCovered()
    {
        // A "C" opening to the right: arms at lat<4 and lat>6, spine at lng<4, notch = lng 4..10, lat 4..6.
        var c = new GeoPolygon(new GeoPoint[]
        {
            new(0, 0), new(0, 10), new(4, 10), new(4, 4), new(6, 4), new(6, 10), new(10, 10), new(10, 0), new(0, 0)
        });
        // Inner square lng 2..6, lat 2..8: every vertex sits in an arm/spine (all covered), but the lng=6 edge
        // runs through the notch (outside the C). The vertex-only check would wrongly report Covers; the
        // edge-crossing check must reject it.
        var innerAcrossNotch = Square(2, 2, 6, 8);
        Assert.False(SpatialPredicates.Covers(c, innerAcrossNotch));

        // A square wholly inside the bottom arm is genuinely covered (no edge crosses the notch).
        var innerInArm = Square(1, 1, 3, 3);
        Assert.True(SpatialPredicates.Covers(c, innerInArm));
    }

    [Fact]
    public void Envelope_AntimeridianCrossing_WidensToFullLongitude()
    {
        // A polygon spanning lng 170 → −170 crosses the antimeridian; the naive min/max box would be the
        // wrong-way [−170, 170] that excludes the true region near ±180. The envelope must widen to the full
        // longitude range so a bbox prune never drops a valid match.
        var crossing = new GeoPolygon(new GeoPoint[]
        {
            new(0, 170), new(1, 175), new(1, -175), new(0, -170), new(0, 170)
        });
        var env = crossing.GetEnvelope();
        Assert.Equal(-180.0, env.MinLongitude);
        Assert.Equal(180.0, env.MaxLongitude);

        // A normal (non-crossing) polygon keeps a tight envelope.
        var tight = this.unit.GetEnvelope();
        Assert.Equal(0.0, tight.MinLongitude);
        Assert.Equal(1.0, tight.MaxLongitude);
    }

    [Fact]
    public void Equals_Same_Polygon()
    {
        var same = Square(0, 0, 1, 1);
        Assert.True(SpatialPredicates.GeometryEquals(this.unit, same));
        Assert.False(SpatialPredicates.GeometryEquals(this.unit, Square(0, 0, 2, 2)));
    }

    [Fact]
    public void Lines_Crossing_At_A_Point()
    {
        var a = Line((0, 0), (2, 2));
        var b = Line((0, 2), (2, 0));
        Assert.True(SpatialPredicates.Intersects(a, b));
        Assert.True(SpatialPredicates.Crosses(a, b));
        Assert.False(SpatialPredicates.Overlaps(a, b));
    }

    [Fact]
    public void Lines_Collinear_Overlap()
    {
        var a = Line((0, 0), (0, 2));
        var b = Line((0, 1), (0, 3));
        Assert.True(SpatialPredicates.Overlaps(a, b));
        Assert.False(SpatialPredicates.Crosses(a, b));
    }

    [Fact]
    public void Line_Crosses_Polygon()
    {
        var line = Line((0.5, -1), (0.5, 2)); // enters and exits the unit square
        Assert.True(SpatialPredicates.Intersects(this.unit, line));
        Assert.True(SpatialPredicates.Crosses(this.unit, line));
        Assert.False(SpatialPredicates.Contains(this.unit, line));
    }

    [Fact]
    public void MultiPolygon_Contains_Point()
    {
        Geometry mpg = new GeoMultiPolygon(new[] { Square(0, 0, 1, 1), Square(5, 5, 6, 6) });
        Geometry p = new GeoPoint(5.5, 5.5);
        Assert.True(SpatialPredicates.Intersects(mpg, p));
        Assert.True(SpatialPredicates.Contains(mpg, p));
    }
}
