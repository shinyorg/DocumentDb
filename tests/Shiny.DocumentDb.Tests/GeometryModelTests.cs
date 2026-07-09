using System.Text.Json;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Phase 1 coverage: geometry model, GeoJSON roundtrip, envelope, measurement + validity accessors.
/// </summary>
public class GeometryModelTests
{
    static readonly GeoPoint[] Square =
    {
        new(0, 0), new(0, 1), new(1, 1), new(1, 0), new(0, 0)
    };

    static string ToJson(Geometry g) => JsonSerializer.Serialize(g);
    static Geometry FromJson(string json) => JsonSerializer.Deserialize<Geometry>(json)!;

    [Fact]
    public void LineString_Roundtrips()
    {
        var ls = new GeoLineString(new GeoPoint[] { new(10, 20), new(11, 21), new(12, 22) });
        var back = (GeoLineString)FromJson(ToJson(ls));

        Assert.Equal(GeoGeometryType.LineString, back.GeometryType);
        Assert.Equal(3, back.Coordinates.Count);
        Assert.Equal(new GeoPoint(10, 20), back.Coordinates[0]);
        Assert.Equal(new GeoPoint(12, 22), back.Coordinates[2]);
    }

    [Fact]
    public void GeoJson_Uses_LongitudeLatitude_Order()
    {
        var ls = new GeoLineString(new GeoPoint[] { new(Latitude: 40, Longitude: -75), new(41, -74) });
        var json = ToJson(ls);
        // First coordinate must serialize as [lng, lat] = [-75, 40]
        Assert.Contains("[-75,40]", json.Replace(" ", ""));
    }

    [Fact]
    public void Polygon_With_Hole_Roundtrips()
    {
        var hole = new GeoPoint[] { new(0.25, 0.25), new(0.25, 0.75), new(0.75, 0.75), new(0.75, 0.25), new(0.25, 0.25) };
        var pg = new GeoPolygon(Square, new IReadOnlyList<GeoPoint>[] { hole });
        var back = (GeoPolygon)FromJson(ToJson(pg));

        Assert.Equal(GeoGeometryType.Polygon, back.GeometryType);
        Assert.Equal(5, back.ExteriorRing.Count);
        Assert.Single(back.InteriorRings);
        Assert.Equal(5, back.InteriorRings[0].Count);
    }

    [Fact]
    public void MultiPolygon_And_Collection_Roundtrip()
    {
        var pg = new GeoPolygon(Square);
        var mpg = new GeoMultiPolygon(new[] { pg });
        var gc = new GeoGeometryCollection(new Geometry[] { new GeoLineString(new GeoPoint[] { new(0, 0), new(1, 1) }), pg });

        Assert.Equal(GeoGeometryType.MultiPolygon, FromJson(ToJson(mpg)).GeometryType);
        var backGc = (GeoGeometryCollection)FromJson(ToJson(gc));
        Assert.Equal(2, backGc.Geometries.Count);
        Assert.Equal(GeoGeometryType.LineString, backGc.Geometries[0].GeometryType);
        Assert.Equal(GeoGeometryType.Polygon, backGc.Geometries[1].GeometryType);
    }

    [Fact]
    public void Point_Roundtrips_Via_Implicit_Geometry()
    {
        Geometry g = new GeoPoint(45, -122);
        var back = FromJson(ToJson(g));
        Assert.Equal(GeoGeometryType.Point, back.GeometryType);
        Assert.Equal(new GeoPoint(45, -122), back.Centroid);
    }

    [Fact]
    public void Envelope_Covers_All_Coordinates()
    {
        var pg = new GeoPolygon(Square);
        var e = pg.GetEnvelope();
        Assert.Equal(0, e.MinLatitude);
        Assert.Equal(0, e.MinLongitude);
        Assert.Equal(1, e.MaxLatitude);
        Assert.Equal(1, e.MaxLongitude);
    }

    [Fact]
    public void Measurement_Accessors()
    {
        var pg = new GeoPolygon(Square);
        // ~1° x 1° near the equator ≈ 12,300 km²; assert order of magnitude.
        Assert.InRange(pg.Area, 1.0e10, 1.6e10);
        Assert.True(pg.Perimeter > 0);
        Assert.Equal(5, pg.NumPoints);
        Assert.Equal(1, pg.NumGeometries);

        var ls = new GeoLineString(new GeoPoint[] { new(0, 0), new(0, 1) });
        Assert.Equal(0.0, ls.Area);
        Assert.True(ls.Length > 100_000); // ~111 km for 1° of longitude at equator

        var mp = new GeoMultiPoint(new GeoPoint[] { new(0, 0), new(1, 1), new(2, 2) });
        Assert.Equal(3, mp.NumGeometries);
        Assert.Equal(3, mp.NumPoints);
    }

    [Fact]
    public void Centroid_Of_Square_Is_Center()
    {
        var c = new GeoPolygon(Square).Centroid;
        Assert.Equal(0.5, c.Latitude, 6);
        Assert.Equal(0.5, c.Longitude, 6);
    }

    [Fact]
    public void Validity_And_Repair()
    {
        // Closed, non-self-intersecting square is valid.
        Assert.True(new GeoPolygon(Square).IsValid);

        // Unclosed 4-point ring is invalid; MakeValid closes it.
        var unclosed = new GeoPoint[] { new(0, 0), new(0, 1), new(1, 1), new(1, 0) };
        var pg = new GeoPolygon(unclosed);
        Assert.False(pg.IsValid);
        var fixedPg = (GeoPolygon)pg.MakeValid();
        Assert.True(fixedPg.IsValid);
        Assert.Equal(fixedPg.ExteriorRing[0], fixedPg.ExteriorRing[^1]);

        // Bowtie self-intersection is not simple.
        var bowtie = new GeoPolygon(new GeoPoint[] { new(0, 0), new(1, 1), new(0, 1), new(1, 0), new(0, 0) });
        Assert.False(bowtie.IsSimple);
    }

    [Fact]
    public void Ring_Validation_On_Construction()
    {
        Assert.Throws<ArgumentException>(() => new GeoLineString(new GeoPoint[] { new(0, 0) }));
        Assert.Throws<ArgumentException>(() => new GeoPolygon(new GeoPoint[] { new(0, 0), new(0, 1), new(1, 1) }));
    }
}
