using System.Globalization;
using Shiny.DocumentDb.Internal.Spatial;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// WKT serialization used by the SQL Server ingestion path. Coordinates are written <c>longitude latitude</c>
/// (X Y), polygon rings must be explicitly closed (SQL Server's STGeomFromText rejects an open ring), and
/// numbers must format culture-invariantly.
/// </summary>
public class WktWriterTests
{
    [Fact]
    public void Polygon_UnclosedRing_IsClosed()
    {
        // GeoPoint(lat, lng); four distinct corners with no repeat of the first point.
        var pg = new GeoPolygon(new GeoPoint[] { new(0, 0), new(0, 1), new(1, 1), new(1, 0) });
        var wkt = WktWriter.ToWkt(pg);
        Assert.StartsWith("POLYGON ((", wkt);
        Assert.EndsWith("0 0))", wkt);                  // closed: last coord repeats the first ("lon lat" = 0 0)
    }

    [Fact]
    public void Polygon_AlreadyClosed_NotDoubled()
    {
        var pg = new GeoPolygon(new GeoPoint[] { new(0, 0), new(0, 1), new(1, 1), new(1, 0), new(0, 0) });
        var wkt = WktWriter.ToWkt(pg);
        Assert.EndsWith("0 0))", wkt);
        Assert.DoesNotContain("0 0, 0 0))", wkt);        // no duplicate closing coordinate
    }

    [Fact]
    public void LineString_IsNotClosed()
    {
        var ls = new GeoLineString(new GeoPoint[] { new(0, 0), new(0, 1), new(1, 1) });
        var wkt = WktWriter.ToWkt(ls);
        Assert.StartsWith("LINESTRING (", wkt);
        Assert.EndsWith("1 1)", wkt);                    // a line is not a ring — no closing coordinate appended
    }

    [Fact]
    public void Coord_UsesInvariantCulture()
    {
        var prev = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");   // comma decimal separator
            var pg = new GeoPolygon(new GeoPoint[] { new(0.5, 1.5), new(0.5, 2.5), new(1.5, 2.5), new(1.5, 1.5) });
            var wkt = WktWriter.ToWkt(pg);
            Assert.Contains("1.5 0.5", wkt);             // "lon lat" with '.' decimal, not "1,5"
            Assert.DoesNotContain(",", wkt.Replace(", ", ""));   // no stray comma-decimals (only coord separators)
        }
        finally
        {
            CultureInfo.CurrentCulture = prev;
        }
    }
}
