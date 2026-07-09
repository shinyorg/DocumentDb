using System.Globalization;
using System.Text;

namespace Shiny.DocumentDb.Internal.Spatial;

/// <summary>
/// Serializes a <see cref="Geometry"/> to OGC Well-Known Text (WKT). Coordinates are written as
/// <c>longitude latitude</c> (X Y). Used by providers that ingest WKT rather than GeoJSON (SQL Server).
/// </summary>
static class WktWriter
{
    public static string ToWkt(Geometry g)
    {
        var sb = new StringBuilder();
        Write(sb, g);
        return sb.ToString();
    }

    static void Write(StringBuilder sb, Geometry g)
    {
        switch (g)
        {
            case GeoPointGeometry p:
                sb.Append("POINT (");
                Coord(sb, p.Point);
                sb.Append(')');
                break;
            case GeoLineString ls:
                sb.Append("LINESTRING ");
                Ring(sb, ls.Coordinates);
                break;
            case GeoPolygon pg:
                sb.Append("POLYGON ");
                Polygon(sb, pg);
                break;
            case GeoMultiPoint mp:
                sb.Append("MULTIPOINT ");
                Ring(sb, mp.Points);
                break;
            case GeoMultiLineString ml:
                sb.Append("MULTILINESTRING (");
                for (var i = 0; i < ml.LineStrings.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    Ring(sb, ml.LineStrings[i].Coordinates);
                }
                sb.Append(')');
                break;
            case GeoMultiPolygon mpg:
                sb.Append("MULTIPOLYGON (");
                for (var i = 0; i < mpg.Polygons.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    Polygon(sb, mpg.Polygons[i]);
                }
                sb.Append(')');
                break;
            case GeoGeometryCollection gc:
                sb.Append("GEOMETRYCOLLECTION (");
                for (var i = 0; i < gc.Geometries.Count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    Write(sb, gc.Geometries[i]);
                }
                sb.Append(')');
                break;
            default:
                throw new NotSupportedException($"Cannot write WKT for {g.GetType().Name}.");
        }
    }

    static void Polygon(StringBuilder sb, GeoPolygon pg)
    {
        sb.Append('(');
        Ring(sb, pg.ExteriorRing);
        for (var i = 0; i < pg.InteriorRings.Count; i++)
        {
            sb.Append(", ");
            Ring(sb, pg.InteriorRings[i]);
        }
        sb.Append(')');
    }

    static void Ring(StringBuilder sb, IReadOnlyList<GeoPoint> coords)
    {
        sb.Append('(');
        for (var i = 0; i < coords.Count; i++)
        {
            if (i > 0) sb.Append(", ");
            Coord(sb, coords[i]);
        }
        sb.Append(')');
    }

    static void Coord(StringBuilder sb, GeoPoint p)
    {
        sb.Append(p.Longitude.ToString("R", CultureInfo.InvariantCulture));
        sb.Append(' ');
        sb.Append(p.Latitude.ToString("R", CultureInfo.InvariantCulture));
    }
}
