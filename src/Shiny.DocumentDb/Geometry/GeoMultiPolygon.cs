using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>A set of polygons. GeoJSON <c>MultiPolygon</c>.</summary>
public sealed class GeoMultiPolygon : Geometry
{
    public GeoMultiPolygon(IReadOnlyList<GeoPolygon> polygons)
        => this.Polygons = polygons ?? throw new ArgumentNullException(nameof(polygons));

    public IReadOnlyList<GeoPolygon> Polygons { get; }

    public override GeoGeometryType GeometryType => GeoGeometryType.MultiPolygon;
    public override bool IsEmpty => this.Polygons.Count == 0;
    public override GeoBoundingBox GetEnvelope() => GeometryMath.Envelope(this.Polygons);
}
