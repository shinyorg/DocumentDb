using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// A polygon: one closed exterior ring plus zero or more interior rings (holes). Each ring is a closed
/// coordinate sequence of 4+ points (first == last). GeoJSON <c>Polygon</c>.
/// </summary>
public sealed class GeoPolygon : Geometry
{
    static readonly IReadOnlyList<IReadOnlyList<GeoPoint>> NoHoles = Array.Empty<IReadOnlyList<GeoPoint>>();

    public GeoPolygon(IReadOnlyList<GeoPoint> exteriorRing)
        : this(exteriorRing, NoHoles) { }

    public GeoPolygon(IReadOnlyList<GeoPoint> exteriorRing, IReadOnlyList<IReadOnlyList<GeoPoint>> interiorRings)
    {
        ArgumentNullException.ThrowIfNull(exteriorRing);
        if (exteriorRing.Count < 4)
            throw new ArgumentException("Polygon exterior ring requires at least 4 coordinates (closed ring).", nameof(exteriorRing));
        this.ExteriorRing = exteriorRing;
        this.InteriorRings = interiorRings ?? NoHoles;
    }

    public IReadOnlyList<GeoPoint> ExteriorRing { get; }
    public IReadOnlyList<IReadOnlyList<GeoPoint>> InteriorRings { get; }

    public override GeoGeometryType GeometryType => GeoGeometryType.Polygon;
    public override bool IsEmpty => this.ExteriorRing.Count == 0;
    public override GeoBoundingBox GetEnvelope() => GeometryMath.Envelope(this.ExteriorRing);
}
