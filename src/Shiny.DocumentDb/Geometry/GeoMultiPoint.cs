using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>A set of coordinates. GeoJSON <c>MultiPoint</c>.</summary>
public sealed class GeoMultiPoint : Geometry
{
    public GeoMultiPoint(IReadOnlyList<GeoPoint> points)
        => this.Points = points ?? throw new ArgumentNullException(nameof(points));

    public IReadOnlyList<GeoPoint> Points { get; }

    public override GeoGeometryType GeometryType => GeoGeometryType.MultiPoint;
    public override bool IsEmpty => this.Points.Count == 0;
    public override GeoBoundingBox GetEnvelope() => GeometryMath.Envelope(this.Points);
}
