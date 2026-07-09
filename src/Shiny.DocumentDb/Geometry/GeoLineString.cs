using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>A connected sequence of 2+ coordinates. GeoJSON <c>LineString</c>.</summary>
public sealed class GeoLineString : Geometry
{
    public GeoLineString(IReadOnlyList<GeoPoint> coordinates)
    {
        ArgumentNullException.ThrowIfNull(coordinates);
        if (coordinates.Count < 2)
            throw new ArgumentException("LineString requires at least 2 coordinates.", nameof(coordinates));
        this.Coordinates = coordinates;
    }

    public IReadOnlyList<GeoPoint> Coordinates { get; }

    /// <summary>True when the line is closed (first and last coordinate coincide, 4+ points).</summary>
    public bool IsClosed => this.Coordinates.Count >= 4 && this.Coordinates[0] == this.Coordinates[^1];

    public override GeoGeometryType GeometryType => GeoGeometryType.LineString;
    public override bool IsEmpty => this.Coordinates.Count == 0;
    public override GeoBoundingBox GetEnvelope() => GeometryMath.Envelope(this.Coordinates);
}
