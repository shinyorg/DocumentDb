namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Adapts the lightweight <see cref="GeoPoint"/> struct to the <see cref="Geometry"/> hierarchy so a point
/// can flow through the geometry query methods and the relate engine. Produced by the implicit
/// <c>GeoPoint → Geometry</c> conversion.
/// </summary>
sealed class GeoPointGeometry : Geometry
{
    public GeoPointGeometry(GeoPoint point) => this.Point = point;

    public GeoPoint Point { get; }

    public override GeoGeometryType GeometryType => GeoGeometryType.Point;
    public override bool IsEmpty => false;
    public override GeoBoundingBox GetEnvelope()
        => new(this.Point.Latitude, this.Point.Longitude, this.Point.Latitude, this.Point.Longitude);
}
