using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>A heterogeneous set of geometries. GeoJSON <c>GeometryCollection</c>.</summary>
public sealed class GeoGeometryCollection : Geometry
{
    public GeoGeometryCollection(IReadOnlyList<Geometry> geometries)
        => this.Geometries = geometries ?? throw new ArgumentNullException(nameof(geometries));

    public IReadOnlyList<Geometry> Geometries { get; }

    public override GeoGeometryType GeometryType => GeoGeometryType.GeometryCollection;
    public override bool IsEmpty => this.Geometries.Count == 0;
    public override GeoBoundingBox GetEnvelope() => GeometryMath.EnvelopeOfGeometries(this.Geometries);
}
