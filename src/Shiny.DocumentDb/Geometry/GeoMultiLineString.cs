using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>A set of line strings. GeoJSON <c>MultiLineString</c>.</summary>
public sealed class GeoMultiLineString : Geometry
{
    public GeoMultiLineString(IReadOnlyList<GeoLineString> lineStrings)
        => this.LineStrings = lineStrings ?? throw new ArgumentNullException(nameof(lineStrings));

    public IReadOnlyList<GeoLineString> LineStrings { get; }

    public override GeoGeometryType GeometryType => GeoGeometryType.MultiLineString;
    public override bool IsEmpty => this.LineStrings.Count == 0;
    public override GeoBoundingBox GetEnvelope() => GeometryMath.Envelope(this.LineStrings);
}
