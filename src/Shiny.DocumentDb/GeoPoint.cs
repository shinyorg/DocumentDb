using System.Text.Json.Serialization;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// Represents a geographic point using WGS84 coordinates.
/// Serializes as GeoJSON: {"type":"Point","coordinates":[longitude,latitude]}
/// </summary>
[JsonConverter(typeof(GeoPointJsonConverter))]
public readonly record struct GeoPoint(double Latitude, double Longitude)
{
    /// <summary>The zero-area envelope of this point, for R*Tree / bbox indexing.</summary>
    public GeoBoundingBox GetEnvelope() => new(this.Latitude, this.Longitude, this.Latitude, this.Longitude);

    /// <summary>
    /// Wraps a point as a <see cref="Geometry"/> so a bare <see cref="GeoPoint"/> can be passed to the
    /// geometry query methods (<c>GeoIntersects</c>, <c>GeoContainedBy</c>, …) without constructing a
    /// geometry explicitly.
    /// </summary>
    public static implicit operator Geometry(GeoPoint point) => new GeoPointGeometry(point);
}
