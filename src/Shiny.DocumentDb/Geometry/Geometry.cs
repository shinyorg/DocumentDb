using System.Text.Json.Serialization;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// Base type for full OGC geometry. Serializes as GeoJSON in the document body.
/// The lightweight <see cref="GeoPoint"/> struct implicitly converts to a point geometry, so it can be
/// passed to any of the geometry query methods (<c>GeoIntersects</c>, <c>GeoContainedBy</c>, …).
/// </summary>
[JsonConverter(typeof(GeometryJsonConverter))]
public abstract class Geometry
{
    /// <summary>The geometry type discriminator.</summary>
    public abstract GeoGeometryType GeometryType { get; }

    /// <summary>True when the geometry has no coordinates.</summary>
    public abstract bool IsEmpty { get; }

    /// <summary>The axis-aligned bounding envelope used for R*Tree / bbox indexing.</summary>
    public abstract GeoBoundingBox GetEnvelope();

    /// <summary>
    /// Approximate geodesic area in square meters (0 for point/line geometries). Spherical-excess
    /// approximation; see the spatial docs for fidelity notes.
    /// </summary>
    public double Area => GeometryMath.Area(this);

    /// <summary>Total geodesic length/perimeter in meters (0 for point geometries).</summary>
    public double Length => GeometryMath.Length(this);

    /// <summary>Alias for <see cref="Length"/> — the perimeter of closed geometries.</summary>
    public double Perimeter => GeometryMath.Length(this);

    /// <summary>The geometry centroid (area-weighted for polygons, coordinate-average otherwise).</summary>
    public GeoPoint Centroid => GeometryMath.Centroid(this);

    /// <summary>Total number of coordinates across the geometry.</summary>
    public int NumPoints => GeometryMath.NumPoints(this);

    /// <summary>Number of sub-geometries (1 for simple types; the element count for Multi*/collections).</summary>
    public int NumGeometries => GeometryMath.NumGeometries(this);

    /// <summary>
    /// Lightweight validity check: coordinate finiteness/range, minimum vertex counts, ring closure,
    /// and (for polygons) that the exterior ring does not self-intersect.
    /// </summary>
    public bool IsValid => GeometryValidation.IsValid(this);

    /// <summary>True when the geometry has no self-intersections (rings/lines).</summary>
    public bool IsSimple => GeometryValidation.IsSimple(this);

    /// <summary>
    /// Best-effort lightweight repair: closes rings, normalizes ring winding (exterior CCW, holes CW),
    /// and drops consecutive-duplicate and degenerate vertices. Does not perform full OGC noding/
    /// polygonization repair.
    /// </summary>
    public Geometry MakeValid() => GeometryValidation.MakeValid(this);
}
