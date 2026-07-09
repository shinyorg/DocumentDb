namespace Shiny.DocumentDb.Internal;

class SpatialMapping
{
    public required Type DocumentType { get; init; }
    public required string PropertyName { get; init; }
    public required string JsonPath { get; set; }

    /// <summary>
    /// Extracts the mapped point, or null. Present for both point and geometry mappings — for a geometry
    /// mapping it yields the point when the geometry is a single point, otherwise null (so the point-only
    /// query methods degrade cleanly).
    /// </summary>
    public required Func<object, GeoPoint?> GetGeoPoint { get; init; }

    /// <summary>
    /// Extracts the mapped geometry, or null. Present for geometry mappings; null for a point mapping (the
    /// point is wrapped as a point geometry on demand via <see cref="ResolveGeometry"/>).
    /// </summary>
    public Func<object, Geometry?>? GetGeometry { get; init; }

    /// <summary>The full geometry for indexing/refine — the mapped geometry, or the point wrapped as one.</summary>
    public Geometry? ResolveGeometry(object document)
    {
        if (this.GetGeometry != null)
            return this.GetGeometry(document);

        var point = this.GetGeoPoint(document);
        return point is null ? null : (Geometry)point.Value;
    }

    /// <summary>The indexing envelope for the mapped value, or null when the document has no location.</summary>
    public GeoBoundingBox? ResolveEnvelope(object document)
        => this.ResolveGeometry(document)?.GetEnvelope();
}
