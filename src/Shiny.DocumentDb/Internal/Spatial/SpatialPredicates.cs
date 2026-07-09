namespace Shiny.DocumentDb.Internal.Spatial;

/// <summary>
/// The OGC topological predicates, expressed as combinations of <see cref="GeometryRelate"/> facts.
/// Each predicate reads <c>subject &lt;predicate&gt; other</c> — e.g. <c>Contains(subject, other)</c> is
/// "subject contains other". These are the C# pass-2 refine used by every provider's geometry query.
/// </summary>
static class SpatialPredicates
{
    public static bool Intersects(Geometry subject, Geometry other)
        => GeometryRelate.Intersects(subject, other);

    public static bool Disjoint(Geometry subject, Geometry other)
        => !GeometryRelate.Intersects(subject, other);

    public static bool Contains(Geometry subject, Geometry other)
        => GeometryRelate.Covers(subject, other) && GeometryRelate.InteriorIntersects(subject, other);

    public static bool Within(Geometry subject, Geometry other)
        => Contains(other, subject);

    public static bool Covers(Geometry subject, Geometry other)
        => GeometryRelate.Covers(subject, other);

    public static bool CoveredBy(Geometry subject, Geometry other)
        => GeometryRelate.Covers(other, subject);

    public static bool GeometryEquals(Geometry subject, Geometry other)
        => GeometryRelate.GeometryEquals(subject, other);

    public static bool Touches(Geometry subject, Geometry other)
        => GeometryRelate.Intersects(subject, other) && !GeometryRelate.InteriorIntersects(subject, other);

    public static bool Overlaps(Geometry subject, Geometry other)
    {
        if (GeometryRelate.Dimension(subject) != GeometryRelate.Dimension(other))
            return false;
        if (!GeometryRelate.InteriorIntersects(subject, other))
            return false;
        if (GeometryRelate.Covers(subject, other) || GeometryRelate.Covers(other, subject))
            return false;
        // For linear geometries, a partial overlap requires a shared collinear segment (else it's a crossing).
        if (GeometryRelate.Dimension(subject) == 1)
            return GeometryRelate.CollinearOverlap(subject, other);
        return true;
    }

    public static bool Crosses(Geometry subject, Geometry other)
    {
        if (!GeometryRelate.InteriorIntersects(subject, other))
            return false;
        if (GeometryRelate.Covers(subject, other) || GeometryRelate.Covers(other, subject))
            return false;
        if (Overlaps(subject, other))
            return false;

        var da = GeometryRelate.Dimension(subject);
        var db = GeometryRelate.Dimension(other);
        // Crossing is defined for differing dimensions, or two lines meeting at a point.
        return da != db || (da == 1 && db == 1);
    }
}
