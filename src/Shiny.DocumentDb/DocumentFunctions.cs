using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Spatial;

namespace Shiny.DocumentDb;

/// <summary>
/// Query-translatable functions with no direct BCL equivalent. Used inside a <c>Where</c> predicate or
/// projection — e.g. <c>store.Query&lt;Person&gt;().Where(p =&gt; DocumentFunctions.Soundex(p.Name) == DocumentFunctions.Soundex("Smith"))</c>.
/// Each method has a real implementation, so the in-memory providers (LiteDB, IndexedDB) execute it
/// directly while the pushdown providers translate it to a native / registered function.
/// </summary>
public static class DocumentFunctions
{
    /// <summary>
    /// Classic American Soundex phonetic code. Translates to native <c>SOUNDEX()</c> (SQL Server, MySQL,
    /// Oracle), the <c>fuzzystrmatch</c> <c>soundex()</c> (PostgreSQL), or a registered connection UDF
    /// (SQLite, DuckDB). Not translatable on Cosmos/Mongo — use a computed stored field there.
    /// </summary>
    public static string Soundex(string value) => Phonetics.Soundex(value);

    // ── Spatial predicates (usable inside a Where; read "a <predicate> b") ─────────────────────────────
    // Each carries a real relate-engine implementation, so in-memory providers run it directly while the
    // spatial providers translate it to a native ST_*/SDO function or a registered UDF (SQLite). Requires a
    // spatial-capable provider; translating one on a non-spatial provider throws.

    /// <summary>True when <paramref name="a"/> and <paramref name="b"/> share any point.</summary>
    public static bool Intersects(Geometry a, Geometry b) => SpatialPredicates.Intersects(a, b);

    /// <summary>True when <paramref name="a"/> and <paramref name="b"/> share no point.</summary>
    public static bool Disjoint(Geometry a, Geometry b) => SpatialPredicates.Disjoint(a, b);

    /// <summary>True when <paramref name="a"/> contains <paramref name="b"/>.</summary>
    public static bool Contains(Geometry a, Geometry b) => SpatialPredicates.Contains(a, b);

    /// <summary>True when <paramref name="a"/> is within <paramref name="b"/>.</summary>
    public static bool Within(Geometry a, Geometry b) => SpatialPredicates.Within(a, b);

    /// <summary>True when <paramref name="a"/> covers <paramref name="b"/> (boundary-inclusive contains).</summary>
    public static bool Covers(Geometry a, Geometry b) => SpatialPredicates.Covers(a, b);

    /// <summary>True when <paramref name="a"/> is covered by <paramref name="b"/>.</summary>
    public static bool CoveredBy(Geometry a, Geometry b) => SpatialPredicates.CoveredBy(a, b);

    /// <summary>True when <paramref name="a"/> touches <paramref name="b"/> (boundaries meet, interiors don't).</summary>
    public static bool Touches(Geometry a, Geometry b) => SpatialPredicates.Touches(a, b);

    /// <summary>True when <paramref name="a"/> crosses <paramref name="b"/>.</summary>
    public static bool Crosses(Geometry a, Geometry b) => SpatialPredicates.Crosses(a, b);

    /// <summary>True when <paramref name="a"/> overlaps <paramref name="b"/> (same dimension, partial).</summary>
    public static bool Overlaps(Geometry a, Geometry b) => SpatialPredicates.Overlaps(a, b);

    /// <summary>True when <paramref name="a"/> is spatially equal to <paramref name="b"/>.</summary>
    public static bool GeoEquals(Geometry a, Geometry b) => SpatialPredicates.GeometryEquals(a, b);

    /// <summary>True when <paramref name="a"/> lies within <paramref name="meters"/> of <paramref name="b"/>.</summary>
    public static bool WithinDistance(Geometry a, Geometry b, double meters) => GeoDistance.Between(a, b) <= meters;

    /// <summary>The distance in meters between <paramref name="a"/> and <paramref name="b"/> (for <c>OrderBy</c>).</summary>
    public static double Distance(Geometry a, Geometry b) => GeoDistance.Between(a, b);
}
