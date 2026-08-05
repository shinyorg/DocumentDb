namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// Enforces that the geometry argument of a <c>DocumentFunctions</c> spatial call names a spatially-mapped
/// property.
/// <para>
/// This is not a stylistic rule. Most relational providers serve these predicates from the spatial sidecar's
/// geometry column (PostgreSQL/DuckDB <c>ST_*</c>, SQL Server <c>geom.ST*</c>, MySQL <c>ST_*</c>, Oracle
/// <c>SDO_RELATE</c>), and that column only ever holds the *mapped* property — so an unmapped path would be
/// answered as though it were the mapped one. The providers that address the JSON path directly (SQLite,
/// Cosmos, MongoDB) would instead answer correctly but unindexed. Rejecting it everywhere keeps one answer.
/// </para>
/// </summary>
static class SpatialPathGuard
{
    public static void Require(string jsonPath, IReadOnlySet<string>? mappedPaths, Type? documentType)
    {
        if (mappedPaths == null || mappedPaths.Contains(jsonPath))
            return;

        var typeName = documentType?.Name;
        var mapped = mappedPaths.Count == 0
            ? $"'{typeName ?? "The document type"}' has no spatial mapping."
            : $"Mapped: {string.Join(", ", mappedPaths.Order().Select(p => $"'{p}'"))}.";

        throw new NotSupportedException(
            $"'{jsonPath}' is not a mapped spatial property, so a DocumentFunctions spatial call cannot address it. " +
            $"{mapped} Map it with MapSpatialProperty in ConfigureDocument<{typeName ?? "T"}>().");
    }
}
