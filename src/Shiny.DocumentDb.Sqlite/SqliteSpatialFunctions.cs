using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Shiny.DocumentDb.Sqlite;

/// <summary>
/// Registers the <c>docdb_st_*</c> scalar UDFs on a SQLite connection so <c>DocumentFunctions</c> spatial
/// predicates can be evaluated inside a SQL <c>WHERE</c> (as the exact refine after the R*Tree bbox prune).
/// Each UDF takes two GeoJSON strings (the stored geometry via <c>json_extract</c> and the query geometry as
/// a parameter) and calls the public relate/distance engine.
/// </summary>
static class SqliteSpatialFunctions
{
    public static void Register(SqliteConnection c)
    {
        Predicate(c, "docdb_st_intersects", DocumentFunctions.Intersects);
        Predicate(c, "docdb_st_disjoint", DocumentFunctions.Disjoint);
        Predicate(c, "docdb_st_contains", DocumentFunctions.Contains);
        Predicate(c, "docdb_st_within", DocumentFunctions.Within);
        Predicate(c, "docdb_st_covers", DocumentFunctions.Covers);
        Predicate(c, "docdb_st_coveredby", DocumentFunctions.CoveredBy);
        Predicate(c, "docdb_st_touches", DocumentFunctions.Touches);
        Predicate(c, "docdb_st_crosses", DocumentFunctions.Crosses);
        Predicate(c, "docdb_st_overlaps", DocumentFunctions.Overlaps);
        Predicate(c, "docdb_st_equals", DocumentFunctions.GeoEquals);

        c.CreateFunction<string?, string?, double, long>("docdb_st_dwithin",
            (stored, query, meters) => stored == null || query == null ? 0
                : DocumentFunctions.WithinDistance(Parse(stored), Parse(query), meters) ? 1 : 0);

        c.CreateFunction<string?, string?, double?>("docdb_st_distance",
            (stored, query) => stored == null || query == null ? null
                : DocumentFunctions.Distance(Parse(stored), Parse(query)));
    }

    static void Predicate(SqliteConnection c, string name, Func<Geometry, Geometry, bool> fn)
        => c.CreateFunction<string?, string?, long>(name,
            (stored, query) => stored == null || query == null ? 0 : fn(Parse(stored), Parse(query)) ? 1 : 0);

    static Geometry Parse(string json) => JsonSerializer.Deserialize<Geometry>(json)!;
}
