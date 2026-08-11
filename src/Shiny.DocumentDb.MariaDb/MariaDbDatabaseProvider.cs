using Shiny.DocumentDb.Internal.FullText;
using Shiny.DocumentDb.MySql;

namespace Shiny.DocumentDb.MariaDb;

/// <summary>
/// MariaDB provider. MariaDB speaks the MySQL wire protocol and the same <c>MySqlConnector</c> ADO.NET
/// driver connects to it, so the vast majority of the SQL surface is inherited verbatim from
/// <see cref="MySqlDatabaseProvider"/> — JSON extraction, CRUD, batch/bulk, temporal history, computed
/// columns, indexes, soundex, and full-text all lower to identical dialect. Only the handful of places
/// where MariaDB genuinely diverges from MySQL 8 are overridden here.
///
/// <para><b>Divergences handled:</b></para>
/// <list type="bullet">
///   <item><b>Spatial</b> runs the portable envelope tier (bbox prune + in-process refine) rather than
///   MySQL's native SRID-4326 <c>ST_*</c> pushdown. MariaDB's geometry columns don't accept MySQL's
///   <c>SRID</c> column attribute, and — critically — MariaDB's <c>ST_Distance</c> is <i>not</i> metric for
///   SRID-4326 geometry, so a native <c>WithinDistance</c>/<c>Distance</c> in meters would return wrong
///   results. The portable tier computes true metres in C# and is correct everywhere.</item>
///   <item><b>Full-text proximity</b> (<c>"a b"@N</c>) is dropped from the advertised Lucene capabilities.
///   MariaDB's boolean-mode full-text has no proximity operator (the <c>@</c> is reserved but unimplemented
///   for this), so the query layer rejects proximity queries up front instead of emitting invalid SQL. Term,
///   phrase, prefix, and required/optional/excluded occur are all still supported.</item>
/// </list>
///
/// <para><b>Array-valued queries are not supported.</b> MySQL's array-unnest lowering uses <c>JSON_TABLE</c>,
/// which MariaDB has never implemented (MDEV-16620, still open through 11.x), and MariaDB also lacks
/// <c>LATERAL</c> — so an outer-correlated array unnest cannot be expressed. Predicates and projections that
/// unnest a JSON array (<c>Any</c>/<c>All</c> over a collection, array aggregates, array <c>GroupBy</c>)
/// therefore throw a clear <see cref="NotSupportedException"/> at query-build time rather than emitting SQL
/// that errors on the server. Scalar CRUD, filtering, ordering, projection, temporal, computed columns,
/// full-text, and backup all work as on MySQL.</para>
/// </summary>
public class MariaDbDatabaseProvider : MySqlDatabaseProvider
{
    public MariaDbDatabaseProvider(string connectionString) : base(connectionString)
    {
    }

    // MariaDB has no SRID-aware metric ST_Distance and rejects MySQL's `SRID` geometry-column attribute,
    // so force the dependency-free envelope tier for all spatial work.
    protected override bool PortableSpatialMode => true;

    // MariaDB boolean-mode full-text supports prefix but not the "phrase"@N proximity operator.
    public override FtCapabilities FullTextQueryCapabilities => FtCapabilities.Prefix;

    // MariaDB has neither JSON_TABLE nor LATERAL, so the outer-correlated array unnest MySQL relies on can't be
    // expressed. Fail loud at SQL-build time instead of emitting a JSON_TABLE(...) that errors on the server.
    public override string JsonEachFrom(string column, string jsonPath)
        => throw new NotSupportedException(
            "MariaDB has no JSON_TABLE, so array-unnest queries (Any/All over a collection, array aggregate " +
            "projections, and GroupBy over an array element) are not supported. Use the MySQL provider for " +
            "these queries, or restructure the model to avoid unnesting a JSON array.");

    // MariaDB does not accept `CAST(expr AS JSON)` (a MySQL 8 syntax). JSON_COMPACT(text) parses a JSON text
    // into a JSON value and is the MariaDB-compatible equivalent everywhere the base emits `CAST(... AS JSON)`.
    public override string BuildSetPropertySql(string tableName) => $"""
        UPDATE `{tableName}`
        SET Data = JSON_SET(Data, @path, JSON_COMPACT(@value)), UpdatedAt = @now
        WHERE Id = @id AND TypeName = @typeName;
        """;

    public override string BuildJsonSetExpression(string sourceExpression, string pathParameter, string valueParameter)
        => $"JSON_SET({sourceExpression}, {pathParameter}, JSON_COMPACT({valueParameter}))";

    public override string JsonTrue() => "JSON_COMPACT('true')";

    public override string JsonFalse() => "JSON_COMPACT('false')";

    // FOR SHARE is MySQL 8 syntax that MariaDB never adopted; LOCK IN SHARE MODE is its spelling of the same
    // shared row lock. FOR UPDATE is identical on both, so only the shared mode diverges.
    public override string BuildLockClause(LockMode mode) => mode switch
    {
        LockMode.Share => " LOCK IN SHARE MODE",
        _ => base.BuildLockClause(mode)
    };
}
