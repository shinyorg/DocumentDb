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
/// <para><b>Minimum version:</b> MariaDB <b>10.6+</b>. Earlier releases lack <c>JSON_TABLE</c>, which the
/// inherited array-unnest path (<c>JsonEachFrom</c>) — used by <c>Any</c>/<c>All</c>/array <c>GroupBy</c> —
/// depends on. Core CRUD/query works on older releases; array-valued predicates do not.</para>
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
}
