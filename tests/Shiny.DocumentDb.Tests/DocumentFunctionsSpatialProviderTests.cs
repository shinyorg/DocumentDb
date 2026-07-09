using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// DocumentFunctions spatial predicates inside a LINQ Where, run against each native-capable provider via its
/// fixture's CreateSpatialStore. Asserts the LINQ surface (native ST_*/SDO pushdown) agrees with the dedicated
/// store.Geo* methods for every predicate. Each test gets an isolated table.
/// </summary>
public abstract class DocumentFunctionsSpatialProviderTestsBase
{
    protected abstract IDocumentStore CreateStore(string tableName);

    /// <summary>Predicates a provider approximates (e.g. MySQL has no ST_Covers) — skipped in conformance.</summary>
    protected virtual ISet<string> ApproximatedPredicates => new HashSet<string>();

    static GeoPolygon Square(double minLng, double minLat, double maxLng, double maxLat) =>
        new(new GeoPoint[]
        {
            new(minLat, minLng), new(minLat, maxLng), new(maxLat, maxLng), new(maxLat, minLng), new(minLat, minLng)
        });

    async Task<IDocumentStore> Seed(string name)
    {
        var store = this.CreateStore($"{name}_{Guid.NewGuid():N}");
        await store.Upsert(new GeoZone { Id = "unit", Name = "Unit", Area = Square(0, 0, 1, 1) });
        await store.Upsert(new GeoZone { Id = "far", Name = "Far", Area = Square(10, 10, 11, 11) });
        await store.Upsert(new GeoZone { Id = "overlap", Name = "Overlap", Area = Square(0.5, 0.5, 1.5, 1.5) });
        return store;
    }

    [Fact]
    public async Task Intersects_In_Where_Composes()
    {
        var store = await this.Seed("df_int");
        Geometry probe = new GeoPoint(0.75, 0.75);
        var hits = await store.Query<GeoZone>()
            .Where(z => DocumentFunctions.Intersects(z.Area!, probe) && z.Name != "Overlap")
            .ToList();
        Assert.Equal(new[] { "unit" }, hits.Select(h => h.Id));
    }

    [Fact]
    public async Task WithinDistance_Count()
    {
        var store = await this.Seed("df_dist");
        Geometry origin = new GeoPoint(0.5, 0.5);
        var count = await store.Query<GeoZone>()
            .Where(z => DocumentFunctions.WithinDistance(z.Area!, origin, 300_000))
            .Count();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Conformance_LinqMatchesDedicated()
    {
        var store = await this.Seed("df_conf");
        // Avoid exact edge-coincidence with the seeds: native engines' boundary rules (ST_Within etc.) can
        // differ from the C# relate on a shared edge — a documented per-provider boundary-case difference.
        var q = Square(0.4, 0.4, 2.6, 2.6);

        async Task Check(string name, System.Linq.Expressions.Expression<Func<GeoZone, bool>> linq, Func<Task<IEnumerable<string>>> dedicated)
        {
            if (this.ApproximatedPredicates.Contains(name))
                return;
            var a = (await store.Query<GeoZone>().Where(linq).ToList()).Select(z => z.Id).OrderBy(x => x).ToArray();
            var b = (await dedicated()).OrderBy(x => x).ToArray();
            Assert.True(a.SequenceEqual(b), $"{name}: LINQ [{string.Join(",", a)}] != dedicated [{string.Join(",", b)}]");
        }

        await Check("Intersects", z => DocumentFunctions.Intersects(z.Area!, q), async () => (await store.GeoIntersects<GeoZone>(q)).Select(r => r.Document.Id));
        await Check("Contains", z => DocumentFunctions.Contains(z.Area!, q), async () => (await store.GeoContains<GeoZone>(q)).Select(r => r.Document.Id));
        await Check("Within", z => DocumentFunctions.Within(z.Area!, q), async () => (await store.GeoContainedBy<GeoZone>(q)).Select(r => r.Document.Id));
        await Check("Disjoint", z => DocumentFunctions.Disjoint(z.Area!, q), async () => (await store.GeoDisjoint<GeoZone>(q)).Select(r => r.Document.Id));
        await Check("Touches", z => DocumentFunctions.Touches(z.Area!, q), async () => (await store.GeoTouches<GeoZone>(q)).Select(r => r.Document.Id));
        await Check("Crosses", z => DocumentFunctions.Crosses(z.Area!, q), async () => (await store.GeoCrosses<GeoZone>(q)).Select(r => r.Document.Id));
        await Check("Overlaps", z => DocumentFunctions.Overlaps(z.Area!, q), async () => (await store.GeoOverlaps<GeoZone>(q)).Select(r => r.Document.Id));
        await Check("Equals", z => DocumentFunctions.GeoEquals(z.Area!, q), async () => (await store.GeoEquals<GeoZone>(q)).Select(r => r.Document.Id));
        await Check("Covers", z => DocumentFunctions.Covers(z.Area!, q), async () => (await store.GeoCovers<GeoZone>(q)).Select(r => r.Document.Id));
        await Check("CoveredBy", z => DocumentFunctions.CoveredBy(z.Area!, q), async () => (await store.GeoCoveredBy<GeoZone>(q)).Select(r => r.Document.Id));
        await Check("WithinDistance", z => DocumentFunctions.WithinDistance(z.Area!, q, 300_000), async () => (await store.GeoWithinDistance<GeoZone>(q, 300_000)).Select(r => r.Document.Id));
    }
}

[Collection("MySQL")]
public class MySqlDocumentFunctionsSpatialTests(MySqlDatabaseFixture fx) : DocumentFunctionsSpatialProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
    // MySQL has no ST_Covers/ST_CoveredBy — approximated with Contains/Within.
    protected override ISet<string> ApproximatedPredicates => new HashSet<string> { "Covers", "CoveredBy" };
}

[Collection("DuckDB")]
public class DuckDbDocumentFunctionsSpatialTests(DuckDbDatabaseFixture fx) : DocumentFunctionsSpatialProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
    // DuckDB has no polygon geodesic distance — WithinDistance is a planar-degree approximation.
    protected override ISet<string> ApproximatedPredicates => new HashSet<string> { "WithinDistance" };
}

[Collection("PostgreSQL")]
public class PostgreSqlDocumentFunctionsSpatialTests(PostgreSqlDatabaseFixture fx) : DocumentFunctionsSpatialProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
}

[Collection("MSSQL")]
public class SqlServerDocumentFunctionsSpatialTests(MsSqlDatabaseFixture fx) : DocumentFunctionsSpatialProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
    // SQL Server geometry has no STCovers/STCoveredBy; WithinDistance is a planar-degree approximation.
    protected override ISet<string> ApproximatedPredicates => new HashSet<string> { "Covers", "CoveredBy", "WithinDistance" };
}

[Collection("MongoDB")]
public class MongoDbDocumentFunctionsSpatialTests(MongoDbDatabaseFixture fx) : DocumentFunctionsSpatialProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
    // MongoDB find filters natively support only $geoIntersects / $geoWithin (+ $centerSphere for a point
    // distance) — the finer predicates in a Where throw; use the dedicated store.Geo* methods for those.
    protected override ISet<string> ApproximatedPredicates =>
        new HashSet<string> { "Contains", "Covers", "CoveredBy", "Touches", "Crosses", "Overlaps", "Equals", "Disjoint", "WithinDistance" };
}
