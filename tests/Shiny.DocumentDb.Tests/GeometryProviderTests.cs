using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Shared full-geometry query behaviour run against every spatial-capable provider via its fixture's
/// <c>CreateSpatialStore</c>. Each test gets an isolated table, so seeded data never crosses tests.
/// Exercises the predicate family + distance ordering end-to-end through each provider's envelope-sidecar
/// (relational) pass-1 plus the C# relate refine.
/// </summary>
public abstract class GeometryProviderTestsBase
{
    protected abstract IDocumentStore CreateStore(string tableName);

    static GeoPolygon Square(double minLng, double minLat, double maxLng, double maxLat) =>
        new(new GeoPoint[]
        {
            new(minLat, minLng), new(minLat, maxLng), new(maxLat, maxLng), new(maxLat, minLng), new(minLat, minLng)
        });

    async Task<IDocumentStore> SeedAsync(string name)
    {
        var store = this.CreateStore($"{name}_{Guid.NewGuid():N}");
        await store.Upsert(new GeoZone { Id = "unit", Name = "Unit", Area = Square(0, 0, 1, 1) });
        await store.Upsert(new GeoZone { Id = "far", Name = "Far", Area = Square(10, 10, 11, 11) });
        await store.Upsert(new GeoZone { Id = "overlap", Name = "Overlap", Area = Square(0.5, 0.5, 1.5, 1.5) });
        return store;
    }

    [Fact]
    public async Task Intersects_Point_Finds_Containing_Zones()
    {
        var store = await this.SeedAsync("geo_int");
        var hits = await store.GeoIntersects<GeoZone>(new GeoPoint(0.75, 0.75));
        Assert.Equal(new[] { "overlap", "unit" }, hits.Select(h => h.Document.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task Intersects_Prunes_Far_Zone()
    {
        var store = await this.SeedAsync("geo_prune");
        var hits = await store.GeoIntersects<GeoZone>(new GeoPoint(0.1, 0.1));
        Assert.DoesNotContain(hits, h => h.Document.Id == "far");
    }

    [Fact]
    public async Task Contains_Query_Polygon()
    {
        var store = await this.SeedAsync("geo_cont");
        var hits = await store.GeoContains<GeoZone>(Square(0.2, 0.2, 0.4, 0.4));
        Assert.Equal(new[] { "unit" }, hits.Select(h => h.Document.Id));
    }

    [Fact]
    public async Task Overlaps_Finds_Partial_Overlap_Only()
    {
        var store = await this.SeedAsync("geo_over");
        var hits = await store.GeoOverlaps<GeoZone>(Square(0.5, 0.5, 2.5, 2.5));
        Assert.Contains(hits, h => h.Document.Id == "unit");
        Assert.DoesNotContain(hits, h => h.Document.Id == "overlap");
        Assert.DoesNotContain(hits, h => h.Document.Id == "far");
    }

    [Fact]
    public async Task Disjoint_Returns_NonIntersecting()
    {
        var store = await this.SeedAsync("geo_disj");
        var hits = await store.GeoDisjoint<GeoZone>(Square(0, 0, 1, 1));
        var ids = hits.Select(h => h.Document.Id).ToHashSet();
        Assert.Contains("far", ids);
        Assert.DoesNotContain("unit", ids);
    }

    [Fact]
    public async Task WithinDistance_And_Distance_Ordering()
    {
        var store = await this.SeedAsync("geo_dist");
        Geometry origin = new GeoPoint(0.5, 0.5);
        var ordered = await store.GeoWithinDistance<GeoZone>(origin, 5_000_000, orderByDistanceFrom: origin);
        var ids = ordered.Select(h => h.Document.Id).ToArray();
        Assert.Equal("far", ids[^1]);
        for (var i = 1; i < ordered.Count; i++)
            Assert.True(ordered[i].DistanceMeters >= ordered[i - 1].DistanceMeters);
    }

    [Fact]
    public async Task NearestNeighbors_Over_Geometry()
    {
        var store = await this.SeedAsync("geo_nn");
        var near = await store.NearestNeighbors<GeoZone>(new GeoPoint(0.5, 0.5), count: 2);
        Assert.DoesNotContain(near, h => h.Document.Id == "far");
    }

    [Fact]
    public async Task Filter_PushDown_Composes_With_Predicate()
    {
        var store = await this.SeedAsync("geo_filter");
        var hits = await store.GeoIntersects<GeoZone>(new GeoPoint(0.75, 0.75), filter: z => z.Name == "Unit");
        Assert.Equal(new[] { "unit" }, hits.Select(h => h.Document.Id));
    }
}

[Collection("PostgreSQL")]
public class PostgreSqlGeometryTests(PostgreSqlDatabaseFixture fx) : GeometryProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
}

[Collection("MySQL")]
public class MySqlGeometryTests(MySqlDatabaseFixture fx) : GeometryProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
}

[Collection("MariaDB")]
public class MariaDbGeometryTests(MariaDbDatabaseFixture fx) : GeometryProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
}

[Collection("CockroachDB")]
public class CockroachDbGeometryTests(CockroachDbDatabaseFixture fx) : GeometryProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
}

[Collection("DuckDB")]
public class DuckDbGeometryTests(DuckDbDatabaseFixture fx) : GeometryProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
}

[Collection("MSSQL")]
public class SqlServerGeometryTests(MsSqlDatabaseFixture fx) : GeometryProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
}

[Collection("Oracle")]
public class OracleGeometryTests(OracleDatabaseFixture fx) : GeometryProviderTestsBase
{
    protected override IDocumentStore CreateStore(string tableName) => fx.CreateSpatialStore(tableName);
}
