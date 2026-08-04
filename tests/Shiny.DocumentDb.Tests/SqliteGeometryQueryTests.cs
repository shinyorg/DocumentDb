using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// Phase 4 end-to-end: the full-geometry predicate + distance query methods over the SQLite two-pass pipeline.
/// </summary>
public class SqliteGeometryQueryTests : IDisposable
{
    sealed class Zone
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public Geometry? Area { get; set; }
    }

    readonly SqliteConnection holdOpen;
    readonly DocumentStore store;

    public SqliteGeometryQueryTests()
    {
        var cs = $"Data Source=geom_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(cs);
        this.holdOpen.Open();

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(cs),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.ConfigureDocument<Zone>(cfg => cfg.MapSpatialProperty(z => z.Area));
        this.store = new DocumentStore(opts);
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.holdOpen.Dispose();
    }

    static GeoPolygon Square(double minLng, double minLat, double maxLng, double maxLat) =>
        new(new GeoPoint[]
        {
            new(minLat, minLng), new(minLat, maxLng), new(maxLat, maxLng), new(maxLat, minLng), new(minLat, minLng)
        });

    async Task Seed()
    {
        await this.store.Upsert(new Zone { Id = "unit", Name = "Unit", Area = Square(0, 0, 1, 1) });
        await this.store.Upsert(new Zone { Id = "far", Name = "Far", Area = Square(10, 10, 11, 11) });
        await this.store.Upsert(new Zone { Id = "overlap", Name = "Overlap", Area = Square(0.5, 0.5, 1.5, 1.5) });
    }

    [Fact]
    public async Task Intersects_Point_Finds_Containing_Zones()
    {
        await this.Seed();
        Geometry probe = new GeoPoint(0.75, 0.75); // inside unit and overlap
        var hits = await this.store.GeoIntersects<Zone>(probe);
        Assert.Equal(new[] { "overlap", "unit" }, hits.Select(h => h.Document.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task Intersects_Prunes_Far_Zone()
    {
        await this.Seed();
        Geometry probe = new GeoPoint(0.1, 0.1);
        var hits = await this.store.GeoIntersects<Zone>(probe);
        Assert.DoesNotContain(hits, h => h.Document.Id == "far");
    }

    [Fact]
    public async Task Contains_Query_Polygon()
    {
        await this.Seed();
        // Small polygon fully inside the unit square.
        var hits = await this.store.GeoContains<Zone>(Square(0.2, 0.2, 0.4, 0.4));
        Assert.Equal(new[] { "unit" }, hits.Select(h => h.Document.Id));
    }

    [Fact]
    public async Task Overlaps_Finds_Partial_Overlap_Only()
    {
        await this.Seed();
        var hits = await this.store.GeoOverlaps<Zone>(Square(0.5, 0.5, 2.5, 2.5));
        // query [0.5,2.5]²: 'unit' [0,1]² partially overlaps; 'overlap' [0.5,1.5]² is fully contained
        // (not an overlap); 'far' is disjoint.
        Assert.Contains(hits, h => h.Document.Id == "unit");
        Assert.DoesNotContain(hits, h => h.Document.Id == "overlap");
        Assert.DoesNotContain(hits, h => h.Document.Id == "far");
    }

    [Fact]
    public async Task Disjoint_Returns_NonIntersecting()
    {
        await this.Seed();
        var hits = await this.store.GeoDisjoint<Zone>(Square(0, 0, 1, 1));
        var ids = hits.Select(h => h.Document.Id).OrderBy(x => x).ToArray();
        Assert.Contains("far", ids);       // clearly disjoint
        Assert.DoesNotContain("unit", ids); // equals the query → intersects
    }

    [Fact]
    public async Task WithinDistance_And_Distance_Ordering()
    {
        await this.Seed();
        Geometry origin = new GeoPoint(0.5, 0.5);
        // Order all zones by distance from the origin point.
        var ordered = await this.store.GeoWithinDistance<Zone>(origin, 5_000_000, orderByDistanceFrom: origin);
        var ids = ordered.Select(h => h.Document.Id).ToArray();
        Assert.Equal("far", ids[^1]); // farthest last
        // distances must be ascending
        for (var i = 1; i < ordered.Count; i++)
            Assert.True(ordered[i].DistanceMeters >= ordered[i - 1].DistanceMeters);
    }

    [Fact]
    public async Task Filter_PushDown_Composes_With_Predicate()
    {
        await this.Seed();
        Geometry probe = new GeoPoint(0.75, 0.75);
        var hits = await this.store.GeoIntersects<Zone>(probe, filter: z => z.Name == "Unit");
        Assert.Equal(new[] { "unit" }, hits.Select(h => h.Document.Id));
    }
}
