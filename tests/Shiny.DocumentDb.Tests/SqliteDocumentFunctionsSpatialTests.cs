using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// Phase 2: DocumentFunctions spatial predicates inside a LINQ Where on SQLite (R*Tree prune + UDF refine),
/// composing with ordinary predicates, ordering, and Count.
/// </summary>
public class SqliteDocumentFunctionsSpatialTests : IDisposable
{
    sealed class Zone
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public bool Active { get; set; }
        public Geometry? Area { get; set; }
    }

    readonly SqliteConnection holdOpen;
    readonly DocumentStore store;

    public SqliteDocumentFunctionsSpatialTests()
    {
        var cs = $"Data Source=df_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(cs);
        this.holdOpen.Open();

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(cs),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.MapSpatialProperty<Zone>(z => z.Area);
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
        await this.store.Upsert(new Zone { Id = "unit", Name = "Unit", Active = true, Area = Square(0, 0, 1, 1) });
        await this.store.Upsert(new Zone { Id = "far", Name = "Far", Active = true, Area = Square(10, 10, 11, 11) });
        await this.store.Upsert(new Zone { Id = "overlap", Name = "Overlap", Active = false, Area = Square(0.5, 0.5, 1.5, 1.5) });
    }

    [Fact]
    public async Task Intersects_In_Where()
    {
        await this.Seed();
        Geometry probe = new GeoPoint(0.75, 0.75);
        var hits = await this.store.Query<Zone>()
            .Where(z => DocumentFunctions.Intersects(z.Area!, probe))
            .ToList();
        Assert.Equal(new[] { "overlap", "unit" }, hits.Select(h => h.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task Intersects_Composes_With_Ordinary_Predicate()
    {
        await this.Seed();
        Geometry probe = new GeoPoint(0.75, 0.75);
        var hits = await this.store.Query<Zone>()
            .Where(z => DocumentFunctions.Intersects(z.Area!, probe) && z.Active)
            .ToList();
        Assert.Equal(new[] { "unit" }, hits.Select(h => h.Id));  // overlap is inactive
    }

    [Fact]
    public async Task Contains_Direction_And_Inverse()
    {
        await this.Seed();
        var small = Square(0.2, 0.2, 0.4, 0.4);
        // field contains query
        var contains = await this.store.Query<Zone>().Where(z => DocumentFunctions.Contains(z.Area!, small)).ToList();
        Assert.Equal(new[] { "unit" }, contains.Select(h => h.Id));
        // query contains field (inverse arg order) → field within query
        var big = Square(-1, -1, 2, 2);
        var within = await this.store.Query<Zone>().Where(z => DocumentFunctions.Contains(big, z.Area!)).ToList();
        Assert.Equal(new[] { "overlap", "unit" }, within.Select(h => h.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task WithinDistance_And_Count()
    {
        await this.Seed();
        Geometry origin = new GeoPoint(0.5, 0.5);
        var count = await this.store.Query<Zone>()
            .Where(z => DocumentFunctions.WithinDistance(z.Area!, origin, 200_000))  // ~200km
            .Count();
        Assert.Equal(2, count);  // unit + overlap; far is ~1500km away
    }

    [Fact]
    public async Task Disjoint_In_Where()
    {
        await this.Seed();
        var q = Square(0, 0, 1, 1);
        var hits = await this.store.Query<Zone>().Where(z => DocumentFunctions.Disjoint(z.Area!, q)).ToList();
        Assert.Contains(hits, h => h.Id == "far");
        Assert.DoesNotContain(hits, h => h.Id == "unit");
    }

    [Fact]
    public async Task Matches_Dedicated_Method()
    {
        await this.Seed();
        var poly = Square(0.5, 0.5, 2.5, 2.5);
        var viaLinq = (await this.store.Query<Zone>().Where(z => DocumentFunctions.Overlaps(z.Area!, poly)).ToList())
            .Select(h => h.Id).OrderBy(x => x).ToArray();
        var viaMethod = (await this.store.GeoOverlaps<Zone>(poly))
            .Select(h => h.Document.Id).OrderBy(x => x).ToArray();
        Assert.Equal(viaMethod, viaLinq);
    }

    // Conformance: the LINQ DocumentFunctions surface and the dedicated store.Geo* methods must agree for
    // every predicate on the same query geometry.
    [Fact]
    public async Task Conformance_AllPredicates_LinqMatchesDedicated()
    {
        await this.Seed();
        var q = Square(0.5, 0.5, 2.5, 2.5);

        async Task Assert2(Func<Task<IEnumerable<string>>> linq, Func<Task<IEnumerable<string>>> dedicated, string name)
        {
            var a = (await linq()).OrderBy(x => x).ToArray();
            var b = (await dedicated()).OrderBy(x => x).ToArray();
            Assert.True(a.SequenceEqual(b), $"{name}: LINQ [{string.Join(",", a)}] != dedicated [{string.Join(",", b)}]");
        }

        Task<IEnumerable<string>> L(System.Linq.Expressions.Expression<Func<Zone, bool>> w)
            => this.store.Query<Zone>().Where(w).ToList().ContinueWith(t => t.Result.Select(z => z.Id));

        await Assert2(() => L(z => DocumentFunctions.Intersects(z.Area!, q)), async () => (await this.store.GeoIntersects<Zone>(q)).Select(r => r.Document.Id), "Intersects");
        await Assert2(() => L(z => DocumentFunctions.Contains(z.Area!, q)), async () => (await this.store.GeoContains<Zone>(q)).Select(r => r.Document.Id), "Contains");
        await Assert2(() => L(z => DocumentFunctions.Within(z.Area!, q)), async () => (await this.store.GeoContainedBy<Zone>(q)).Select(r => r.Document.Id), "Within/ContainedBy");
        await Assert2(() => L(z => DocumentFunctions.Disjoint(z.Area!, q)), async () => (await this.store.GeoDisjoint<Zone>(q)).Select(r => r.Document.Id), "Disjoint");
        await Assert2(() => L(z => DocumentFunctions.Touches(z.Area!, q)), async () => (await this.store.GeoTouches<Zone>(q)).Select(r => r.Document.Id), "Touches");
        await Assert2(() => L(z => DocumentFunctions.Crosses(z.Area!, q)), async () => (await this.store.GeoCrosses<Zone>(q)).Select(r => r.Document.Id), "Crosses");
        await Assert2(() => L(z => DocumentFunctions.Overlaps(z.Area!, q)), async () => (await this.store.GeoOverlaps<Zone>(q)).Select(r => r.Document.Id), "Overlaps");
        await Assert2(() => L(z => DocumentFunctions.GeoEquals(z.Area!, q)), async () => (await this.store.GeoEquals<Zone>(q)).Select(r => r.Document.Id), "Equals");
        await Assert2(() => L(z => DocumentFunctions.Covers(z.Area!, q)), async () => (await this.store.GeoCovers<Zone>(q)).Select(r => r.Document.Id), "Covers");
        await Assert2(() => L(z => DocumentFunctions.CoveredBy(z.Area!, q)), async () => (await this.store.GeoCoveredBy<Zone>(q)).Select(r => r.Document.Id), "CoveredBy");
        await Assert2(() => L(z => DocumentFunctions.WithinDistance(z.Area!, q, 300_000)), async () => (await this.store.GeoWithinDistance<Zone>(q, 300_000)).Select(r => r.Document.Id), "WithinDistance");
    }
}
