using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Geo;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Geo.Tests;

public class GeoReferenceTests : IDisposable
{
    readonly SqliteConnection holdOpen;
    readonly DocumentStore store;

    public GeoReferenceTests()
    {
        var cs = $"Data Source=geo_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(cs);
        this.holdOpen.Open();

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(cs),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.MapGeoReferenceData();
        this.store = new DocumentStore(opts);
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.holdOpen.Dispose();
    }

    [Fact]
    public void DataSets_Have_Expected_Counts()
    {
        // 50 states + DC = 51 US regions; 13 Canadian provinces/territories.
        Assert.Equal(51, GeoDataSets.Regions.Count(r => r.CountryCode == GeoDataSets.UnitedStates));
        Assert.Equal(13, GeoDataSets.Regions.Count(r => r.CountryCode == GeoDataSets.Canada));
        Assert.All(GeoDataSets.Cities, c => Assert.False(string.IsNullOrWhiteSpace(c.Name)));
        Assert.NotEmpty(GeoDataSets.Cities);
    }

    [Fact]
    public void Ids_Are_Unique()
    {
        var regionIds = GeoDataSets.Regions.Select(r => r.Id).ToArray();
        Assert.Equal(regionIds.Length, regionIds.Distinct().Count());

        var cityIds = GeoDataSets.Cities.Select(c => c.Id).ToArray();
        Assert.Equal(cityIds.Length, cityIds.Distinct().Count());
    }

    [Fact]
    public void Boundaries_Are_Closed_Rings()
    {
        foreach (var region in GeoDataSets.Regions)
        {
            var ring = Assert.IsType<GeoPolygon>(region.Boundary).ExteriorRing;
            Assert.True(ring.Count >= 4, $"{region.Id} ring too short");
            Assert.Equal(ring[0], ring[^1]); // closed
        }
    }

    [Fact]
    public void City_Location_Preserves_Lat_Lon_Axis_Order()
    {
        var denver = GeoDataSets.Cities.Single(c => c.Id == "US-CO-denver");
        Assert.Equal(39.7392, denver.Location.Latitude, 3);
        Assert.Equal(-104.9903, denver.Location.Longitude, 3);
    }

    [Fact]
    public async Task Seeder_Populates_Store()
    {
        var ct = TestContext.Current.CancellationToken;
        await new GeoReferenceSeeder().SeedAsync(this.store, ct);

        Assert.Equal(GeoDataSets.Regions.Count, await this.store.Count<GeoRegion>(cancellationToken: ct));
        Assert.Equal(GeoDataSets.Cities.Count, await this.store.Count<GeoCity>(cancellationToken: ct));
    }

    [Fact]
    public async Task Seeder_Is_Idempotent()
    {
        var ct = TestContext.Current.CancellationToken;
        var seeder = new GeoReferenceSeeder();
        await seeder.SeedAsync(this.store, ct);
        await seeder.SeedAsync(this.store, ct);

        Assert.Equal(GeoDataSets.Regions.Count, await this.store.Count<GeoRegion>(cancellationToken: ct));
    }

    [Fact]
    public async Task Point_Falls_Inside_Its_Region()
    {
        var ct = TestContext.Current.CancellationToken;
        await new GeoReferenceSeeder().SeedAsync(this.store, ct);

        // Denver sits well inside Colorado's rectangular boundary.
        Geometry denver = new GeoPoint(39.7392, -104.9903);
        var hits = await this.store.GeoIntersects<GeoRegion>(denver, ct: ct);

        Assert.Contains(hits, h => h.Document.Id == "US-CO");
        Assert.DoesNotContain(hits, h => h.Document.Id == "US-WY"); // neighbouring state, not a match
    }

    [Fact]
    public async Task Nearby_Cities_Found_By_Distance()
    {
        var ct = TestContext.Current.CancellationToken;
        await new GeoReferenceSeeder().SeedAsync(this.store, ct);

        Geometry origin = new GeoPoint(40.7128, -74.0060); // New York City
        var hits = await this.store.GeoWithinDistance<GeoCity>(origin, 50_000, orderByDistanceFrom: origin, ct: ct);

        Assert.NotEmpty(hits);
        Assert.Equal("US-NY-new-york", hits[0].Document.Id); // closest to the origin
    }
}
