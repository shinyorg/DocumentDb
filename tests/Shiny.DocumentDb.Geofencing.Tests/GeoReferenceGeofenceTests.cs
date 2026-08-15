using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Geo;
using Shiny.DocumentDb.Sqlite;
using Shiny.Extensions.Stores;
using Xunit;

namespace Shiny.DocumentDb.Geofencing.Tests;

/// <summary>
/// The shipped US/Canada reference dataset used as a geofence source - state boundaries in containment mode
/// and city centres in proximity mode, together, off one store.
/// </summary>
public class GeoReferenceGeofenceTests : IAsyncLifetime
{
    static readonly GeoPoint Denver = new(39.7392, -104.9903);
    static readonly GeoPoint Casper = new(42.8666, -106.3131);   // Wyoming
    static readonly GeoPoint Toronto = new(43.6532, -79.3832);

    readonly SqliteConnection holdOpen;
    readonly DocumentStore store;
    readonly MemoryKeyValueStore state = new();
    readonly RecordingDelegate handler = new();


    public GeoReferenceGeofenceTests()
    {
        var cs = $"Data Source=geofence_ref_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
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


    public ValueTask InitializeAsync()
        => new(new GeoReferenceSeeder().SeedAsync(this.store, TestContext.Current.CancellationToken));


    public ValueTask DisposeAsync()
    {
        this.store.Dispose();
        this.holdOpen.Dispose();
        return default;
    }


    DocumentRegionEvaluator Evaluator() => Geo.Evaluator(
        this.store,
        this.state,
        this.handler,
        new DocumentGeofenceConfig()
            .AddRegionSet<GeoRegion>("regions", r => r.Id, r => r.Name)
            .AddRegionSet<GeoCity>("cities", c => c.Id, c => c.Name, withinMeters: 25_000)
    );


    [Fact]
    public async Task Driving_Denver_To_Casper_Crosses_The_State_Line()
    {
        var ct = TestContext.Current.CancellationToken;
        var evaluator = this.Evaluator();

        var arrival = await evaluator.Evaluate(Denver, ct);
        Assert.Contains(arrival, c => c is { RegionSet: "regions", Entered: true, RegionId: "US-CO" });
        Assert.Contains(arrival, c => c is { RegionSet: "cities", Entered: true, RegionId: "US-CO-denver" });

        var departure = await evaluator.Evaluate(Casper, ct);
        Assert.Contains(departure, c => c is { RegionSet: "regions", Entered: false, RegionId: "US-CO" });
        Assert.Contains(departure, c => c is { RegionSet: "regions", Entered: true, RegionId: "US-WY" });
        Assert.Contains(departure, c => c is { RegionSet: "cities", Entered: false, RegionId: "US-CO-denver" });

        // The exit always precedes the entry for the same set.
        var regionChanges = departure.Where(c => c.RegionSet == "regions").ToList();
        Assert.False(regionChanges[0].Entered);
        Assert.True(regionChanges[1].Entered);
    }


    [Fact]
    public async Task Regions_Carry_Their_Document_Across_Countries()
    {
        var ct = TestContext.Current.CancellationToken;

        var changes = await this.Evaluator().Evaluate(Toronto, ct);

        var region = Assert.Single(changes, c => c.RegionSet == "regions");
        Assert.Equal("CA-ON", region.RegionId);
        Assert.Equal("Ontario", region.RegionAs<GeoRegion>()!.Name);
        Assert.Equal("CA", region.RegionAs<GeoRegion>()!.CountryCode);
    }
}
