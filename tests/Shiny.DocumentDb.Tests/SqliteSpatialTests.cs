using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// Spatial query coverage for the only provider that implements them (SQLite R*Tree).
/// </summary>
public class SqliteSpatialTests : IDisposable
{
    sealed class Place
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public GeoPoint Location { get; set; }
    }

    readonly SqliteConnection holdOpen;
    readonly DocumentStore store;

    public SqliteSpatialTests()
    {
        // Hold a named in-memory db open for the duration of the test so the store can attach
        // a fresh connection that sees the same database.
        var connectionString = $"Data Source=spatial_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(connectionString);
        this.holdOpen.Open();

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connectionString),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.MapSpatialProperty<Place>(p => p.Location);
        this.store = new DocumentStore(opts);
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.holdOpen.Dispose();
    }

    // Coordinates around midtown Manhattan as test fixtures.
    static readonly GeoPoint TimesSquare = new(40.7580, -73.9855);
    static readonly GeoPoint CentralPark = new(40.7829, -73.9654);   // ~3 km north
    static readonly GeoPoint StatueOfLiberty = new(40.6892, -74.0445); // ~8.5 km south
    static readonly GeoPoint LAX = new(33.9416, -118.4085);            // ~3,900 km away

    async Task SeedAsync()
    {
        await this.store.Insert(new Place { Id = "ts", Name = "TimesSquare", Location = TimesSquare });
        await this.store.Insert(new Place { Id = "cp", Name = "CentralPark", Location = CentralPark });
        await this.store.Insert(new Place { Id = "sol", Name = "StatueOfLiberty", Location = StatueOfLiberty });
        await this.store.Insert(new Place { Id = "lax", Name = "LAX", Location = LAX });
    }

    [Fact]
    public async Task BatchInsert_PopulatesSpatialIndex()
    {
        // Regression: BatchInsert skipped the spatial sidecar, so batch-inserted points were never in the
        // R*Tree and spatial queries couldn't find them. They must now be spatially queryable.
        await this.store.BatchInsert(new[]
        {
            new Place { Id = "ts", Name = "TimesSquare", Location = TimesSquare },
            new Place { Id = "cp", Name = "CentralPark", Location = CentralPark },
            new Place { Id = "lax", Name = "LAX", Location = LAX }
        });

        var results = await this.store.WithinRadius<Place>(TimesSquare, 5_000);
        Assert.Equal(new[] { "CentralPark", "TimesSquare" }, results.Select(r => r.Document.Name).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task WithinRadius_ReturnsOnlyPointsInside_OrderedByDistance()
    {
        await this.SeedAsync();
        var results = await this.store.WithinRadius<Place>(TimesSquare, 5_000); // 5 km

        Assert.Equal(2, results.Count);
        Assert.Equal("TimesSquare", results[0].Document.Name);
        Assert.Equal("CentralPark", results[1].Document.Name);
        Assert.True(results[0].DistanceMeters < results[1].DistanceMeters);
    }

    [Fact]
    public async Task WithinRadius_NoMatches_ReturnsEmpty()
    {
        await this.SeedAsync();
        var results = await this.store.WithinRadius<Place>(new GeoPoint(0, 0), 1_000);
        Assert.Empty(results);
    }

    [Fact]
    public async Task WithinRadius_WithPredicateFilter()
    {
        await this.SeedAsync();
        var results = await this.store.WithinRadius<Place>(
            TimesSquare, 5_000, p => p.Name == "CentralPark");

        Assert.Single(results);
        Assert.Equal("CentralPark", results[0].Document.Name);
    }

    [Fact]
    public async Task WithinBoundingBox_ReturnsMatches()
    {
        await this.SeedAsync();
        // Bounding box around midtown / lower Manhattan, excludes LAX.
        var box = new GeoBoundingBox(40.65, -74.10, 40.80, -73.90);
        var results = await this.store.WithinBoundingBox<Place>(box);

        Assert.Equal(3, results.Count);
        Assert.DoesNotContain(results, r => r.Name == "LAX");
    }

    [Fact]
    public async Task NearestNeighbors_ReturnsTopK_OrderedByDistance()
    {
        await this.SeedAsync();
        var results = await this.store.NearestNeighbors<Place>(TimesSquare, 2);

        Assert.Equal(2, results.Count);
        Assert.Equal("TimesSquare", results[0].Document.Name);
        Assert.Equal("CentralPark", results[1].Document.Name);
    }

    [Fact]
    public async Task Remove_AlsoRemovesFromSpatialIndex()
    {
        await this.SeedAsync();
        await this.store.Remove<Place>("cp");

        var results = await this.store.WithinRadius<Place>(TimesSquare, 5_000);
        Assert.Single(results);
        Assert.Equal("TimesSquare", results[0].Document.Name);
    }

    [Fact]
    public async Task Update_RewritesSpatialEntry()
    {
        await this.SeedAsync();
        // Move "cp" far away — it should no longer appear in a midtown 5km radius.
        await this.store.Update(new Place { Id = "cp", Name = "CentralPark", Location = LAX });

        var results = await this.store.WithinRadius<Place>(TimesSquare, 5_000);
        Assert.Single(results);
        Assert.Equal("TimesSquare", results[0].Document.Name);
    }

    [Fact]
    public async Task Clear_AlsoClearsSpatialIndex()
    {
        await this.SeedAsync();
        await this.store.Clear<Place>();

        var results = await this.store.WithinRadius<Place>(TimesSquare, 50_000);
        Assert.Empty(results);
    }
}

/// <summary>
/// Spatial coverage for documents whose GeoPoint is nullable — a null location must not throw on
/// write, must be excluded from results, and clearing it on update must purge the stale index row.
/// </summary>
public class SqliteNullableSpatialTests : IDisposable
{
    sealed class Event
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public GeoPoint? Location { get; set; }
    }

    readonly SqliteConnection holdOpen;
    readonly DocumentStore store;

    static readonly GeoPoint TimesSquare = new(40.7580, -73.9855);
    static readonly GeoPoint CentralPark = new(40.7829, -73.9654);

    public SqliteNullableSpatialTests()
    {
        var connectionString = $"Data Source=spatial_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(connectionString);
        this.holdOpen.Open();

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connectionString),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.MapSpatialProperty<Event>(e => e.Location);
        this.store = new DocumentStore(opts);
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.holdOpen.Dispose();
    }

    [Fact]
    public async Task Insert_NullLocation_DoesNotThrow_AndIsExcluded()
    {
        await this.store.Insert(new Event { Id = "a", Name = "NoCoords" });
        await this.store.Insert(new Event { Id = "b", Name = "HasCoords", Location = TimesSquare });

        var results = await this.store.WithinRadius<Event>(TimesSquare, 5_000);

        Assert.Single(results);
        Assert.Equal("HasCoords", results[0].Document.Name);
    }

    [Fact]
    public async Task Update_SetLocation_ThenAppearsInResults()
    {
        await this.store.Insert(new Event { Id = "a", Name = "NoCoords" });
        await this.store.Update(new Event { Id = "a", Name = "NoCoords", Location = CentralPark });

        var results = await this.store.WithinRadius<Event>(TimesSquare, 5_000);

        Assert.Single(results);
        Assert.Equal("NoCoords", results[0].Document.Name);
    }

    [Fact]
    public async Task Update_ClearLocation_PurgesStaleIndexRow()
    {
        await this.store.Insert(new Event { Id = "a", Name = "WasHere", Location = TimesSquare });
        Assert.Single(await this.store.WithinRadius<Event>(TimesSquare, 5_000));

        // Remove the coordinates — the R*Tree row must be purged, not left stale.
        await this.store.Update(new Event { Id = "a", Name = "WasHere", Location = null });

        var results = await this.store.WithinRadius<Event>(TimesSquare, 5_000);
        Assert.Empty(results);
    }
}
