using System.Text;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// What a delete takes with it. None of these sidecars has a foreign key back to the document, so a row left
/// behind is not a constraint violation - it is a bounding box that resolves to nothing, an embedding the ANN
/// index still returns, and a payload no reader can reach. Deleting through the tool has to leave the database
/// in the state the library would have left it.
/// </summary>
public sealed class DeleteCleanupTests : IAsyncLifetime
{
    sealed class Place
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public GeoPoint? Location { get; set; }
        public DocumentBlob? Attachment { get; set; }
    }

    const string Table = "documents";
    const string TypeName = nameof(Place);

    string directory = "";
    string databasePath = "";
    string profileId = "";
    DocumentAdminService admin = null!;
    ConnectionManager connections = null!;

    public async ValueTask InitializeAsync()
    {
        this.directory = Path.Combine(Path.GetTempPath(), $"docdbadmin_{Guid.NewGuid():N}");
        Directory.CreateDirectory(this.directory);
        this.databasePath = Path.Combine(this.directory, "store.db");

        await this.Seed();

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ShinyDocDbMyAdmin:DataDirectory"] = Path.Combine(this.directory, "admin"),
                ["ShinyDocDbMyAdmin:SecretKey"] = "test-key",
                ["ConnectionStrings:store"] = $"Data Source={this.databasePath}",
                ["Shiny:DocumentDb:store:Provider"] = "Sqlite"
            })
            .Build();

        var paths = new AppPaths(configuration);
        var protector = new SecretProtector(configuration, paths, NullLogger<SecretProtector>.Instance);
        var provided = new ProvidedConnections(configuration, NullLogger<ProvidedConnections>.Instance);
        var profiles = new ProfileStore(paths, protector, provided,
            new DemoMode(configuration, NullLogger<DemoMode>.Instance));

        this.profileId = provided.Profiles.Single().Id;
        this.connections = new ConnectionManager(profiles, NullLogger<ConnectionManager>.Instance);
        this.admin = new DocumentAdminService(this.connections, NullLogger<DocumentAdminService>.Instance);
    }

    /// <summary>Three places, spatially indexed, each with a payload and a history sidecar.</summary>
    async Task Seed()
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.databasePath}"),
            TableName = Table
        };
        options.ConfigureDocument<Place>(cfg => cfg
            .MapSpatialProperty(p => p.Location)
            .MapBlob(p => p.Attachment)
            .MapTemporal());

        using var store = new DocumentStore(options);
        await store.Insert(Make("ts", "TimesSquare", new GeoPoint(40.7580, -73.9855)));
        await store.Insert(Make("cp", "CentralPark", new GeoPoint(40.7829, -73.9654)));
        await store.Insert(Make("lax", "LAX", new GeoPoint(33.9416, -118.4085)));
    }

    static Place Make(string id, string name, GeoPoint point) => new()
    {
        Id = id,
        Name = name,
        Location = point,
        Attachment = DocumentBlob.FromBytes(Encoding.UTF8.GetBytes($"notes for {id}"), "text/plain", $"{id}.txt")
    };

    public ValueTask DisposeAsync()
    {
        this.connections?.Dispose();
        SqliteConnection.ClearAllPools();
        if (Directory.Exists(this.directory))
        {
            try { Directory.Delete(this.directory, recursive: true); }
            catch (IOException) { /* a driver still holding the file is not a test failure */ }
        }

        return ValueTask.CompletedTask;
    }

    async Task<long> Scalar(string sql)
    {
        await using var db = new SqliteConnection($"Data Source={this.databasePath}");
        await db.OpenAsync();
        await using var cmd = db.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    Task<long> SpatialRows() => this.Scalar($"SELECT COUNT(*) FROM {Table}_spatial");

    Task<long> SpatialMapRows() => this.Scalar($"SELECT COUNT(*) FROM {Table}_spatial_map");

    Task<long> BlobRows() => this.Scalar($"SELECT COUNT(*) FROM {Table}_blobs");

    // ── One document ────────────────────────────────────────────────────

    [Fact]
    public async Task Delete_RemovesTheSpatialIndexRowAndItsMapRow()
    {
        Assert.Equal(3, await this.SpatialRows());

        await this.admin.DeleteDocuments(this.profileId, Table, TypeName, ["cp"]);

        // Both halves: SQLite's R*Tree row is keyed by a rowid that only the map table resolves, so
        // leaving either behind leaves the index inconsistent.
        Assert.Equal(2, await this.SpatialRows());
        Assert.Equal(2, await this.SpatialMapRows());
    }

    [Fact]
    public async Task Delete_RemovesTheDocumentsBlobs()
    {
        Assert.Equal(3, await this.BlobRows());

        await this.admin.DeleteDocuments(this.profileId, Table, TypeName, ["cp"]);

        Assert.Equal(2, await this.BlobRows());
    }

    [Fact]
    public async Task Delete_LeavesTheOtherDocumentsSidecarRowsAlone()
    {
        await this.admin.DeleteDocuments(this.profileId, Table, TypeName, ["cp"]);

        var geometry = await this.admin.LoadGeometry(
            this.profileId, Table, TypeName, "location",
            new BrowseQuery { Sort = new BrowseSort("Id", false, true) });

        Assert.Equal(2, geometry.Features.Count);
        Assert.DoesNotContain(geometry.Features, f => f.DocumentId == "cp");
    }

    [Fact]
    public async Task Delete_AppendsARemovedVersionToHistory()
    {
        await this.admin.DeleteDocuments(this.profileId, Table, TypeName, ["cp"]);

        // Ordered Version ASC, so the tombstone the delete wrote is the last one.
        var versions = await this.admin.ListVersions(this.profileId, Table, TypeName, "cp");

        var tombstone = versions[^1];
        Assert.Equal("Removed", tombstone.Operation);
        Assert.True(tombstone.IsTombstone);
        Assert.Equal(DocumentAdminService.HistoryActor, tombstone.Actor);
    }

    // ── The whole type ──────────────────────────────────────────────────

    [Fact]
    public async Task ClearType_TakesEverySidecarRowWithIt()
    {
        await this.admin.ClearType(this.profileId, Table, TypeName);

        Assert.Equal(0, await this.SpatialRows());
        Assert.Equal(0, await this.SpatialMapRows());
        Assert.Equal(0, await this.BlobRows());
    }

    // ── The sidecar probe the delete dialog asks ────────────────────────

    [Fact]
    public async Task HasSpatialSidecar_IsTrueForASpatiallyMappedTable()
        => Assert.True(await this.admin.HasSpatialSidecar(this.profileId, Table));
}
