using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The reads behind the per-row Blobs and Geometry links in Browse. Both sidecar views are keyed by
/// document id, so a row link is the whole-type view narrowed to one id - and these prove the narrowing
/// actually happens rather than the link quietly opening the unfiltered view.
/// </summary>
public sealed class DocumentScopedSidecarTests : IAsyncLifetime
{
    sealed class Place
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public GeoPoint Location { get; set; }
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
            // Demo mode off: these exercise the write paths a demo instance closes.
            new DemoMode(configuration, NullLogger<DemoMode>.Instance));

        this.profileId = provided.Profiles.Single().Id;
        this.connections = new ConnectionManager(profiles, NullLogger<ConnectionManager>.Instance);
        this.admin = new DocumentAdminService(this.connections, NullLogger<DocumentAdminService>.Instance);
    }

    /// <summary>Three places, each with a point and its own attachment, written through the library.</summary>
    async Task Seed()
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.databasePath}"),
            TableName = Table
        };
        options.ConfigureDocument<Place>(cfg => cfg.MapBlob(p => p.Attachment));

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
        if (Directory.Exists(this.directory))
        {
            try { Directory.Delete(this.directory, recursive: true); }
            catch (IOException) { /* a driver still holding the file is not a test failure */ }
        }

        return ValueTask.CompletedTask;
    }

    static BrowseQuery Scoped(string? documentId) => new()
    {
        Sort = new BrowseSort("Id", false, true),
        Filters = documentId is null ? [] : [new BrowseFilter("Id", FilterOperator.Equals, documentId)]
    };

    // ── Geometry ────────────────────────────────────────────────────────

    [Fact]
    public async Task Geometry_IsFoundAtTheMappedPath()
    {
        var paths = await this.admin.FindGeometryPaths(this.profileId, Table, TypeName);

        var found = Assert.Single(paths);
        Assert.Equal("location", found.Path, ignoreCase: true);
        Assert.Equal("Point", found.Kind);
    }

    [Fact]
    public async Task Geometry_UnscopedDrawsEveryDocument()
    {
        var path = (await this.admin.FindGeometryPaths(this.profileId, Table, TypeName)).Single().Path;

        var set = await this.admin.LoadGeometry(this.profileId, Table, TypeName, path, Scoped(null));

        Assert.Equal(3, set.Features.Count);
        Assert.Equal(3, set.DocumentsScanned);
    }

    [Fact]
    public async Task Geometry_ScopedToOneDocument_DrawsOnlyThatDocument()
    {
        var path = (await this.admin.FindGeometryPaths(this.profileId, Table, TypeName)).Single().Path;

        var set = await this.admin.LoadGeometry(this.profileId, Table, TypeName, path, Scoped("cp"));

        var feature = Assert.Single(set.Features);
        Assert.Equal("cp", feature.DocumentId);

        // Scanned, not just drawn: the filter has to reach the query, or a large type would still be
        // read end to end to render one shape.
        Assert.Equal(1, set.DocumentsScanned);
    }

    [Fact]
    public async Task Geometry_ScopedToAnUnknownDocument_IsEmptyRatherThanEverything()
    {
        var path = (await this.admin.FindGeometryPaths(this.profileId, Table, TypeName)).Single().Path;

        var set = await this.admin.LoadGeometry(this.profileId, Table, TypeName, path, Scoped("nope"));

        Assert.True(set.IsEmpty);
    }

    // ── Blobs ───────────────────────────────────────────────────────────

    [Fact]
    public async Task Blobs_UnscopedListsEveryDocumentsPayloads()
    {
        var blobs = await this.admin.ListBlobs(this.profileId, Table, TypeName);

        Assert.Equal(3, blobs.Count);
        Assert.Equal(["cp", "lax", "ts"], blobs.Select(b => b.DocumentId).Order());
    }

    [Fact]
    public async Task Blobs_ScopedToOneDocument_ListsOnlyThatDocumentsPayloads()
    {
        var blobs = await this.admin.ListBlobs(this.profileId, Table, TypeName, "cp");

        var blob = Assert.Single(blobs);
        Assert.Equal("cp", blob.DocumentId);
        Assert.Equal("cp.txt", blob.FileName);
    }

    [Fact]
    public async Task Blobs_ScopedToAnUnknownDocument_IsEmptyRatherThanEverything()
        => Assert.Empty(await this.admin.ListBlobs(this.profileId, Table, TypeName, "nope"));
}
