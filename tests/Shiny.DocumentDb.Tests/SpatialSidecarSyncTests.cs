using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// The spatial index is a sidecar (R*Tree on SQLite), so every write kind has to leave it agreeing with the
// document body. The rule mirrors blobs: a write that carries the whole document (insert, replace) may drop
// the index row when the geometry is gone, but an RFC 7396 merge must not — the patch only carries what it
// changes, and an omitted geometry stays put in the body.
[Collection("SQLite")]
public class SpatialSidecarSyncTests : IDisposable
{
    public class Zone
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public Geometry? Area { get; set; }
    }

    readonly DocumentStore store;
    readonly SqliteDatabaseFixture fixture;

    public SpatialSidecarSyncTests(SqliteDatabaseFixture fixture)
    {
        this.fixture = fixture;
        this.store = this.NewStore();
    }

    DocumentStore NewStore()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.fixture.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.ConfigureDocument<Zone>(cfg => cfg.MapSpatialProperty(z => z.Area));
        return new DocumentStore(opts);
    }

    public void Dispose() => this.store.Dispose();

    static GeoPolygon Square(double minLng, double minLat, double maxLng, double maxLat) =>
        new(new GeoPoint[]
        {
            new(minLat, minLng), new(minLat, maxLng), new(maxLat, maxLng), new(maxLat, minLng), new(minLat, minLng)
        });

    Task<IReadOnlyList<SpatialResult<Zone>>> Inside() => this.store.GeoIntersects<Zone>((Geometry)new GeoPoint(0.5, 0.5));

    [Fact]
    public async Task Upsert_merge_keeps_the_index_when_the_patch_omits_the_geometry()
    {
        await this.store.Upsert(new Zone { Id = "z1", Name = "A", Area = Square(0, 0, 1, 1) });
        Assert.Single(await this.Inside());

        // Name-only patch: Area is absent, so the merge leaves the stored polygon alone...
        await this.store.Upsert(new Zone { Id = "z1", Name = "B" });

        var stored = await this.store.Get<Zone>("z1");
        Assert.Equal("B", stored!.Name);
        Assert.NotNull(stored.Area);          // ...body still has it...
        Assert.Single(await this.Inside());   // ...and so must the index.
    }

    [Fact]
    public async Task Update_merge_keeps_the_index_when_the_patch_omits_the_geometry()
    {
        await this.store.Insert(new Zone { Id = "z1", Name = "A", Area = Square(0, 0, 1, 1) });
        Assert.Single(await this.Inside());

        await this.store.Update(new Zone { Id = "z1", Name = "B" }, patch: true);

        Assert.NotNull((await this.store.Get<Zone>("z1"))!.Area);
        Assert.Single(await this.Inside());
    }

    [Fact]
    public async Task Merge_still_reindexes_a_geometry_the_patch_does_carry()
    {
        await this.store.Upsert(new Zone { Id = "z1", Name = "A", Area = Square(0, 0, 1, 1) });
        Assert.Single(await this.Inside());

        // Move it far away — the index must follow the new polygon, not keep the old one.
        await this.store.Upsert(new Zone { Id = "z1", Area = Square(10, 10, 11, 11) });

        Assert.Empty(await this.Inside());
        Assert.Single(await this.store.GeoIntersects<Zone>((Geometry)new GeoPoint(10.5, 10.5)));
    }

    [Fact]
    public async Task Update_replace_drops_the_index_when_the_geometry_is_cleared()
    {
        await this.store.Insert(new Zone { Id = "z1", Name = "A", Area = Square(0, 0, 1, 1) });
        Assert.Single(await this.Inside());

        // Replace carries the whole document, so a null Area genuinely means "no location".
        await this.store.Update(new Zone { Id = "z1", Name = "B", Area = null });

        Assert.Empty(await this.Inside());
    }

    [Fact]
    public async Task Upsert_replace_drops_the_index_when_the_geometry_is_cleared()
    {
        await this.store.Upsert(new Zone { Id = "z1", Name = "A", Area = Square(0, 0, 1, 1) });
        Assert.Single(await this.Inside());

        await this.store.Upsert(new Zone { Id = "z1", Name = "B", Area = null }, patchIfUpdate: false);

        Assert.Empty(await this.Inside());
    }

    [Fact]
    public async Task Merge_inside_a_session_keeps_the_index_too()
    {
        await this.store.Upsert(new Zone { Id = "z1", Name = "A", Area = Square(0, 0, 1, 1) });

        await this.store.OpenSession()
            .Upsert(new Zone { Id = "z1", Name = "B" })
            .SaveChanges();

        Assert.NotNull((await this.store.Get<Zone>("z1"))!.Area);
        Assert.Single(await this.Inside());
    }

    [Fact]
    public async Task Replace_inside_a_session_drops_a_cleared_index_row()
    {
        await this.store.Upsert(new Zone { Id = "z1", Name = "A", Area = Square(0, 0, 1, 1) });

        // The buffered session verb is a replace (Update), not a merge.
        await this.store.OpenSession()
            .Update(new Zone { Id = "z1", Name = "B", Area = null })
            .SaveChanges();

        Assert.Empty(await this.Inside());
    }
}
