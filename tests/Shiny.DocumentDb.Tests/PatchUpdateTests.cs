using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Update(patch) and Upsert(patchIfUpdate) — the two write-mode axes (merge vs replace, update-only vs
/// insert-or-update) on the relational core, incl. the late-bound JSON lane.
/// </summary>
public class PatchUpdateTests : IDisposable
{
    sealed class Thing
    {
        public string Id { get; set; } = "";
        public string? Name { get; set; }
        public string? Color { get; set; }
        public int? Count { get; set; }
    }

    readonly SqliteConnection holdOpen;
    readonly DocumentStore store;

    public PatchUpdateTests()
    {
        var cs = $"Data Source=patch_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.holdOpen = new SqliteConnection(cs);
        this.holdOpen.Open();
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(cs),
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.holdOpen.Dispose();
    }

    Task Seed() => this.store.Insert(new Thing { Id = "t1", Name = "A", Color = "red", Count = 5 });

    [Fact]
    public async Task Update_Replace_OverwritesOmittedFields()
    {
        await this.Seed();
        await this.store.Update(new Thing { Id = "t1", Name = "B" }, patch: false);
        var got = await this.store.Get<Thing>("t1");
        Assert.Equal("B", got!.Name);
        Assert.Null(got.Color);   // replaced wholesale — Color is gone
        Assert.Null(got.Count);
    }

    [Fact]
    public async Task Update_Patch_MergesOnlyProvidedFields()
    {
        await this.Seed();
        await this.store.Update(new Thing { Id = "t1", Name = "B" }, patch: true);
        var got = await this.store.Get<Thing>("t1");
        Assert.Equal("B", got!.Name);   // changed
        Assert.Equal("red", got.Color); // preserved (null in patch → ignored)
        Assert.Equal(5, got.Count);     // preserved
    }

    [Fact]
    public async Task Update_Patch_OnMissing_Throws()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            this.store.Update(new Thing { Id = "nope", Name = "x" }, patch: true));
    }

    [Fact]
    public async Task Upsert_Replace_OverwritesOnUpdate()
    {
        await this.Seed();
        await this.store.Upsert(new Thing { Id = "t1", Name = "B" }, patchIfUpdate: false);
        var got = await this.store.Get<Thing>("t1");
        Assert.Equal("B", got!.Name);
        Assert.Null(got.Color);   // replaced, not merged
    }

    [Fact]
    public async Task Upsert_Replace_InsertsWhenAbsent()
    {
        await this.store.Upsert(new Thing { Id = "new", Name = "X", Color = "blue" }, patchIfUpdate: false);
        var got = await this.store.Get<Thing>("new");
        Assert.Equal("X", got!.Name);
        Assert.Equal("blue", got.Color);
    }

    [Fact]
    public async Task Upsert_Merge_Default_PreservesUnsetFields()
    {
        await this.Seed();
        await this.store.Upsert(new Thing { Id = "t1", Name = "B" }, patchIfUpdate: true);
        var got = await this.store.Get<Thing>("t1");
        Assert.Equal("B", got!.Name);
        Assert.Equal("red", got.Color); // merge default preserves
    }

    [Fact]
    public async Task JsonLane_Update_Patch_MergesPartialObject()
    {
        await this.Seed();
        var n = await this.store.Update(typeof(Thing), new JsonObject { ["id"] = "t1", ["name"] = "B" }, patch: true);
        Assert.Equal(1, n);
        var got = await this.store.Get<Thing>("t1");
        Assert.Equal("B", got!.Name);
        Assert.Equal("red", got.Color); // partial JsonObject merged — Color untouched
        Assert.Equal(5, got.Count);
    }
}
