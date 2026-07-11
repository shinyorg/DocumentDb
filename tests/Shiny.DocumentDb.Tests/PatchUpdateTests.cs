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
        public Meta? Meta { get; set; }
        public string[]? Tags { get; set; }
    }

    sealed class Meta
    {
        public string? A { get; set; }
        public string? B { get; set; }
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

    [Fact]
    public async Task Update_Patch_DeepMergesNestedObject_PreservesSiblings()
    {
        await this.store.Insert(new Thing { Id = "t1", Name = "A", Meta = new Meta { A = "x", B = "y" } });
        // Patch only Meta.A; Meta.B (null in the patch) must be preserved by the recursive merge, not dropped
        // by replacing the whole Meta object.
        await this.store.Update(new Thing { Id = "t1", Meta = new Meta { A = "z" } }, patch: true);
        var got = await this.store.Get<Thing>("t1");
        Assert.Equal("z", got!.Meta!.A);
        Assert.Equal("y", got.Meta.B);   // sibling preserved by deep merge
        Assert.Equal("A", got.Name);
    }

    [Fact]
    public async Task Update_Patch_ReplacesArrayWholesale_NoConcat()
    {
        await this.store.Insert(new Thing { Id = "t1", Tags = ["a", "b"] });
        await this.store.Update(new Thing { Id = "t1", Tags = ["c"] }, patch: true);
        var got = await this.store.Get<Thing>("t1");
        Assert.Equal(["c"], got!.Tags);   // arrays replace, they do not merge/concat (RFC 7396)
    }

    [Fact]
    public async Task JsonLane_Patch_NullValue_DoesNotDeleteKey()
    {
        await this.Seed();
        // Intentional deviation from strict RFC 7396: a null in the patch is a no-op (removal goes through
        // RemoveProperty), so Color must remain unchanged rather than being deleted/nulled.
        await this.store.Update(typeof(Thing), new JsonObject { ["id"] = "t1", ["color"] = null }, patch: true);
        var got = await this.store.Get<Thing>("t1");
        Assert.Equal("red", got!.Color);
    }
}

/// <summary>DB-free assertion that the read-modify-write merge/replace fallback uses a row-locking SELECT on
/// every pooled relational provider — otherwise concurrent Update(patch)/Upsert(patchIfUpdate:false) lose
/// writes. SQLite/DuckDB are exempt (serialized by a single-connection semaphore).</summary>
public class PatchLockingSqlTests
{
    [Fact]
    public void MySql_And_MariaDb_LockRowForUpdate()
    {
        Assert.Contains("FOR UPDATE", new Shiny.DocumentDb.MySql.MySqlDatabaseProvider("Server=x").BuildSelectDataForUpdateSql("t"));
        Assert.Contains("FOR UPDATE", new Shiny.DocumentDb.MariaDb.MariaDbDatabaseProvider("Server=x").BuildSelectDataForUpdateSql("t"));
    }

    [Fact]
    public void Oracle_LocksRowForUpdate()
        => Assert.Contains("FOR UPDATE", new Shiny.DocumentDb.Oracle.OracleDatabaseProvider("User Id=x;Password=y;Data Source=z").BuildSelectDataForUpdateSql("t"));

    [Fact]
    public void Postgres_And_Cockroach_LockRowForUpdate()
    {
        Assert.Contains("FOR UPDATE", new Shiny.DocumentDb.PostgreSql.PostgreSqlDatabaseProvider("Host=x").BuildSelectDataForUpdateSql("t"));
        Assert.Contains("FOR UPDATE", new Shiny.DocumentDb.CockroachDb.CockroachDbDatabaseProvider("Host=x").BuildSelectDataForUpdateSql("t"));
    }

    [Fact]
    public void SqlServer_LocksWithUpdlock()
        => Assert.Contains("UPDLOCK", new Shiny.DocumentDb.SqlServer.SqlServerDatabaseProvider("Server=x").BuildSelectDataForUpdateSql("t"));
}
