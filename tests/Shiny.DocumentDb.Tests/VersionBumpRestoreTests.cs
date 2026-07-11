using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Regression: Update bumped the caller's in-memory version *before* the optimistic-concurrency write, so a
/// failed CAS left the object at version+1 — a naive retry would then send a doubly-bumped version. The version
/// is now restored on <see cref="ConcurrencyException"/>.
/// </summary>
public class VersionBumpRestoreTests : IDisposable
{
    sealed class Doc
    {
        public string Id { get; set; } = "";
        public string? Name { get; set; }
        public int Version { get; set; }
    }

    readonly SqliteConnection hold;
    readonly DocumentStore store;

    public VersionBumpRestoreTests()
    {
        var cs = $"Data Source=vbump_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        this.hold = new SqliteConnection(cs);
        this.hold.Open();
        var opts = new DocumentStoreOptions { DatabaseProvider = new SqliteDatabaseProvider(cs), TableName = $"t{Guid.NewGuid():N}" };
        opts.MapVersionProperty<Doc>(x => x.Version);
        this.store = new DocumentStore(opts);
    }

    public void Dispose()
    {
        this.store.Dispose();
        this.hold.Dispose();
    }

    [Fact]
    public async Task Update_ConcurrencyFailure_RestoresInMemoryVersion()
    {
        await this.store.Insert(new Doc { Id = "d1", Name = "v1" });   // stored version -> 1

        var stale = await this.store.Get<Doc>("d1");   // version 1
        await this.store.Update(new Doc { Id = "d1", Name = "v2", Version = 1 });   // stored -> 2

        // 'stale' still carries version 1; updating it must fail the CAS and leave its version at 1 (restored),
        // not the pre-write bump of 2.
        await Assert.ThrowsAsync<ConcurrencyException>(() => this.store.Update(stale!));
        Assert.Equal(1, stale!.Version);
    }
}
