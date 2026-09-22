using Shiny.DocumentDb.LiteDb;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// DocumentMetadata on the LiteDB provider beyond the shared conformance suite — the write and read paths that are
// LiteDB's own (upsert branch, batch insert, sessions, temporal sidecar, full-text, predicate-driven bulk writes).
public class LiteDbDocumentMetadataTests
{
    static LiteDbDocumentStore CreateStore(Action<LiteDbDocumentStoreOptions>? configure = null)
    {
        var opts = new LiteDbDocumentStoreOptions
        {
            ConnectionString = $"Filename={Path.GetTempFileName()};Connection=direct",
            CollectionName = $"t{Guid.NewGuid():N}"
        };
        configure?.Invoke(opts);
        return new LiteDbDocumentStore(opts);
    }

    [Fact]
    public async Task Upsert_ThatInserts_StampsCreatedAt()
    {
        using var store = CreateStore();
        var note = new StampedNote { Id = "a", Title = "new" };

        await store.Upsert(note);

        Assert.True(note.Metadata!.IsPersisted);
        Assert.Equal(note.Metadata.UpdatedAt, note.Metadata.CreatedAt);
        var loaded = await store.Get<StampedNote>("a");
        Assert.Equal(note.Metadata.CreatedAt, loaded!.Metadata!.CreatedAt);
    }

    [Fact]
    public async Task BatchInsert_StampsEveryInstance_WithTheStoredTimestamp()
    {
        using var store = CreateStore();
        var notes = Enumerable.Range(0, 4).Select(i => new StampedNote { Id = $"n{i}" }).ToList();

        await store.BatchInsert(notes);

        var loaded = await store.Query<StampedNote>().ToList();
        Assert.Equal(4, loaded.Count);
        Assert.All(loaded, n =>
        {
            var written = notes.Single(x => x.Id == n.Id).Metadata!;
            Assert.True(written.IsPersisted);
            Assert.Equal(written.CreatedAt, n.Metadata!.CreatedAt);
            Assert.Equal(written.UpdatedAt, n.Metadata.UpdatedAt);
        });
    }

    [Fact]
    public async Task Session_StampsOnSaveChanges()
    {
        using var store = CreateStore();
        var note = new StampedNote { Id = "a", Title = "unit" };
        // SaveChanges runs the pending writes as one LiteDB BeginTrans/Commit unit of work.
        await using (var session = store.OpenSession())
        {
            session.Add(note);
            await session.SaveChanges();
        }

        Assert.True(note.Metadata!.IsPersisted);
        var loaded = await store.Get<StampedNote>("a");
        Assert.Equal(note.Metadata.UpdatedAt, loaded!.Metadata!.UpdatedAt);
    }

    [Fact]
    public async Task TemporalSnapshots_NewUpButDoNotStamp()
    {
        using var store = CreateStore(o => o.ConfigureDocument<StampedNote>(cfg => cfg.MapTemporal()));
        await store.Insert(new StampedNote { Id = "a", Title = "v1" });
        var asOf = DateTimeOffset.UtcNow;
        await Task.Delay(20);

        var snapshot = await store.AsOf<StampedNote>("a", asOf);
        Assert.NotNull(snapshot!.Metadata);
        Assert.False(snapshot.Metadata!.IsPersisted);

        var history = await store.History<StampedNote>("a");
        Assert.NotEmpty(history);
        Assert.All(history, v => Assert.False(v.Document!.Metadata!.IsPersisted));
    }

    [Fact]
    public async Task Restore_KeepsTheLiveCreatedAt()
    {
        using var store = CreateStore(o => o.ConfigureDocument<StampedNote>(cfg => cfg.MapTemporal()));
        var note = new StampedNote { Id = "a", Title = "v1" };
        await store.Insert(note);
        await Task.Delay(20);
        note.Title = "v2";
        await store.Update(note);

        var restored = await store.Restore<StampedNote>("a", 1);

        var live = await store.Get<StampedNote>("a");
        Assert.Equal("v1", live!.Title);
        Assert.Equal(note.Metadata!.CreatedAt, live.Metadata!.CreatedAt);
        Assert.Equal(restored!.Metadata!.UpdatedAt, live.Metadata.UpdatedAt);
        Assert.True(live.Metadata.UpdatedAt > live.Metadata.CreatedAt);
    }

    [Fact]
    public async Task FullTextSearch_StampsResults()
    {
        using var store = CreateStore(o => o.ConfigureDocument<StampedNote>(cfg => cfg.MapFullTextProperty(x => x.Title)));
        var note = new StampedNote { Id = "a", Title = "quick brown fox" };
        await store.Insert(note);

        var results = await store.FullTextSearch<StampedNote>("fox");

        var hit = Assert.Single(results);
        Assert.True(hit.Document.Metadata!.IsPersisted);
        Assert.Equal(note.Metadata!.CreatedAt, hit.Document.Metadata.CreatedAt);
    }

    [Fact]
    public async Task ExecuteDelete_FiltersOnTheEnvelope()
    {
        using var store = CreateStore();
        await store.Insert(new StampedNote { Id = "old" });
        await Task.Delay(20);
        var cutoff = DateTimeOffset.UtcNow;
        await Task.Delay(20);
        await store.Insert(new StampedNote { Id = "new" });

        var deleted = await store.Query<StampedNote>().Where(x => x.Metadata!.CreatedAt < cutoff).ExecuteDelete();

        Assert.Equal(1, deleted);
        var left = await store.Query<StampedNote>().ToList();
        Assert.Equal("new", Assert.Single(left).Id);
    }

    [Fact]
    public async Task JsonCursorPage_DoesNotCarryTheMetadata()
    {
        using var store = CreateStore();
        await store.Insert(new StampedNote { Id = "a", Title = "hello" });

        var page = await store.Query<StampedNote>().ToJsonCursorPage(null, 10);

        var body = Assert.Single(page.Items);
        Assert.DoesNotContain(body, kv => string.Equals(kv.Key, "metadata", StringComparison.OrdinalIgnoreCase));
    }
}
