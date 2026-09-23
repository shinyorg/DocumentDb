using Shiny.DocumentDb.MongoDb;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.MongoDb;

/// <summary>
/// The MongoDB-only paths of the <see cref="DocumentMetadata"/> contract that the shared conformance suite does not
/// reach: the bulk-write fast paths, temporal snapshots and Restore, the <c>$lookup</c> join, and <c>$text</c> search.
/// </summary>
[Collection("MongoDB")]
public class MongoDbDocumentMetadataTests(MongoDbDatabaseFixture db)
{
    MongoDbDocumentStore NewStore(Action<MongoDbDocumentStoreOptions>? configure = null)
        => db.CreateConfiguredStore($"m{Guid.NewGuid():N}", configure ?? (_ => { }));

    [Fact]
    public async Task BatchInsert_StampsEveryDocument_EqualToTheEnvelope()
    {
        using var store = this.NewStore();
        var notes = new[] { new StampedNote { Id = "a", Title = "one" }, new StampedNote { Id = "b", Title = "two" } };

        await store.BatchInsert(notes);

        foreach (var note in notes)
        {
            Assert.True(note.Metadata!.IsPersisted);
            Assert.Equal(note.Metadata.CreatedAt, note.Metadata.UpdatedAt);
            var loaded = (await store.Get<StampedNote>(note.Id))!;
            Assert.Equal(note.Metadata.CreatedAt, loaded.Metadata!.CreatedAt);
            Assert.Equal(note.Metadata.UpdatedAt, loaded.Metadata.UpdatedAt);
        }
    }

    [Fact]
    public async Task BatchUpsert_StampsMergesAndInserts_KeepingTheLiveCreatedAt()
    {
        using var store = this.NewStore();
        var original = new StampedNote { Id = "a", Title = "v1" };
        await store.Insert(original);
        await Task.Delay(20);

        var merge = new StampedNote { Id = "a", Title = "v2" };
        var fresh = new StampedNote { Id = "b", Title = "new" };
        await store.BatchUpsert([merge, fresh]);

        Assert.Equal(original.Metadata!.CreatedAt, merge.Metadata!.CreatedAt);
        Assert.True(merge.Metadata.UpdatedAt > merge.Metadata.CreatedAt);
        Assert.Equal(fresh.Metadata!.CreatedAt, fresh.Metadata.UpdatedAt);

        var reread = (await store.Get<StampedNote>("a"))!;
        Assert.Equal("v2", reread.Title);
        Assert.Equal(merge.Metadata.CreatedAt, reread.Metadata!.CreatedAt);
        Assert.Equal(merge.Metadata.UpdatedAt, reread.Metadata.UpdatedAt);
        Assert.Equal(fresh.Metadata.UpdatedAt, (await store.Get<StampedNote>("b"))!.Metadata!.UpdatedAt);
    }

    [Fact]
    public async Task BatchUpdate_StampsUpdatedAt_EqualToTheEnvelope()
    {
        using var store = this.NewStore();
        await store.Insert(new StampedNote { Id = "a", Title = "v1" });
        var loaded = (await store.Get<StampedNote>("a"))!;
        var created = loaded.Metadata!.CreatedAt;
        await Task.Delay(20);

        loaded.Title = "v2";
        await store.BatchUpdate([loaded]);

        Assert.Equal(created, loaded.Metadata.CreatedAt);
        Assert.True(loaded.Metadata.UpdatedAt > created);
        Assert.Equal(loaded.Metadata.UpdatedAt, (await store.Get<StampedNote>("a"))!.Metadata!.UpdatedAt);
    }

    [Fact]
    public async Task TemporalSnapshots_AreNewedUpButUnstamped_AndRestoreKeepsTheLiveCreatedAt()
    {
        using var store = this.NewStore(o => o.ConfigureDocument<StampedNote>(cfg => cfg.MapTemporal()));
        var note = new StampedNote { Id = "a", Title = "v1" };
        await store.Insert(note);
        var created = note.Metadata!.CreatedAt;
        await Task.Delay(20);
        note.Title = "v2";
        await store.Update(note);

        var history = await store.History<StampedNote>("a");
        Assert.Equal(2, history.Count);
        Assert.All(history, v =>
        {
            Assert.NotNull(v.Document!.Metadata);
            Assert.False(v.Document.Metadata!.IsPersisted);
        });
        var asOf = (await store.AsOf<StampedNote>("a", DateTimeOffset.UtcNow))!;
        Assert.False(asOf.Metadata!.IsPersisted);

        await Task.Delay(20);
        var restored = (await store.Restore<StampedNote>("a", history.Single(v => v.Document!.Title == "v1").Version))!;

        Assert.True(restored.Metadata!.IsPersisted);
        Assert.Equal(created, restored.Metadata.CreatedAt);
        var live = (await store.Get<StampedNote>("a"))!;
        Assert.Equal("v1", live.Title);
        Assert.Equal(created, live.Metadata!.CreatedAt);
        Assert.Equal(restored.Metadata.UpdatedAt, live.Metadata.UpdatedAt);
    }

    [Fact]
    public async Task Join_FiltersAndOrdersOnTheEnvelope_AndStampsBothSides()
    {
        using var store = this.NewStore();
        foreach (var id in new[] { "n1", "n2", "n3" })
        {
            await store.Insert(new StampedNote { Id = id, Title = id });
            await Task.Delay(20);
        }
        var cutoff = (await store.Get<StampedNote>("n1"))!.Metadata!.CreatedAt;

        var rows = await store.Query<StampedNote>()
            .Join<StampedNote>((l, r) => l.Id == r.Id)
            .Where((l, r) => r.Metadata!.CreatedAt > cutoff)
            .OrderByDescending((l, r) => l.Metadata!.CreatedAt)
            .Select((l, r) => new KeyValuePair<StampedNote, StampedNote>(l, r))
            .ToList();

        Assert.Equal(["n3", "n2"], rows.Select(p => p.Key.Id).ToArray());
        Assert.All(rows, p =>
        {
            Assert.True(p.Key.Metadata!.IsPersisted);
            Assert.True(p.Value.Metadata!.IsPersisted);
            Assert.Equal(p.Key.Metadata.CreatedAt, p.Value.Metadata.CreatedAt);
        });
    }

    [Fact]
    public async Task StringJoin_FiltersOrdersAndProjectsTheEnvelope()
    {
        using var store = this.NewStore();
        foreach (var id in new[] { "n1", "n2", "n3" })
        {
            await store.Insert(new StampedNote { Id = id, Title = id });
            await Task.Delay(20);
        }
        var first = (await store.Get<StampedNote>("n1"))!.Metadata!;
        var third = (await store.Get<StampedNote>("n3"))!.Metadata!;

        var rows = await store.Query<StampedNote>()
            .Join<StampedNote>("l", "r", "l.id = r.id")
            .Where($"r.Metadata.CreatedAt > {first.CreatedAt}")
            .OrderByDescending("l.Metadata.CreatedAt")
            .Project("l.id as id, l.Metadata.CreatedAt as leftCreated, r.Metadata.UpdatedAt as rightUpdated")
            .ToList();

        Assert.Equal(["n3", "n2"], rows.Select(r => r["id"]!.GetValue<string>()).ToArray());
        Assert.Equal(third.CreatedAt, rows[0]["leftCreated"]!.GetValue<DateTimeOffset>());
        Assert.Equal(third.UpdatedAt, rows[0]["rightUpdated"]!.GetValue<DateTimeOffset>());
    }

    [Fact]
    public async Task FullTextSearch_StampsFromTheEnvelope()
    {
        using var store = this.NewStore(o => o.ConfigureDocument<StampedNote>(cfg => cfg.MapFullTextProperty(x => x.Title)));
        var note = new StampedNote { Id = "a", Title = "hello world" };
        await store.Insert(note);

        var hit = Assert.Single(await store.FullTextSearch<StampedNote>("hello"));

        Assert.True(hit.Document.Metadata!.IsPersisted);
        Assert.Equal(note.Metadata!.CreatedAt, hit.Document.Metadata.CreatedAt);
        Assert.Equal(note.Metadata.UpdatedAt, hit.Document.Metadata.UpdatedAt);
    }
}
