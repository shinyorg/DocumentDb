using Shiny.DocumentDb.CosmosDb;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// <see cref="DocumentMetadata"/> on the Cosmos-only paths the cross-provider conformance suite doesn't reach: batch
/// inserts, the observable upsert insert branch, raw-SQL queries, unique-index writes, temporal snapshots and the
/// envelope-field SQL itself.
/// </summary>
[Collection("CosmosDB")]
public class CosmosDbMetadataTests(CosmosDbDatabaseFixture db)
{
    CosmosDbDocumentStore NewStore(Action<CosmosDbDocumentStoreOptions>? configure = null)
        => db.CreateConfiguredStore($"m{Guid.NewGuid():N}", o => configure?.Invoke(o));

    static void AssertSameStamp(DocumentMetadata? expected, DocumentMetadata? actual)
    {
        Assert.NotNull(expected);
        Assert.NotNull(actual);
        Assert.True(actual!.IsPersisted);
        Assert.Equal(expected!.CreatedAt, actual.CreatedAt);
        Assert.Equal(expected.UpdatedAt, actual.UpdatedAt);
    }

    [Fact]
    public void EnvelopeFields_TranslateToTheItemRoot_NotTheBody()
    {
        using var store = this.NewStore();
        var cutoff = new DateTimeOffset(2026, 1, 2, 8, 0, 0, TimeSpan.FromHours(5));

        var query = ((IDocumentStore)store).Query<StampedNote>()
            .Where(x => x.Metadata!.UpdatedAt > cutoff)
            .OrderBy(x => x.Metadata!.CreatedAt)
            .ToQueryString();

        Assert.Contains("(c.updatedAt > @p0)", query.Sql);
        Assert.Contains("ORDER BY c.createdAt ASC", query.Sql);
        Assert.DoesNotContain("c.data.metadata", query.Sql);
        // Bound as the envelope stores it: UTC, round-trip format, so string order is instant order.
        Assert.Equal("2026-01-02T03:00:00.0000000+00:00", query.Parameters["@p0"]);
    }

    [Fact]
    public async Task BatchInsert_StampsEveryInstance_EqualToTheReread()
    {
        using var store = this.NewStore();
        var notes = new[] { new StampedNote { Id = "a", Rank = 1 }, new StampedNote { Id = "b", Rank = 2 } };

        await ((IDocumentStore)store).BatchInsert(notes);

        foreach (var note in notes)
        {
            Assert.Equal(note.Metadata!.CreatedAt, note.Metadata.UpdatedAt);
            AssertSameStamp(note.Metadata, (await ((IDocumentStore)store).Get<StampedNote>(note.Id))!.Metadata);
        }
    }

    [Fact]
    public async Task Upsert_ThatCreates_StampsCreatedAt()
    {
        using var store = this.NewStore();
        var note = new StampedNote { Id = "a", Title = "new" };

        await ((IDocumentStore)store).Upsert(note);

        Assert.Equal(note.Metadata!.CreatedAt, note.Metadata.UpdatedAt);
        AssertSameStamp(note.Metadata, (await ((IDocumentStore)store).Get<StampedNote>("a"))!.Metadata);
    }

    [Fact]
    public async Task RawSqlQuery_AndStream_Stamp()
    {
        IDocumentStore store = this.NewStore();
        using var _ = (IDisposable)store;
        var note = new StampedNote { Id = "a", Rank = 7 };
        await store.Insert(note);

        var listed = Assert.Single(await store.Query<StampedNote>("c.data.rank = 7"));
        AssertSameStamp(note.Metadata, listed.Metadata);

        var streamed = new List<StampedNote>();
        await foreach (var n in store.QueryStream<StampedNote>("c.data.rank = 7"))
            streamed.Add(n);
        AssertSameStamp(note.Metadata, Assert.Single(streamed).Metadata);
    }

    [Fact]
    public async Task UniqueIndexWrites_Stamp()
    {
        IDocumentStore store = this.NewStore(o => o.ConfigureDocument<StampedNote>(cfg => cfg.MapUniqueIndex(x => x.Title)));
        using var _ = (IDisposable)store;

        var note = new StampedNote { Id = "a", Title = "one" };
        await store.Insert(note);
        AssertSameStamp(note.Metadata, (await store.Get<StampedNote>("a"))!.Metadata);
        var created = note.Metadata!.CreatedAt;

        await Task.Delay(20);
        note.Title = "two";
        await store.Update(note);
        Assert.Equal(created, note.Metadata.CreatedAt);
        AssertSameStamp(note.Metadata, (await store.Get<StampedNote>("a"))!.Metadata);

        await Task.Delay(20);
        var patch = new StampedNote { Id = "a", Title = "three" };
        await store.Upsert(patch);
        var reread = (await store.Get<StampedNote>("a"))!;
        Assert.Equal(created, reread.Metadata!.CreatedAt);
        Assert.Equal(patch.Metadata!.UpdatedAt, reread.Metadata.UpdatedAt);

        var batch = new[] { new StampedNote { Id = "b", Title = "four" } };
        await store.BatchInsert(batch);
        AssertSameStamp(batch[0].Metadata, (await store.Get<StampedNote>("b"))!.Metadata);
    }

    [Fact]
    public async Task TemporalSnapshots_AreNewedUpButUnstamped_AndRestoreKeepsCreatedAt()
    {
        var store = this.NewStore(o => o.ConfigureDocument<StampedNote>(cfg => cfg.MapTemporal()));
        using var _ = store;

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

        var asOf = await store.AsOf<StampedNote>("a", DateTimeOffset.UtcNow);
        Assert.NotNull(asOf!.Metadata);
        Assert.False(asOf.Metadata!.IsPersisted);

        await Task.Delay(20);
        var restored = (await store.Restore<StampedNote>("a", history.Min(v => v.Version)))!;
        Assert.Equal("v1", restored.Title);
        Assert.Equal(created, restored.Metadata!.CreatedAt);
        Assert.True(restored.Metadata.UpdatedAt > note.Metadata.UpdatedAt);
        AssertSameStamp(restored.Metadata, (await ((IDocumentStore)store).Get<StampedNote>("a"))!.Metadata);
    }
}
