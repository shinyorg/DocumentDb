using System.Text.Json.Nodes;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The <see cref="DocumentMetadata"/> contract, asserted identically on every provider: the store news the property
/// up, stamps it from the envelope on every read and after every write, keeps it out of the body, and answers
/// Where/OrderBy on it from the envelope.
/// </summary>
public abstract class DocumentMetadataConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    protected IDocumentStore NewStore() => this.Fixture.CreateStore($"m{Guid.NewGuid():N}");

    // Three notes written far enough apart that every provider's timestamp precision tells them apart.
    async Task<(IDocumentStore Store, StampedNote[] Notes)> SeededAsync()
    {
        var store = this.NewStore();
        var notes = new[]
        {
            new StampedNote { Id = "n1", Title = "first", Rank = 1 },
            new StampedNote { Id = "n2", Title = "second", Rank = 2 },
            new StampedNote { Id = "n3", Title = "third", Rank = 3 }
        };
        foreach (var note in notes)
        {
            await store.Insert(note);
            await Task.Delay(40);
        }
        return (store, notes);
    }

    [Fact]
    public async Task Insert_NewsUpAndStampsTheWrittenInstance()
    {
        using var store = (IDisposable)this.NewStore();
        var note = new StampedNote { Id = "a", Title = "hello" };
        Assert.Null(note.Metadata);

        var before = DateTimeOffset.UtcNow.AddSeconds(-5);
        await ((IDocumentStore)store).Insert(note);

        Assert.NotNull(note.Metadata);
        Assert.True(note.Metadata!.IsPersisted);
        Assert.Equal(note.Metadata.CreatedAt, note.Metadata.UpdatedAt);
        Assert.InRange(note.Metadata.CreatedAt, before, DateTimeOffset.UtcNow.AddSeconds(5));
    }

    // Nothing has serialized through the store's options yet — resolving the metadata property must not depend on a
    // write having locked a resolver in first.
    [Fact]
    public async Task ReadsAsTheFirstOperation_Work()
    {
        var store = this.NewStore();
        using var _ = (IDisposable)store;

        Assert.Null(await store.Get<StampedNote>("missing"));
        Assert.Empty(await this.NewStoreQuery().Where(x => x.Metadata!.UpdatedAt > DateTimeOffset.MinValue).ToList());
    }

    IDocumentQuery<StampedNote> NewStoreQuery() => this.NewStore().Query<StampedNote>();

    [Fact]
    public async Task Get_StampsFromTheEnvelope_EqualToTheWrite()
    {
        var store = this.NewStore();
        using var _ = (IDisposable)store;
        var note = new StampedNote { Id = "a", Title = "hello" };
        await store.Insert(note);

        var loaded = await store.Get<StampedNote>("a");

        Assert.NotNull(loaded!.Metadata);
        Assert.True(loaded.Metadata!.IsPersisted);
        Assert.Equal(note.Metadata!.CreatedAt, loaded.Metadata.CreatedAt);
        Assert.Equal(note.Metadata.UpdatedAt, loaded.Metadata.UpdatedAt);
    }

    [Fact]
    public async Task Update_AdvancesUpdatedAt_AndKeepsCreatedAt()
    {
        var store = this.NewStore();
        using var _ = (IDisposable)store;
        await store.Insert(new StampedNote { Id = "a", Title = "v1" });
        var loaded = (await store.Get<StampedNote>("a"))!;
        var created = loaded.Metadata!.CreatedAt;

        await Task.Delay(40);
        loaded.Title = "v2";
        await store.Update(loaded);

        Assert.Equal(created, loaded.Metadata.CreatedAt);
        Assert.True(loaded.Metadata.UpdatedAt > created);

        var reread = (await store.Get<StampedNote>("a"))!;
        Assert.Equal(created, reread.Metadata!.CreatedAt);
        Assert.Equal(loaded.Metadata.UpdatedAt, reread.Metadata.UpdatedAt);
    }

    [Fact]
    public async Task Upsert_OverAnExistingRow_AdvancesUpdatedAt_AndKeepsCreatedAt()
    {
        var store = this.NewStore();
        using var _ = (IDisposable)store;
        var original = new StampedNote { Id = "a", Title = "v1" };
        await store.Insert(original);

        await Task.Delay(40);
        var patch = new StampedNote { Id = "a", Title = "v2" };
        await store.Upsert(patch);

        Assert.NotNull(patch.Metadata);
        Assert.True(patch.Metadata!.IsPersisted);

        var reread = (await store.Get<StampedNote>("a"))!;
        Assert.Equal("v2", reread.Title);
        Assert.Equal(original.Metadata!.CreatedAt, reread.Metadata!.CreatedAt);
        Assert.Equal(patch.Metadata.UpdatedAt, reread.Metadata.UpdatedAt);
        Assert.True(reread.Metadata.UpdatedAt > reread.Metadata.CreatedAt);
    }

    [Fact]
    public async Task EveryQueryTerminal_Stamps()
    {
        var (store, notes) = await this.SeededAsync();
        using var _ = (IDisposable)store;
        var byId = notes.ToDictionary(n => n.Id, n => n.Metadata!);

        void AssertStamped(StampedNote n)
        {
            Assert.NotNull(n.Metadata);
            Assert.True(n.Metadata!.IsPersisted);
            Assert.Equal(byId[n.Id].CreatedAt, n.Metadata.CreatedAt);
            Assert.Equal(byId[n.Id].UpdatedAt, n.Metadata.UpdatedAt);
        }

        var list = await store.Query<StampedNote>().ToList();
        Assert.Equal(3, list.Count);
        Assert.All(list, AssertStamped);

        var streamed = new List<StampedNote>();
        await foreach (var n in store.Query<StampedNote>().ToAsyncEnumerable())
            streamed.Add(n);
        Assert.Equal(3, streamed.Count);
        Assert.All(streamed, AssertStamped);

        AssertStamped((await store.Query<StampedNote>().Where(x => x.Rank == 2).FirstOrDefault())!);
        AssertStamped(await store.Query<StampedNote>().Where(x => x.Rank == 3).Single());

        var page = await store.Query<StampedNote>().OrderBy(x => x.Rank).ToCursorPage(null, 2);
        Assert.Equal(2, page.Items.Count);
        Assert.All(page.Items, AssertStamped);
    }

    [Fact]
    public async Task Where_OnUpdatedAt_FiltersByTheEnvelope()
    {
        var (store, notes) = await this.SeededAsync();
        using var _ = (IDisposable)store;
        var cutoff = notes[1].Metadata!.UpdatedAt;

        var after = await store.Query<StampedNote>().Where(x => x.Metadata!.UpdatedAt > cutoff).ToList();
        Assert.Equal(["n3"], after.Select(n => n.Id).ToArray());

        var atOrBefore = await store.Query<StampedNote>().Where(x => x.Metadata!.UpdatedAt <= cutoff).OrderBy(x => x.Rank).ToList();
        Assert.Equal(["n1", "n2"], atOrBefore.Select(n => n.Id).ToArray());

        // An insert stamps CreatedAt == UpdatedAt, so n2 (at the cutoff) and n3 are both on or after it.
        Assert.Equal(2, await store.Query<StampedNote>().Where(x => x.Metadata!.CreatedAt >= cutoff).Count());
    }

    [Fact]
    public async Task Where_AcceptsANonUtcOffset()
    {
        var (store, notes) = await this.SeededAsync();
        using var _ = (IDisposable)store;
        // Same instant, expressed at +05:00 — the comparison is on the instant, not the wall-clock text.
        var cutoff = notes[1].Metadata!.UpdatedAt.ToOffset(TimeSpan.FromHours(5));

        var after = await store.Query<StampedNote>().Where(x => x.Metadata!.UpdatedAt > cutoff).ToList();

        Assert.Equal(["n3"], after.Select(n => n.Id).ToArray());
    }

    [Fact]
    public async Task Where_ContainsOverTimestamps_MatchesTheEnvelope()
    {
        var (store, notes) = await this.SeededAsync();
        using var _ = (IDisposable)store;
        DateTimeOffset[] wanted = [notes[0].Metadata!.UpdatedAt, notes[2].Metadata!.UpdatedAt.ToOffset(TimeSpan.FromHours(-4))];

        var viaArray = await store.Query<StampedNote>().Where(x => wanted.Contains(x.Metadata!.UpdatedAt)).ToList();
        Assert.Equal(["n1", "n3"], viaArray.Select(n => n.Id).Order().ToArray());

        var viaWhereIn = await store.Query<StampedNote>().WhereIn(x => x.Metadata!.UpdatedAt, wanted).ToList();
        Assert.Equal(["n1", "n3"], viaWhereIn.Select(n => n.Id).Order().ToArray());
    }

    [Fact]
    public async Task Where_DatePartOfATimestamp_MatchesTheEnvelope()
    {
        var (store, notes) = await this.SeededAsync();
        using var _ = (IDisposable)store;
        var year = notes[0].Metadata!.CreatedAt.UtcDateTime.Year;

        var thisYear = await store.Query<StampedNote>().Where(x => x.Metadata!.CreatedAt.Year == year).ToList();
        Assert.Equal(3, thisYear.Count);

        var otherYear = await store.Query<StampedNote>().Where(x => x.Metadata!.CreatedAt.Year == year - 1).ToList();
        Assert.Empty(otherYear);
    }

    [Fact]
    public async Task Where_StringGrammar_MatchesLinq()
    {
        var (store, notes) = await this.SeededAsync();
        using var _ = (IDisposable)store;
        var cutoff = notes[0].Metadata!.CreatedAt;

        var viaString = await store.Query<StampedNote>().Where($"Metadata.CreatedAt > {cutoff}").OrderBy(x => x.Rank).ToList();

        Assert.Equal(["n2", "n3"], viaString.Select(n => n.Id).ToArray());
    }

    [Fact]
    public async Task OrderBy_CreatedAt_LinqAndString()
    {
        var (store, _) = await this.SeededAsync();
        using var __ = (IDisposable)store;

        var descending = await store.Query<StampedNote>().OrderByDescending(x => x.Metadata!.CreatedAt).ToList();
        Assert.Equal(["n3", "n2", "n1"], descending.Select(n => n.Id).ToArray());

        var viaString = await store.Query<StampedNote>().OrderBy("Metadata.CreatedAt", "desc").ToList();
        Assert.Equal(["n3", "n2", "n1"], viaString.Select(n => n.Id).ToArray());
    }

    [Fact]
    public async Task PreinitialisedInitOnlyInstance_IsStampedInPlace()
    {
        var store = this.NewStore();
        using var _ = (IDisposable)store;
        var note = new InitStampedNote { Id = "a", Title = "hello" };
        var instance = note.Metadata;
        Assert.False(instance.IsPersisted);

        await store.Insert(note);

        Assert.Same(instance, note.Metadata);
        Assert.True(note.Metadata.IsPersisted);

        var loaded = (await store.Get<InitStampedNote>("a"))!;
        Assert.True(loaded.Metadata.IsPersisted);
        Assert.Equal(note.Metadata.CreatedAt, loaded.Metadata.CreatedAt);
    }

    // The instance a write stamped carries exactly the envelope values a fresh read returns.
    static async Task AssertStampedAsStored(IDocumentStore store, StampedNote written)
    {
        var stored = (await store.Get<StampedNote>(written.Id))!;
        Assert.True(written.Metadata!.IsPersisted);
        Assert.Equal(stored.Metadata!.CreatedAt, written.Metadata.CreatedAt);
        Assert.Equal(stored.Metadata.UpdatedAt, written.Metadata.UpdatedAt);
    }

    [Fact]
    public async Task UniqueIndexedWrites_StampTheStoredTimestamps()
    {
        IDocumentStore store;
        try
        {
            store = this.Fixture.CreateStore($"m{Guid.NewGuid():N}", o => o.ConfigureDocument<StampedNote>(cfg => cfg.MapUniqueIndex(x => x.Title)));
        }
        catch (DocumentConfigurationException)
        {
            // The provider has no unique indexes (DuckDB) — nothing to stamp through.
            return;
        }
        using var _ = (IDisposable)store;

        var note = new StampedNote { Id = "a", Title = "one" };
        await store.Insert(note);
        await AssertStampedAsStored(store, note);

        await Task.Delay(40);
        note.Title = "two";
        await store.Update(note);
        await AssertStampedAsStored(store, note);

        var patch = new StampedNote { Id = "a", Title = "three" };
        await store.Upsert(patch);
        Assert.Equal((await store.Get<StampedNote>("a"))!.Metadata!.UpdatedAt, patch.Metadata!.UpdatedAt);

        var batch = new[] { new StampedNote { Id = "b", Title = "b" }, new StampedNote { Id = "c", Title = "c" } };
        await store.BatchInsert(batch);
        foreach (var inserted in batch)
            await AssertStampedAsStored(store, inserted);
    }

    [Fact]
    public async Task VersionedWrites_StampTheStoredTimestamps()
    {
        var store = this.Fixture.CreateStore($"m{Guid.NewGuid():N}", o => o.ConfigureDocument<StampedNote>(cfg => cfg.MapVersionProperty(x => x.Rank)));
        using var _ = (IDisposable)store;

        var note = new StampedNote { Id = "a", Title = "one" };
        await store.Insert(note);
        await Task.Delay(40);
        note.Title = "two";
        await store.Update(note);

        await AssertStampedAsStored(store, note);
        Assert.True(note.Metadata!.UpdatedAt > note.Metadata.CreatedAt);
    }

    public sealed class NoteStamp
    {
        public string Id { get; set; } = "";
        public DateTimeOffset Updated { get; set; }
    }

    [Fact]
    public async Task Projections_OverMetadata_ReturnTheStampedValues()
    {
        var (store, notes) = await this.SeededAsync();
        using var _ = (IDisposable)store;
        var expected = notes[1].Metadata!.UpdatedAt;

        var typed = await store.Query<StampedNote>().Where(x => x.Rank == 2).Select(x => new NoteStamp { Id = x.Id, Updated = x.Metadata!.UpdatedAt }).ToList();
        Assert.Equal(expected, Assert.Single(typed).Updated);

        var viaString = await store.Query<StampedNote>().Where(x => x.Rank == 2).Project("Id, Metadata.UpdatedAt").ToList();
        var row = Assert.Single(viaString);
        var updated = row.Single(kv => string.Equals(kv.Key, "updatedAt", StringComparison.OrdinalIgnoreCase)).Value;
        Assert.Equal(expected, updated!.GetValue<DateTimeOffset>());
    }

    [Fact]
    public async Task Body_DoesNotCarryTheMetadata()
    {
        var store = this.NewStore();
        using var _ = (IDisposable)store;
        await store.Insert(new StampedNote { Id = "a", Title = "hello" });

        var query = store.Query<StampedNote>();
        if (!query.SupportsRawJson)
            return;

        var raw = await query.ToJsonList();
        var body = Assert.Single(raw);
        Assert.DoesNotContain(body, kv => string.Equals(kv.Key, "metadata", StringComparison.OrdinalIgnoreCase));
    }
}
