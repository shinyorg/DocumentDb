using System.Text.Json;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// DocumentMetadata behaviour that isn't provider-specific enough to belong in the conformance suite: shape
// validation, cancelled writes, sessions, temporal snapshots, search reads and the source-generated serializer lane.
public class DocumentMetadataTests
{
    static DocumentStore CreateStore(Action<DocumentStoreOptions>? configure = null, string? connectionString = null)
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connectionString ?? "Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        };
        configure?.Invoke(opts);
        return new DocumentStore(opts);
    }

    sealed class InsertCanceller : IDocumentInterceptor
    {
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            ctx.Cancel();
            return Task.CompletedTask;
        }

        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [Fact]
    public void ConfiguredGetOnlyProperty_FailsValidation()
    {
        var ex = Assert.Throws<DocumentConfigurationException>(() =>
            CreateStore(o => o.ConfigureDocument<GetOnlyStampedNote>(_ => { })));
        Assert.Contains("GetOnlyStampedNote.Metadata", ex.Message);
        Assert.Contains("get-only", ex.Message);
    }

    [Fact]
    public void ConfiguredTwoMetadataProperties_FailsValidation()
    {
        var ex = Assert.Throws<DocumentConfigurationException>(() =>
            CreateStore(o => o.ConfigureDocument<TwiceStampedNote>(_ => { })));
        Assert.Contains("at most one", ex.Message);
    }

    [Fact]
    public async Task UnconfiguredGetOnlyProperty_ThrowsOnFirstUse()
    {
        using var store = CreateStore();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.Insert(new GetOnlyStampedNote { Id = "a" }));
        Assert.Contains("get-only", ex.Message);
    }

    [Fact]
    public async Task CancelledWrite_StampsNothing()
    {
        using var store = CreateStore(o => o.AddInterceptor(new InsertCanceller()));
        var note = new StampedNote { Id = "a" };

        await store.Insert(note);

        Assert.Null(note.Metadata);
        Assert.Null(await store.Get<StampedNote>("a"));
    }

    [Fact]
    public async Task SessionSaveChanges_StampsTheBufferedDocuments()
    {
        using var store = CreateStore();
        var first = new StampedNote { Id = "a", Title = "one" };
        var second = new StampedNote { Id = "b", Title = "two" };
        await using (var session = store.OpenSession())
        {
            session.Add(first).Add(second);
            await session.SaveChanges();
        }

        Assert.True(first.Metadata!.IsPersisted);
        Assert.True(second.Metadata!.IsPersisted);
        var loaded = await store.Get<StampedNote>("b");
        Assert.Equal(second.Metadata.CreatedAt, loaded!.Metadata!.CreatedAt);
    }

    [Fact]
    public async Task ExplicitTransaction_StampsWritesAndLockedReads()
    {
        using var store = CreateStore();
        await using var session = store.OpenSession();
        await using var tx = await session.BeginTransaction();
        var note = new StampedNote { Id = "a", Title = "tx" };
        session.Add(note);
        await session.SaveChanges();

        var inside = await session.Get<StampedNote>("a", LockMode.Update);
        Assert.Equal(note.Metadata!.UpdatedAt, inside!.Metadata!.UpdatedAt);

        var queried = await session.Query<StampedNote>().ToList();
        Assert.True(Assert.Single(queried).Metadata!.IsPersisted);
        await tx.Commit();
    }

    [Fact]
    public async Task BatchInsert_StampsEveryDocument_OnTheFastPath()
    {
        using var store = CreateStore();
        var notes = Enumerable.Range(0, 5).Select(i => new StampedNote { Id = $"n{i}" }).ToList();

        await store.BatchInsert(notes);

        Assert.All(notes, n => Assert.True(n.Metadata!.IsPersisted));
        var loaded = await store.Query<StampedNote>().ToList();
        Assert.All(loaded, n => Assert.Equal(notes.Single(x => x.Id == n.Id).Metadata!.CreatedAt, n.Metadata!.CreatedAt));
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
    }

    [Fact]
    public async Task FullTextSearch_StampsResults()
    {
        using var store = CreateStore(o => o.ConfigureDocument<StampedNote>(cfg => cfg.MapFullTextProperty(x => x.Title)));
        var note = new StampedNote { Id = "a", Title = "quick brown fox" };
        await store.Insert(note);

        var results = await store.FullTextSearch<StampedNote>("fox");

        var hit = Assert.Single(results);
        Assert.Equal(note.Metadata!.CreatedAt, hit.Document.Metadata!.CreatedAt);
    }

    [Fact]
    public async Task SourceGeneratedTypeInfo_StampsAndStripsTheBody()
    {
        var connection = $"Data Source=md_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        using var holdOpen = new SqliteConnection(connection);
        holdOpen.Open();
        var table = $"t{Guid.NewGuid():N}";

        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connection),
            TableName = table,
            JsonSerializerOptions = new JsonSerializerOptions(TestJsonContext.Default.Options),
            UseReflectionFallback = false
        });

        var note = new StampedNote { Id = "a", Title = "aot", Metadata = null };
        await store.Insert(note, TestJsonContext.Default.StampedNote);
        var loaded = await store.Get("a", TestJsonContext.Default.StampedNote);
        Assert.Equal(note.Metadata!.CreatedAt, loaded!.Metadata!.CreatedAt);

        await using var cmd = holdOpen.CreateCommand();
        cmd.CommandText = $"SELECT Data FROM \"{table}\" WHERE Id = 'a'";
        var body = (string)(await cmd.ExecuteScalarAsync())!;
        Assert.DoesNotContain("etadata", body);
    }

    public sealed class NoteStamp
    {
        public string Id { get; set; } = "";
        public DateTimeOffset Updated { get; set; }
    }

    public sealed record NotePair(string Title, DateTimeOffset LeftCreated, DateTimeOffset RightUpdated);

    [Fact]
    public async Task TypedSelect_OverMetadata_ProjectsTheStampedValues()
    {
        using var store = CreateStore();
        var note = new StampedNote { Id = "a", Rank = 1 };
        await store.Insert(note);

        var projected = await store.Query<StampedNote>().Where(x => x.Rank == 1).Select(x => new NoteStamp { Id = x.Id, Updated = x.Metadata!.UpdatedAt }).ToList();

        var row = Assert.Single(projected);
        Assert.Equal(note.Metadata!.UpdatedAt, row.Updated);
    }

    [Fact]
    public async Task StringProject_OverMetadata_ProjectsTheStampedValues()
    {
        using var store = CreateStore();
        var note = new StampedNote { Id = "a", Rank = 1 };
        await store.Insert(note);

        var projected = await store.Query<StampedNote>().Project("Id, Metadata.UpdatedAt").ToList();

        var row = Assert.Single(projected);
        Assert.Equal("a", (string?)row["id"]);
        Assert.Equal(note.Metadata!.UpdatedAt, row["updatedAt"]!.GetValue<DateTimeOffset>());
    }

    [Fact]
    public async Task Join_StampsBothSides_AndFiltersOnEitherEnvelope()
    {
        using var store = CreateStore();
        var left = new StampedNote { Id = "a", Title = "left" };
        await store.Insert(left);
        await Task.Delay(20);
        var right = new InitStampedNote { Id = "a", Title = "right" };
        await store.Insert(right);

        var pairs = await store.Query<StampedNote>()
            .Join<InitStampedNote>((l, r) => l.Id == r.Id)
            .Where((l, r) => l.Metadata!.CreatedAt < r.Metadata.CreatedAt)
            .Select((l, r) => new NotePair(l.Title, l.Metadata!.CreatedAt, r.Metadata.UpdatedAt))
            .ToList();

        var pair = Assert.Single(pairs);
        Assert.Equal(left.Metadata!.CreatedAt, pair.LeftCreated);
        Assert.Equal(right.Metadata.UpdatedAt, pair.RightUpdated);
    }

    [Fact]
    public async Task StringJoin_FiltersOrdersAndProjectsEitherEnvelope()
    {
        using var store = CreateStore();
        var left = new StampedNote { Id = "a", Title = "left" };
        await store.Insert(left);
        await Task.Delay(20);
        var right = new InitStampedNote { Id = "a", Title = "right" };
        await store.Insert(right);
        await store.Insert(new StampedNote { Id = "b", Title = "unpaired" });
        var cutoff = left.Metadata!.CreatedAt;

        var rows = await store.Query<StampedNote>()
            .Join<InitStampedNote>("l", "r", "l.id = r.id")
            .Where($"l.Metadata.CreatedAt < r.Metadata.CreatedAt and r.Metadata.UpdatedAt > {cutoff}")
            .OrderByDescending("l.Metadata.CreatedAt")
            .Project("l.id as id, l.Metadata.CreatedAt as leftCreated, r.Metadata.UpdatedAt as rightUpdated")
            .ToList();

        var row = Assert.Single(rows);
        Assert.Equal("a", row["id"]!.GetValue<string>());
        Assert.Equal(left.Metadata.CreatedAt, row["leftCreated"]!.GetValue<DateTimeOffset>());
        Assert.Equal(right.Metadata.UpdatedAt, row["rightUpdated"]!.GetValue<DateTimeOffset>());

        var none = await store.Query<StampedNote>()
            .Join<InitStampedNote>("l", "r", "l.id = r.id")
            .Where("l.Metadata.CreatedAt > r.Metadata.CreatedAt")
            .Project("l.id")
            .ToList();
        Assert.Empty(none);
    }

    [Fact]
    public async Task AnyPropertyName_IsStampedAndQueryable_OnEverySurface()
    {
        using var store = CreateStore();
        var first = new AuditedNote { Id = "a", Rank = 1 };
        await store.Insert(first);
        await Task.Delay(20);
        await store.Insert(new AuditedNote { Id = "b", Rank = 2 });
        var cutoff = first.Audit!.CreatedAt;

        var loaded = await store.Get<AuditedNote>("a");
        Assert.Equal(cutoff, loaded!.Audit!.CreatedAt);

        var linq = await store.Query<AuditedNote>().Where(x => x.Audit!.CreatedAt > cutoff).ToList();
        Assert.Equal(["b"], linq.Select(n => n.Id).ToArray());

        var viaString = await store.Query<AuditedNote>().Where($"Audit.CreatedAt > {cutoff}").ToList();
        Assert.Equal(["b"], viaString.Select(n => n.Id).ToArray());

        var ordered = await store.Query<AuditedNote>().OrderBy("audit.createdAt", "desc").ToList();
        Assert.Equal(["b", "a"], ordered.Select(n => n.Id).ToArray());

        var projected = await store.Query<AuditedNote>().Where(x => x.Rank == 1).Project("Id, Audit.UpdatedAt").ToList();
        Assert.Equal(first.Audit.UpdatedAt, Assert.Single(projected)["updatedAt"]!.GetValue<DateTimeOffset>());
    }

    [Fact]
    public void OutsideTheStore_MetadataSerializesAsAnObject()
    {
        var note = new StampedNote { Id = "a", Metadata = new DocumentMetadata() };
        var json = JsonSerializer.Serialize(note, TestJsonContext.Default.StampedNote);
        Assert.Contains("\"Metadata\":{\"CreatedAt\":", json);

        var back = JsonSerializer.Deserialize(json, TestJsonContext.Default.StampedNote)!;
        Assert.NotNull(back.Metadata);
        Assert.False(back.Metadata!.IsPersisted);
    }
    [Fact]
    public async Task TenantId_IsNull_WithoutMultiTenancy()
    {
        using var store = CreateStore();
        var note = new StampedNote { Id = "a" };
        await store.Insert(note);

        Assert.Null(note.Metadata!.TenantId);
        Assert.Null((await store.Get<StampedNote>("a"))!.Metadata!.TenantId);
    }

    [Fact]
    public void Serialized_TenantId_RoundTrips_AndIsOmittedWhenNull()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        var json = JsonSerializer.Serialize(new DocumentMetadata { TenantId = "acme" }, options);
        Assert.Contains("\"tenantId\":\"acme\"", json);
        Assert.Equal("acme", JsonSerializer.Deserialize<DocumentMetadata>(json, options)!.TenantId);

        Assert.DoesNotContain("tenantId", JsonSerializer.Serialize(new DocumentMetadata(), options));
    }
}
