using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// Late-bound JSON lane (Insert/Update/Upsert and Get/Query/QueryStream by <see cref="Type"/> + <c>JsonNode</c>)
/// on the SQLite relational store. Exercises the write pipeline parity, the mapped-property presence check,
/// and the raw-JSON read path.
/// </summary>
public class JsonLaneTests : IDisposable
{
    sealed class Place
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public GeoPoint Location { get; set; }
    }

    readonly List<IDisposable> disposables = new();

    DocumentStore CreateStore(Action<DocumentStoreOptions>? configure = null)
    {
        var connectionString = $"Data Source=jsonlane_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        var holdOpen = new SqliteConnection(connectionString);
        holdOpen.Open();
        this.disposables.Add(holdOpen);

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connectionString),
            TableName = $"t{Guid.NewGuid():N}"
        };
        configure?.Invoke(opts);
        var store = new DocumentStore(opts);
        this.disposables.Add(store);
        return store;
    }

    public void Dispose()
    {
        for (var i = this.disposables.Count - 1; i >= 0; i--)
            this.disposables[i].Dispose();
    }

    // ── Writes ──────────────────────────────────────────────────────────

    [Fact]
    public async Task Insert_Object_RoundTripsViaTypedGet()
    {
        var store = this.CreateStore();
        var node = JsonNode.Parse("""{ "id": "u1", "name": "Alice" }""")!;

        var n = await store.Insert(typeof(User), node);

        Assert.Equal(1, n);
        var got = await store.Get<User>("u1");
        Assert.NotNull(got);
        Assert.Equal("Alice", got!.Name);
    }

    [Fact]
    public async Task Insert_Guid_AutoGeneratesId()
    {
        var store = this.CreateStore();
        var node = (JsonObject)JsonNode.Parse("""{ "name": "no-id-yet" }""")!;

        await store.Insert(typeof(GuidIdModel), node);

        // Id injected into the node and readable.
        var id = node["id"]!.GetValue<Guid>();
        Assert.NotEqual(Guid.Empty, id);
        var got = await store.Get<GuidIdModel>(id);
        Assert.NotNull(got);
        Assert.Equal("no-id-yet", got!.Name);
    }

    [Fact]
    public async Task Insert_Int_AutoGeneratesId()
    {
        var store = this.CreateStore();
        var node = (JsonObject)JsonNode.Parse("""{ "name": "first" }""")!;

        await store.Insert(typeof(IntIdModel), node);

        Assert.Equal(1, node["id"]!.GetValue<int>());
    }

    [Fact]
    public async Task Insert_String_MissingId_Throws()
    {
        var store = this.CreateStore();
        var node = JsonNode.Parse("""{ "name": "no-id" }""")!;

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.Insert(typeof(StringIdModel), node));
    }

    [Fact]
    public async Task Insert_Array_WritesAll_ReturnsCount()
    {
        var store = this.CreateStore();
        var node = JsonNode.Parse("""[ { "id": "a", "name": "A" }, { "id": "b", "name": "B" } ]""")!;

        var n = await store.Insert(typeof(User), node);

        Assert.Equal(2, n);
        Assert.NotNull(await store.Get<User>("a"));
        Assert.NotNull(await store.Get<User>("b"));
    }

    [Fact]
    public async Task Insert_Array_DuplicateId_RollsBackWholeBatch()
    {
        var store = this.CreateStore();
        await store.Insert(typeof(User), JsonNode.Parse("""{ "id": "x", "name": "existing" }""")!);

        // Second element collides with the pre-existing "x" → the whole array must roll back.
        var batch = JsonNode.Parse("""[ { "id": "new1", "name": "N1" }, { "id": "x", "name": "dup" } ]""")!;
        await Assert.ThrowsAnyAsync<Exception>(() => store.Insert(typeof(User), batch));

        Assert.Null(await store.Get<User>("new1"));
    }

    [Fact]
    public async Task Update_Replaces_Document()
    {
        var store = this.CreateStore();
        await store.Insert(typeof(User), JsonNode.Parse("""{ "id": "u1", "name": "Alice" }""")!);

        await store.Update(typeof(User), JsonNode.Parse("""{ "id": "u1", "name": "Alicia" }""")!);

        var got = await store.Get<User>("u1");
        Assert.Equal("Alicia", got!.Name);
    }

    [Fact]
    public async Task Update_MissingDocument_Throws()
    {
        var store = this.CreateStore();
        await Assert.ThrowsAnyAsync<Exception>(() =>
            store.Update(typeof(User), JsonNode.Parse("""{ "id": "ghost", "name": "X" }""")!));
    }

    [Fact]
    public async Task Update_DefaultId_Throws()
    {
        var store = this.CreateStore();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.Update(typeof(User), JsonNode.Parse("""{ "name": "no-id" }""")!));
    }

    [Fact]
    public async Task Upsert_InsertsWhenAbsent_MergesWhenPresent()
    {
        var store = this.CreateStore();

        await store.Upsert(typeof(User), JsonNode.Parse("""{ "id": "u1", "name": "Alice" }""")!);
        Assert.Equal("Alice", (await store.Get<User>("u1"))!.Name);

        // Merge-patch: only the supplied member changes.
        await store.Upsert(typeof(User), JsonNode.Parse("""{ "id": "u1", "name": "Alice2" }""")!);
        Assert.Equal("Alice2", (await store.Get<User>("u1"))!.Name);
    }

    // ── Versioning ──────────────────────────────────────────────────────

    [Fact]
    public async Task Insert_SeedsVersion_And_Update_Bumps()
    {
        var store = this.CreateStore(o => o.MapVersionProperty<VersionedUser>(u => u.Version));

        await store.Insert(typeof(VersionedUser), JsonNode.Parse("""{ "id": "u1", "name": "A", "age": 1 }""")!);
        Assert.Equal(1, (await store.Get<VersionedUser>("u1"))!.Version);

        await store.Update(typeof(VersionedUser), JsonNode.Parse("""{ "id": "u1", "name": "A", "age": 2, "version": 1 }""")!);
        Assert.Equal(2, (await store.Get<VersionedUser>("u1"))!.Version);
    }

    [Fact]
    public async Task Update_StaleVersion_ThrowsConcurrency()
    {
        var store = this.CreateStore(o => o.MapVersionProperty<VersionedUser>(u => u.Version));
        await store.Insert(typeof(VersionedUser), JsonNode.Parse("""{ "id": "u1", "name": "A", "age": 1 }""")!);
        await store.Update(typeof(VersionedUser), JsonNode.Parse("""{ "id": "u1", "name": "A", "age": 2, "version": 1 }""")!);

        // Stale expected version 1 (stored is now 2).
        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            store.Update(typeof(VersionedUser), JsonNode.Parse("""{ "id": "u1", "name": "A", "age": 3, "version": 1 }""")!));
    }

    // ── Mapped-property presence + spatial sidecar ──────────────────────

    [Fact]
    public async Task Insert_MissingSpatialMember_Throws()
    {
        var store = this.CreateStore(o => o.MapSpatialProperty<Place>(p => p.Location));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.Insert(typeof(Place), JsonNode.Parse("""{ "id": "p1", "name": "no-location" }""")!));
        Assert.Contains("location", ex.Message);
    }

    [Fact]
    public async Task Insert_NullSpatialMember_IsAllowed()
    {
        var store = this.CreateStore(o => o.MapSpatialProperty<Place>(p => p.Location));

        // Present-but-null is a deliberate "no value" — writes the doc, skips the sidecar.
        // (Read back via the JSON lane; the typed GeoPoint struct converter can't represent a null location.)
        await store.Insert(typeof(Place), JsonNode.Parse("""{ "id": "p1", "name": "nowhere", "location": null }""")!);
        var raw = await store.Get(typeof(Place), "p1");
        Assert.NotNull(raw);
        Assert.Equal("nowhere", raw!["name"]!.GetValue<string>());
    }

    [Fact]
    public async Task Insert_WithGeoJson_WritesSpatialSidecar()
    {
        var store = this.CreateStore(o => o.MapSpatialProperty<Place>(p => p.Location));

        // Times Square, GeoJSON [lng, lat].
        await store.Insert(typeof(Place), JsonNode.Parse(
            """{ "id": "ts", "name": "TimesSquare", "location": { "type": "Point", "coordinates": [-73.9855, 40.7580] } }""")!);

        var near = await store.WithinRadius<Place>(new GeoPoint(40.7580, -73.9855), 1000);
        Assert.Contains(near, r => r.Document.Id == "ts");
    }

    [Fact]
    public async Task Upsert_WithId_SkipsPresenceCheck()
    {
        var store = this.CreateStore(o => o.MapSpatialProperty<Place>(p => p.Location));
        await store.Insert(typeof(Place), JsonNode.Parse(
            """{ "id": "ts", "name": "TimesSquare", "location": { "type": "Point", "coordinates": [-73.9855, 40.7580] } }""")!);

        // Partial merge carrying an Id must not require the location member.
        await store.Upsert(typeof(Place), JsonNode.Parse("""{ "id": "ts", "name": "Renamed" }""")!);
        Assert.Equal("Renamed", (await store.Get<Place>("ts"))!.Name);
    }

    // ── Interceptors ────────────────────────────────────────────────────

    [Fact]
    public async Task Interceptor_SeesJson_ButNullDocument()
    {
        string? seenJson = null;
        object? seenDocument = "sentinel";
        var store = this.CreateStore(o => o.OnBeforeWrite<User>((ctx, _) =>
        {
            seenJson = ctx.GetJson();
            seenDocument = ctx.Document;
            return Task.CompletedTask;
        }));

        await store.Insert(typeof(User), JsonNode.Parse("""{ "id": "u1", "name": "Alice" }""")!);

        Assert.Null(seenDocument);                 // no CLR instance → object-mutating interceptors no-op
        Assert.NotNull(seenJson);
        Assert.Contains("Alice", seenJson!);
    }

    // ── Shape / bad input ───────────────────────────────────────────────

    [Fact]
    public async Task Body_StoredAsIs()
    {
        var store = this.CreateStore();
        await store.Insert(typeof(User), JsonNode.Parse("""{ "id": "u1", "name": "Alice" }""")!);

        var raw = await store.Get(typeof(User), "u1");
        Assert.NotNull(raw);
        Assert.Equal("Alice", raw!["name"]!.GetValue<string>());
    }

    [Fact]
    public async Task Primitive_Throws()
    {
        var store = this.CreateStore();
        await Assert.ThrowsAsync<ArgumentException>(() => store.Insert(typeof(User), JsonValue.Create(5)!));
    }

    // ── Read lane ───────────────────────────────────────────────────────

    [Fact]
    public async Task Get_ReturnsJsonNode_OrNull()
    {
        var store = this.CreateStore();
        await store.Insert(typeof(User), JsonNode.Parse("""{ "id": "u1", "name": "Alice" }""")!);

        var got = await store.Get(typeof(User), "u1");
        Assert.NotNull(got);
        Assert.Equal("u1", got!["id"]!.GetValue<string>());

        Assert.Null(await store.Get(typeof(User), "missing"));
    }

    [Fact]
    public async Task Query_FiltersAsJsonNodes()
    {
        var store = this.CreateStore();
        await store.Insert(typeof(User), JsonNode.Parse("""[ { "id": "a", "name": "keep" }, { "id": "b", "name": "drop" } ]""")!);

        var results = await store.Query(typeof(User), "json_extract(Data, '$.name') = @n", new { n = "keep" });

        Assert.Single(results);
        Assert.Equal("a", results[0]!["id"]!.GetValue<string>());
    }

    [Fact]
    public async Task QueryStream_FiltersAsJsonNodes()
    {
        var store = this.CreateStore();
        await store.Insert(typeof(User), JsonNode.Parse("""[ { "id": "a", "name": "keep" }, { "id": "b", "name": "keep" } ]""")!);

        var count = 0;
        await foreach (var node in store.QueryStream(typeof(User), "json_extract(Data, '$.name') = @n", new { n = "keep" }))
        {
            Assert.Equal("keep", node!["name"]!.GetValue<string>());
            count++;
        }
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Insert_InjectsGeneratedId_IntoCallersNode()
    {
        var store = this.CreateStore();
        var node = (JsonObject)JsonNode.Parse("""{ "name": "no-id" }""")!;

        await store.Insert(typeof(GuidIdModel), node);

        // Mirrors typed Insert<T>: the generated Id is written back onto the caller's node.
        Assert.True(node.ContainsKey("id"));
        Assert.NotEqual(Guid.Empty, node["id"]!.GetValue<Guid>());
    }
}
