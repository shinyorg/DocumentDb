using System.Text.Json.Nodes;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The <see cref="IJsonDocumentCollection"/> contract, asserted identically on every provider that supports
/// it — and, for everything that applies to both, run twice: once name-keyed (schema-free) and once
/// type-keyed (late-bound over a registered type). That double run is the point of the suite: the two
/// keyings are one lane, so a behaviour that diverges between them is a bug unless it is one of the
/// documented differences (id generation, and the mappings/pipeline a CLR type brings).
/// </summary>
public abstract class JsonCollectionConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    /// <summary>The two keyings, as xUnit theory data.</summary>
    public static TheoryData<bool> Keyings => new() { true, false };

    IDocumentStore NewStore() => this.Fixture.CreateStore($"t{Guid.NewGuid():N}");

    static IJsonDocumentCollection Open(IDocumentStore store, bool typeKeyed)
        => typeKeyed ? store.Collection(typeof(User)) : store.Collection("orders");

    /// <summary>
    /// A numeric field path. Type-keyed collections read the leaf type from metadata; schema-free ones need
    /// the explicit <c>:type</c> hint, which is a parse error on the typed surface.
    /// </summary>
    static string NumAge(bool typeKeyed) => typeKeyed ? "age" : "age:number";

    // One body that satisfies both keyings. The keys are User's serialized names (the store's default
    // naming policy is camelCase) because a type-keyed collection stores the body AS-IS — and a schema-free
    // collection addresses whatever key is actually there, so the same paths work for it too.
    static JsonObject Doc(string id, string name, int age, string? email = "x@y.com")
    {
        var o = new JsonObject
        {
            ["id"] = id,
            ["name"] = name,
            ["age"] = age
        };
        o["email"] = email is null ? null : JsonValue.Create(email);
        return o;
    }

    async Task<(IDocumentStore Store, IJsonDocumentCollection Collection)> Seeded(bool typeKeyed)
    {
        var store = this.NewStore();
        var collection = Open(store, typeKeyed);
        await collection.Insert(new JsonArray(
            Doc("u1", "Alice", 30),
            Doc("u2", "Bob", 40),
            Doc("u3", "Carol", 50, null),
            Doc("u4", "Dave", 20)));
        return (store, collection);
    }

    // ── Identity ────────────────────────────────────────────────────────

    [Theory, MemberData(nameof(Keyings))]
    public void Collection_ReportsItsIdentity(bool typeKeyed)
    {
        using var store = (IDisposable)this.NewStore();
        var collection = Open((IDocumentStore)store, typeKeyed);

        Assert.Equal(typeKeyed ? typeof(User) : null, collection.DocumentType);
        Assert.False(string.IsNullOrWhiteSpace(collection.Name));
        Assert.False(string.IsNullOrWhiteSpace(collection.IdProperty));
    }

    [Fact]
    public void Collection_IsCachedPerName()
    {
        using var store = (IDisposable)this.NewStore();
        var s = (IDocumentStore)store;
        Assert.Same(s.Collection("orders"), s.Collection("orders"));
        Assert.Same(s.Collection(typeof(User)), s.Collection(typeof(User)));
        Assert.NotSame(s.Collection("orders"), s.Collection("invoices"));
    }

    // ── Writes ──────────────────────────────────────────────────────────

    [Theory, MemberData(nameof(Keyings))]
    public async Task Insert_ReturnsStoredId_AndRoundTrips(bool typeKeyed)
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = Open((IDocumentStore)owner, typeKeyed);

        var id = await collection.Insert(Doc("u1", "Alice", 30));

        Assert.Equal("u1", id);
        var got = await collection.Get("u1");
        Assert.NotNull(got);
        Assert.Equal("Alice", got!["name"]!.GetValue<string>());
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Insert_Array_IsAtomic(bool typeKeyed)
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = Open((IDocumentStore)owner, typeKeyed);
        await collection.Insert(Doc("dup", "Existing", 1));

        var batch = new JsonArray(Doc("fresh", "Fresh", 2), Doc("dup", "Collides", 3));
        await Assert.ThrowsAnyAsync<Exception>(() => collection.Insert(batch));

        Assert.Null(await collection.Get("fresh"));
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Update_Patch_LeavesUntouchedKeysAlone(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        var n = await collection.Update(new JsonObject { ["id"] = "u1", ["age"] = 31 }, patch: true);

        Assert.Equal(1, n);
        var got = await collection.Get("u1");
        Assert.Equal(31, got!["age"]!.GetValue<int>());
        Assert.Equal("Alice", got["name"]!.GetValue<string>());
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Update_Replace_DropsAbsentKeys(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        await collection.Update(new JsonObject { ["id"] = "u1", ["name"] = "OnlyName" });

        var got = await collection.Get("u1");
        Assert.Equal("OnlyName", got!["name"]!.GetValue<string>());
        Assert.False(got.ContainsKey("age"));
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Upsert_InsertsWhenAbsent_MergesWhenPresent(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        await collection.Upsert(Doc("new1", "New", 1));
        Assert.NotNull(await collection.Get("new1"));

        await collection.Upsert(new JsonObject { ["id"] = "u1", ["age"] = 99 });
        var merged = await collection.Get("u1");
        Assert.Equal(99, merged!["age"]!.GetValue<int>());
        Assert.Equal("Alice", merged["name"]!.GetValue<string>());
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task BatchUpsert_WritesAll(bool typeKeyed)
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = Open((IDocumentStore)owner, typeKeyed);

        var n = await collection.BatchUpsert([Doc("b1", "One", 1), Doc("b2", "Two", 2)]);

        Assert.Equal(2, n);
        Assert.Equal(2, await collection.Query().Count());
    }

    // ── Ids ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Insert_SchemaFree_GeneratesSortableId_AndStampsIt()
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = ((IDocumentStore)owner).Collection("orders");

        var body = new JsonObject { ["name"] = "no id" };
        var id = await collection.Insert(body);

        Assert.False(string.IsNullOrWhiteSpace(id));
        // Stamped back onto the caller's object, so the body and the Id column cannot disagree.
        Assert.Equal(id, body["id"]!.GetValue<string>());
        Assert.NotNull(await collection.Get(id));
    }

    [Fact]
    public async Task Insert_SchemaFree_GeneratedIdsAreTimeOrdered()
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = ((IDocumentStore)owner).Collection("orders");

        var first = await collection.Insert(new JsonObject { ["n"] = 1 });
        await Task.Delay(5);
        var second = await collection.Insert(new JsonObject { ["n"] = 2 });

        // UUIDv7 leads with a 48-bit millisecond timestamp, which is the whole reason it is the schema-free
        // default: ids sort in insert order. Only the timestamp is ordered — two ids minted inside the same
        // millisecond differ in their random tail — so compare the prefix and space the writes.
        Assert.Equal(32, first.Length);
        Assert.True(string.CompareOrdinal(first[..12], second[..12]) < 0,
            $"expected a time-ordered id but got {first} then {second}");
    }

    [Fact]
    public async Task Insert_SchemaFree_ReadsIdCaseInsensitively()
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = ((IDocumentStore)owner).Collection("orders");

        await collection.Insert(new JsonObject { ["ID"] = "shouty", ["n"] = 1 });

        Assert.NotNull(await collection.Get("shouty"));
    }

    [Fact]
    public async Task SchemaFree_CustomIdProperty()
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = ((IDocumentStore)owner).Collection("orders", idProperty: "orderNo");

        var id = await collection.Insert(new JsonObject { ["orderNo"] = "SO-1", ["total"] = 10 });

        Assert.Equal("SO-1", id);
        Assert.Equal("orderNo", collection.IdProperty);
        Assert.NotNull(await collection.Get("SO-1"));
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Get_Miss_ReturnsNull(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        Assert.Null(await collection.Get("nope"));
    }

    // ── Isolation ───────────────────────────────────────────────────────

    [Fact]
    public async Task Collections_AndTypedQueries_ShareATableWithoutSeeingEachOther()
    {
        using var owner = (IDisposable)this.NewStore();
        var store = (IDocumentStore)owner;

        await store.Collection("orders").Insert(new JsonObject { ["id"] = "o1", ["total"] = 1 });
        await store.Collection("invoices").Insert(new JsonObject { ["id"] = "i1", ["total"] = 2 });
        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });

        Assert.Equal(1, await store.Collection("orders").Query().Count());
        Assert.Equal(1, await store.Collection("invoices").Query().Count());
        Assert.Equal(1, await store.Query<User>().Count());
        Assert.Null(await store.Collection("orders").Get("i1"));
    }

    // ── Query grammar ───────────────────────────────────────────────────

    [Theory, MemberData(nameof(Keyings))]
    public async Task Where_Comparisons(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        Assert.Equal(2, await collection.Query().Where(NumAge(typeKeyed) + " >= 40").Count());
        Assert.Equal(1, await collection.Query().Where("name == 'Alice'").Count());
        Assert.Equal(3, await collection.Query().Where("name != 'Alice'").Count());
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Where_Composes_WithAnd(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        // Two Where calls is the regression that catches a missing VisitChildren override on the field node:
        // combining predicates runs a visitor over the second one.
        var n = await collection.Query().Where(NumAge(typeKeyed) + " >= 30").Where(NumAge(typeKeyed) + " < 50").Count();

        Assert.Equal(2, n);
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Where_NullChecks_InList_AndLogicalOperators(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        Assert.Equal(1, await collection.Query().Where("email is null").Count());
        Assert.Equal(3, await collection.Query().Where("email is not null").Count());
        Assert.Equal(2, await collection.Query().Where("name in ('Alice', 'Bob')").Count());
        Assert.Equal(2, await collection.Query().Where("name == 'Alice' or name == 'Carol'").Count());
        Assert.Equal(3, await collection.Query().Where("not (name == 'Alice')").Count());
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Where_StringFunctions(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        Assert.Equal(1, await collection.Query().Where("lower(name) == 'alice'").Count());
        Assert.Equal(1, await collection.Query().Where("contains(name, 'lic')").Count());
        Assert.Equal(1, await collection.Query().Where("startswith(name, 'Bo')").Count());
        Assert.Equal(1, await collection.Query().Where("length(name) == 3").Count());
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Where_Interpolated_BindsAsParameter(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        var name = "Alice";
        var minAge = 25;
        var n = await collection.Query().Where($"name == {name} and age >= {minAge}").Count();

        Assert.Equal(1, n);
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task OrderBy_AndPaginate(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        var byName = await collection.Query().OrderBy("name").ToList();
        Assert.Equal(["Alice", "Bob", "Carol", "Dave"], byName.Select(d => d["name"]!.GetValue<string>()));

        var desc = await collection.Query().OrderBy("name desc").Paginate(0, 2).ToList();
        Assert.Equal(["Dave", "Carol"], desc.Select(d => d["name"]!.GetValue<string>()));
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task OrderBy_Numeric_SortsNumerically(bool typeKeyed)
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = Open((IDocumentStore)owner, typeKeyed);
        // 9 / 10 / 100 sort differently as text than as numbers — the whole point of the type hint.
        await collection.Insert(new JsonArray(Doc("a", "A", 9), Doc("b", "B", 10), Doc("c", "C", 100)));

        var ages = (await collection.Query().OrderBy(NumAge(typeKeyed)).ToList())
            .Select(d => d["age"]!.GetValue<int>());

        Assert.Equal([9, 10, 100], ages);
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task OrderBy_MultipleKeys(bool typeKeyed)
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = Open((IDocumentStore)owner, typeKeyed);
        await collection.Insert(new JsonArray(
            Doc("a", "Same", 2), Doc("b", "Same", 1), Doc("c", "Other", 3)));

        var ids = (await collection.Query().OrderBy("name, " + NumAge(typeKeyed)).ToList())
            .Select(d => d["id"]!.GetValue<string>());

        Assert.Equal(["c", "b", "a"], ids);
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Terminals(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        Assert.Equal(4, await collection.Query().Count());
        Assert.True(await collection.Query().Any());
        Assert.False(await collection.Query().Where("name == 'Nobody'").Any());
        Assert.Equal(4, (await collection.Query().ToList()).Count);

        var streamed = new List<JsonObject>();
        await foreach (var d in collection.Query().ToAsyncEnumerable())
            streamed.Add(d);
        Assert.Equal(4, streamed.Count);
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task ToQueryString_DoesNotExecute(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        var q = collection.Query().Where(NumAge(typeKeyed) + " > 30").ToQueryString();

        Assert.False(string.IsNullOrWhiteSpace(q.Sql));
        Assert.Contains("@typeName", q.Parameters.Keys);
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task CursorPage_WalksEveryDocument(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        var seen = new List<string>();
        string? cursor = null;
        do
        {
            var page = await collection.Query().OrderBy("name").ToCursorPage(cursor, 2);
            seen.AddRange(page.Items.Select(d => d["name"]!.GetValue<string>()));
            cursor = page.NextCursor;
        }
        while (cursor != null);

        Assert.Equal(["Alice", "Bob", "Carol", "Dave"], seen);
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Project_SelectsFieldsAtTheDatabase(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        var rows = await collection.Query().Where("name == 'Alice'").Project("name, lower(name) as lowered").ToList();

        var row = Assert.Single(rows);
        Assert.Equal("Alice", row["name"]!.GetValue<string>());
        Assert.Equal("alice", row["lowered"]!.GetValue<string>());
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task GroupBy_WithHaving(bool typeKeyed)
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = Open((IDocumentStore)owner, typeKeyed);
        await collection.Insert(new JsonArray(
            Doc("a", "Repeat", 10), Doc("b", "Repeat", 20), Doc("c", "Once", 5)));

        var rows = await collection.Query()
            .GroupBy("name")
            .Having("count() > 1")
            .Project("name, count() as n, sum(age) as total")
            .ToList();

        var row = Assert.Single(rows);
        Assert.Equal("Repeat", row["name"]!.GetValue<string>());
        Assert.Equal(2, row["n"]!.GetValue<int>());
        Assert.Equal(30, row["total"]!.GetValue<double>());
    }

    // ── Set-based writes ────────────────────────────────────────────────

    [Theory, MemberData(nameof(Keyings))]
    public async Task ExecuteUpdate_AndExecuteDelete(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        var updated = await collection.Query().Where(NumAge(typeKeyed) + " >= 40").ExecuteUpdate("name", "Senior");
        Assert.Equal(2, updated);
        Assert.Equal(2, await collection.Query().Where("name == 'Senior'").Count());

        var deleted = await collection.Query().Where("name == 'Senior'").ExecuteDelete();
        Assert.Equal(2, deleted);
        Assert.Equal(2, await collection.Query().Count());
    }

    [Theory, MemberData(nameof(Keyings))]
    public async Task Remove_BatchRemove_Clear(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        Assert.True(await collection.Remove("u1"));
        Assert.False(await collection.Remove("u1"));
        Assert.Equal(3, await collection.Query().Count());

        Assert.Equal(2, await collection.BatchRemove(["u2", "u3"]));
        Assert.Equal(1, await collection.Query().Count());

        Assert.Equal(1, await collection.Clear());
        Assert.Equal(0, await collection.Query().Count());
    }

    // ── Raw SQL escape hatch ────────────────────────────────────────────

    [Theory, MemberData(nameof(Keyings))]
    public async Task RawWhereClause_Query_AndStream(bool typeKeyed)
    {
        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        var rows = await collection.Query("1 = 1");
        Assert.Equal(4, rows.Count);

        var streamed = 0;
        await foreach (var _row in collection.QueryStream("1 = 1"))
            streamed++;
        Assert.Equal(4, streamed);
    }

    // ── Indexes ─────────────────────────────────────────────────────────

    /// <summary>
    /// Overridden to <c>false</c> by the providers whose engine rejects an index over a JSON extraction
    /// expression. This is a pre-existing provider gap shared with the typed <c>CreateIndexAsync</c>, not
    /// something JSON collections introduce.
    /// </summary>
    protected virtual bool SupportsJsonExpressionIndexes => true;

    [Theory, MemberData(nameof(Keyings))]
    public async Task CreateIndex_ThenQueriesStillMatch(bool typeKeyed)
    {
        if (!this.SupportsJsonExpressionIndexes)
            return;

        var (store, collection) = await this.Seeded(typeKeyed);
        using var _ = (IDisposable)store;

        // Creating the same index twice is deliberately not asserted: MySQL and Oracle have no
        // CREATE INDEX IF NOT EXISTS, so re-creation throws there — the same as the typed CreateIndexAsync.
        await collection.CreateIndex(default, "name");

        Assert.Equal(1, await collection.Query().Where("name == 'Alice'").Count());
    }

    // ── Validation ──────────────────────────────────────────────────────

    [Fact]
    public void SchemaFree_RejectsUnsafeCollectionNames()
    {
        using var owner = (IDisposable)this.NewStore();
        var store = (IDocumentStore)owner;

        Assert.Throws<ArgumentException>(() => store.Collection("orders'; DROP TABLE x--"));
        Assert.Throws<ArgumentException>(() => store.Collection("has space"));
        Assert.Throws<ArgumentException>(() => store.Collection("1leading"));
        Assert.Throws<ArgumentException>(() => store.Collection("dotted.name"));
    }

    [Fact]
    public async Task SchemaFree_RejectsUnsafeFieldPaths()
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = ((IDocumentStore)owner).Collection("orders");

        await Assert.ThrowsAnyAsync<ArgumentException>(() => collection.Query().Where("a[0] == 1").Count());
        await Assert.ThrowsAnyAsync<ArgumentException>(() => collection.CreateIndex(default, "bad-path"));
    }

    [Fact]
    public async Task SchemaFree_MappingBackedFunctionsThrowWithAPointer()
    {
        using var owner = (IDisposable)this.NewStore();
        var collection = ((IDocumentStore)owner).Collection("orders");

        var hasFlag = await Assert.ThrowsAnyAsync<ArgumentException>(
            () => collection.Query().Where("hasflag(perms, 'Write')").Count());
        Assert.Contains("Query<T>()", hasFlag.Message);

        var spatial = await Assert.ThrowsAnyAsync<ArgumentException>(
            () => collection.Query().Where("intersects(area, '{\"type\":\"Point\",\"coordinates\":[0,0]}')").Count());
        Assert.Contains("Query<T>()", spatial.Message);
    }
}
