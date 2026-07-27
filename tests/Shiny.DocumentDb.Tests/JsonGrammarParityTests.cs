using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// The query-surface parity rule, enforced: the <em>same filter string</em> run against
/// <c>Query&lt;User&gt;()</c>, <c>Collection(typeof(User)).Query()</c> and <c>Collection("users").Query()</c>
/// must select the same documents.
/// </summary>
/// <remarks>
/// This is what makes a one-sided addition to the grammar a test failure rather than a silent gap. The
/// documented exceptions are the functions that need a <c>Type</c>-keyed registration — they are asserted to
/// throw with a pointer to the typed surface instead.
/// </remarks>
public class JsonGrammarParityTests : IDisposable
{
    readonly List<IDisposable> disposables = [];

    DocumentStore CreateStore()
    {
        var connectionString = $"Data Source=parity_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        var holdOpen = new SqliteConnection(connectionString);
        holdOpen.Open();
        this.disposables.Add(holdOpen);

        var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider(connectionString),
            TableName = $"t{Guid.NewGuid():N}"
        });
        this.disposables.Add(store);
        return store;
    }

    public void Dispose()
    {
        for (var i = this.disposables.Count - 1; i >= 0; i--)
            this.disposables[i].Dispose();
    }

    sealed record Row(string Id, string Name, int Age, string? Email, string Created);

    static readonly Row[] Seed =
    [
        new("u1", "Alice", 30, "alice@x.com", "2026-01-15T10:00:00"),
        new("u2", "Bob", 40, "bob@y.com", "2026-03-02T08:30:00"),
        new("u3", "Carol", 50, null, "2026-07-20T22:15:00"),
        new("u4", "dave", 20, "dave@x.com", "2025-11-05T06:45:00")
    ];

    async Task<(IDocumentStore Store, IJsonDocumentCollection Typed, IJsonDocumentCollection Free)> Seeded()
    {
        var store = this.CreateStore();
        var typed = store.Collection(typeof(ParityDoc));
        var free = store.Collection("parity_free");

        foreach (var r in Seed)
        {
            var body = new JsonObject
            {
                ["id"] = r.Id,
                ["name"] = r.Name,
                ["age"] = r.Age,
                ["created"] = r.Created
            };
            body["email"] = r.Email is null ? null : JsonValue.Create(r.Email);
            await typed.Insert(body.DeepClone().AsObject());
            await free.Insert(body.DeepClone().AsObject());
        }
        return (store, typed, free);
    }

    public class ParityDoc
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public int Age { get; set; }
        public string? Email { get; set; }
        public DateTime Created { get; set; }
    }

    /// <summary>
    /// One case per grammar function that a schema-free collection can reach. Where a hint is needed, the
    /// schema-free form carries it — that difference is the documented contract, not a parity break.
    /// </summary>
    public static TheoryData<string, string> SharedFunctions => new()
    {
        // filter (typed form),                  filter (schema-free form)
        { "age > 30",                            "age:number > 30" },
        { "age >= 30 and age < 50",              "age:number >= 30 and age:number < 50" },
        { "name == 'Alice'",                     "name == 'Alice'" },
        { "name != 'Alice'",                     "name != 'Alice'" },
        { "email is null",                       "email is null" },
        { "email is not null",                   "email is not null" },
        { "name in ('Alice', 'Bob')",            "name in ('Alice', 'Bob')" },
        { "not (name == 'Alice')",               "not (name == 'Alice')" },
        { "contains(name, 'li')",                "contains(name, 'li')" },
        { "startswith(name, 'A')",               "startswith(name, 'A')" },
        { "endswith(name, 'e')",                 "endswith(name, 'e')" },
        { "isnullorempty(email)",                "isnullorempty(email)" },
        { "lower(name) == 'alice'",              "lower(name) == 'alice'" },
        { "upper(name) == 'ALICE'",              "upper(name) == 'ALICE'" },
        { "trim(name) == 'Alice'",               "trim(name) == 'Alice'" },
        { "ltrim(name) == 'Alice'",              "ltrim(name) == 'Alice'" },
        { "rtrim(name) == 'Alice'",              "rtrim(name) == 'Alice'" },
        { "length(name) == 5",                   "length(name) == 5" },
        { "substring(name, 0, 2) == 'Al'",       "substring(name, 0, 2) == 'Al'" },
        { "replace(name, 'A', 'X') == 'Xlice'",  "replace(name, 'A', 'X') == 'Xlice'" },
        { "indexof(name, 'li') == 1",            "indexof(name, 'li') == 1" },
        { "abs(age) == 30",                      "abs(age) == 30" },
        { "ceiling(age) == 30",                  "ceiling(age) == 30" },
        { "ceil(age) == 30",                     "ceil(age) == 30" },
        { "floor(age) == 30",                    "floor(age) == 30" },
        { "round(age) == 30",                    "round(age) == 30" },
        { "round(age, 0) == 30",                 "round(age, 0) == 30" },
        { "sqrt(age) > 5",                       "sqrt(age) > 5" },
        { "sign(age) == 1",                      "sign(age) == 1" },
        { "pow(age, 2) > 800",                   "pow(age, 2) > 800" },
        { "concat(name, name) == 'AliceAlice'",  "concat(name, name) == 'AliceAlice'" },
        { "soundex(name) == soundex('Alise')",   "soundex(name) == soundex('Alise')" },
        { "year(created) == 2026",               "year(created:date) == 2026" },
        { "month(created) == 1",                 "month(created:date) == 1" },
        { "day(created) == 15",                  "day(created:date) == 15" },
        { "hour(created) == 10",                 "hour(created:date) == 10" },
        { "minute(created) == 0",                "minute(created:date) == 0" },
        { "second(created) == 0",                "second(created:date) == 0" }
    };

    [Theory, MemberData(nameof(SharedFunctions))]
    public async Task SameFilter_SelectsSameDocuments_OnAllThreeSurfaces(string typedFilter, string schemaFreeFilter)
    {
        var (store, typed, free) = await this.Seeded();
        using var _ = (IDisposable)store;

        var viaTypedQuery = (await store.Query<ParityDoc>().Where(typedFilter).ToList())
            .Select(d => d.Id).OrderBy(x => x).ToList();
        var viaTypedCollection = (await typed.Query().Where(typedFilter).ToList())
            .Select(d => d["id"]!.GetValue<string>()).OrderBy(x => x).ToList();
        var viaSchemaFree = (await free.Query().Where(schemaFreeFilter).ToList())
            .Select(d => d["id"]!.GetValue<string>()).OrderBy(x => x).ToList();

        Assert.Equal(viaTypedQuery, viaTypedCollection);
        Assert.Equal(viaTypedQuery, viaSchemaFree);
    }

    /// <summary>
    /// The three documented exceptions: each needs a registration keyed by CLR type, so a schema-free
    /// collection cannot reach it and says which surface can.
    /// </summary>
    [Theory]
    [InlineData("hasflag(perms, 'Write')")]
    [InlineData("intersects(area, '{\"type\":\"Point\",\"coordinates\":[0,0]}')")]
    [InlineData("within(area, '{\"type\":\"Point\",\"coordinates\":[0,0]}')")]
    [InlineData("withindistance(area, '{\"type\":\"Point\",\"coordinates\":[0,0]}', 100)")]
    [InlineData("lucenematch(name, 'alice')")]
    [InlineData("lucenescore(name, 'alice') > 0")]
    public async Task MappingBackedFunctions_ThrowWithAPointerToTheTypedSurface(string filter)
    {
        var (store, _typed, free) = await this.Seeded();
        using var _ = (IDisposable)store;

        var ex = await Assert.ThrowsAnyAsync<ArgumentException>(() => free.Query().Where(filter).Count());

        Assert.Contains("schema-free", ex.Message);
        Assert.Contains("Query<T>()", ex.Message);
    }

    [Fact]
    public async Task TypeHint_IsAParseErrorOnATypeKeyedCollection()
    {
        var (store, typed, _free) = await this.Seeded();
        using var _ = (IDisposable)store;

        // The typed surface knows the leaf type, so a hint is not merely unnecessary — it is not grammar.
        await Assert.ThrowsAnyAsync<ArgumentException>(() => typed.Query().Where("age:number > 30").Count());
    }
}
