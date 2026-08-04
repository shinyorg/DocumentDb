using System.Text.Json;
using System.Text.Json.Nodes;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// What the raw terminals promise <em>beyond</em> the cross-provider contract in
/// <see cref="DocumentQueryConformanceTestsBase"/>: on a store that persists the body as JSON, the body the
/// caller gets is the stored one, not a re-serialize of a materialized document. SQLite stands in for the whole
/// relational tier — they all read the same <c>Data</c> column through the same query.
/// </summary>
public class RawJsonQueryTests
{
    static DocumentStore CreateStore() => new(new DocumentStoreOptions
    {
        DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
        TableName = $"t{Guid.NewGuid():N}"
    });

    [Fact]
    public async Task RawTerminals_ReturnThePersistedBody_NotARoundTripThroughT()
    {
        using var store = CreateStore();
        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });

        // Give the stored body a property User does not have. A read that round-tripped through User would drop
        // it, so its survival is the proof that nothing deserialized.
        var collection = store.Collection<User>();
        var stored = (await collection.Get("u1"))!;
        stored["extra"] = "kept";
        await collection.Update(stored);

        var raw = await store.Query<User>().Where(x => x.Id == "u1").FirstOrDefaultRawJson();

        Assert.NotNull(raw);
        Assert.Equal("kept", JsonNode.Parse(raw!)!["extra"]!.GetValue<string>());
        Assert.Equal("kept", (await store.Query<User>().ToJsonList())[0]["extra"]!.GetValue<string>());

        // The typed lane still materializes normally — it just ignores what it does not know about.
        Assert.Equal("Alice", (await store.Query<User>().Where(x => x.Id == "u1").First()).Name);
    }

    [Fact]
    public async Task WriteJsonArrayTo_ProducesAResponseBodyWithoutBufferingTheSet()
    {
        using var store = CreateStore();
        for (var i = 0; i < 5; i++)
            await store.Insert(new User { Id = $"u{i}", Name = $"N{i}", Age = 20 + i });

        using var stream = new MemoryStream();
        var count = await store.Query<User>()
            .Where(x => x.Age >= 22)
            .OrderByDescending(x => x.Age)
            .WriteJsonArrayTo(stream);

        Assert.Equal(3, count);

        var array = JsonNode.Parse(stream.ToArray())!.AsArray();
        Assert.Equal(
            ["u4", "u3", "u2"],
            array.Select(n => n.Deserialize<User>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!.Id).ToArray());
    }
}
