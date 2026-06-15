using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// A user-defined function with a real body (so in-memory providers run it) that we map to a SQL
// function via MapFunctionTranslation. Backed here by the registered `soundex` UDF on SQLite.
public static class CustomFns
{
    public static string Phonetic(string value) => DocumentFunctions.Soundex(value);
}

[Collection("SQLite")]
public class CustomTranslationTests : IDisposable
{
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
    readonly IDocumentStore store;

    public CustomTranslationTests(SqliteDatabaseFixture db)
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = db.CreateProvider(),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        };
        options.MapFunctionTranslation(() => CustomFns.Phonetic(default!), "soundex");
        this.store = new DocumentStore(options);
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public async Task CustomFunction_TranslatesToSql()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "Smith" }, ctx.Account);
        await this.store.Insert(new Account { Id = "a2", Name = "Smyth" }, ctx.Account);
        await this.store.Insert(new Account { Id = "a3", Name = "Jones" }, ctx.Account);

        var results = await this.store.Query(ctx.Account)
            .Where(a => CustomFns.Phonetic(a.Name) == CustomFns.Phonetic("Smith"))
            .ToList();

        Assert.Equal(["a1", "a2"], results.Select(a => a.Id).OrderBy(x => x));
    }
}
