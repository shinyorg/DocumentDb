using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Phonetic querying via <see cref="DocumentFunctions.Soundex"/>. On SQLite the canonical CLR Soundex is
/// registered as a connection UDF (the bundled build omits <c>SQLITE_SOUNDEX</c>).
/// </summary>
public abstract class SoundexTestsBase : IDisposable
{
    protected readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected SoundexTestsBase(IDatabaseFixture fixture)
        => this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        });

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public async Task Soundex_MatchesPhoneticallySimilar()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "Smith" }, ctx.Account);
        await this.store.Insert(new Account { Id = "a2", Name = "Smyth" }, ctx.Account);
        await this.store.Insert(new Account { Id = "a3", Name = "Jones" }, ctx.Account);

        var results = await this.store.Query(ctx.Account)
            .Where(a => DocumentFunctions.Soundex(a.Name) == DocumentFunctions.Soundex("Smith"))
            .ToList();

        Assert.Equal(["a1", "a2"], results.Select(a => a.Id).OrderBy(x => x));
    }

    [Fact]
    public void Soundex_CanonicalCodes()
    {
        // Sanity-check the canonical algorithm the UDF and in-memory path share.
        Assert.Equal("S530", DocumentFunctions.Soundex("Smith"));
        Assert.Equal("S530", DocumentFunctions.Soundex("Smyth"));
        Assert.Equal("R163", DocumentFunctions.Soundex("Robert"));
        Assert.Equal("R163", DocumentFunctions.Soundex("Rupert"));
        Assert.Equal("T522", DocumentFunctions.Soundex("Tymczak"));
        Assert.Equal("0000", DocumentFunctions.Soundex(""));
    }
}
