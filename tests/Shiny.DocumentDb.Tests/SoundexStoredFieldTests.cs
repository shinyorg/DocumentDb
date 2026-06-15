using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Phonetic search on providers without a native/registered <c>soundex</c> function (CosmosDB, MongoDB,
/// LiteDB) via the recommended computed-stored-field pattern: store a precomputed Soundex key on each
/// document and query it by equality (compute the search term's key in C# first).
/// </summary>
public abstract class SoundexStoredFieldTestsBase : IDisposable
{
    protected readonly IDocumentStore store;

    protected SoundexStoredFieldTestsBase(IDocumentStoreFixture fixture)
        => this.store = fixture.CreateStore($"t{Guid.NewGuid():N}");

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public async Task StoredSoundexField_MatchesPhonetically()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "Smith", NameSoundex = DocumentFunctions.Soundex("Smith") });
        await this.store.Insert(new Account { Id = "a2", Name = "Smyth", NameSoundex = DocumentFunctions.Soundex("Smyth") });
        await this.store.Insert(new Account { Id = "a3", Name = "Jones", NameSoundex = DocumentFunctions.Soundex("Jones") });

        var key = DocumentFunctions.Soundex("Smith");
        var r = await this.store.Query<Account>().Where(a => a.NameSoundex == key).ToList();

        Assert.Equal(["a1", "a2"], r.Select(a => a.Id).OrderBy(x => x));
    }
}
