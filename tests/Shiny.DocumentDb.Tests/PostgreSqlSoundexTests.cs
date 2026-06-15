using System.Text.Json;
using Shiny.DocumentDb.PostgreSql;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.PostgreSql;

/// <summary>
/// Native PostgreSQL Soundex via the <c>fuzzystrmatch</c> extension (enabled with
/// <see cref="PostgreSqlDatabaseProvider.EnableFuzzyStringMatch"/>).
/// </summary>
[Collection("PostgreSQL")]
public class PostgreSqlSoundexTests : IDisposable
{
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
    readonly IDocumentStore store;

    public PostgreSqlSoundexTests(PostgreSqlDatabaseFixture db)
    {
        var baseProvider = (PostgreSqlDatabaseProvider)db.CreateProvider();
        var provider = new PostgreSqlDatabaseProvider(baseProvider.ConnectionString) { EnableFuzzyStringMatch = true };
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = provider,
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public async Task NativeSoundex_MatchesPhonetically()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "Smith" }, ctx.Account);
        await this.store.Insert(new Account { Id = "a2", Name = "Smyth" }, ctx.Account);
        await this.store.Insert(new Account { Id = "a3", Name = "Jones" }, ctx.Account);

        var r = await this.store.Query(ctx.Account)
            .Where(a => DocumentFunctions.Soundex(a.Name) == DocumentFunctions.Soundex("Smith"))
            .ToList();

        Assert.Equal(["a1", "a2"], r.Select(a => a.Id).OrderBy(x => x));
    }
}
