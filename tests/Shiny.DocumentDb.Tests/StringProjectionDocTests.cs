using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// <c>Project(string)</c> with fields + scalar functions on the document/in-memory providers (CosmosDB,
/// MongoDB, LiteDB) — evaluated client-side via the shared compile-free projection path.
/// </summary>
public abstract class StringProjectionDocTestsBase : IDisposable
{
    protected readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected StringProjectionDocTestsBase(IDocumentStoreFixture fixture)
        => this.store = fixture.CreateStore($"t{Guid.NewGuid():N}");

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public async Task FieldsAndFunctions()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "Smith", Permissions = Permissions.Read | Permissions.Write });
        await this.store.Insert(new Account { Id = "a2", Name = "Bob", Permissions = Permissions.Read });

        var rows = await this.store.Query<Account>()
            .Where(a => a.Name == "Smith")
            .Project("name, lower(name) as lname, length(name) as len, substring(name, 0, 2) as init", ctx.Account)
            .ToList();

        var row = Assert.Single(rows);
        Assert.Equal("Smith", row["name"]!.GetValue<string>());
        Assert.Equal("smith", row["lname"]!.GetValue<string>());
        Assert.Equal(5, row["len"]!.GetValue<int>());
        Assert.Equal("Sm", row["init"]!.GetValue<string>());
    }
}
