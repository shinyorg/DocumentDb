using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Flag-enum querying via <c>HasFlag</c> on the document-NoSQL backends (Cosmos bitwise, MongoDB
/// <c>$bitsAllSet</c>). Relational coverage lives in <see cref="ScalarFunctionTestsBase"/>.
/// </summary>
public abstract class FlagEnumTestsBase : IDisposable
{
    protected readonly IDocumentStore store;

    protected FlagEnumTestsBase(IDocumentStoreFixture fixture)
        => this.store = fixture.CreateStore($"t{Guid.NewGuid():N}");

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    async Task SeedAsync()
    {
        await this.store.Insert(new Account { Id = "a1", Name = "Alice", Permissions = Permissions.Read | Permissions.Write });
        await this.store.Insert(new Account { Id = "a2", Name = "Bob", Permissions = Permissions.Read });
        await this.store.Insert(new Account { Id = "a3", Name = "Smith", Permissions = Permissions.Admin });
    }

    [Fact]
    public async Task HasFlag_Write()
    {
        await this.SeedAsync();
        var r = await this.store.Query<Account>().Where(a => a.Permissions.HasFlag(Permissions.Write)).ToList();
        Assert.Equal(["a1", "a3"], r.Select(a => a.Id).OrderBy(x => x));
    }

    [Fact]
    public async Task HasFlag_Delete()
    {
        await this.SeedAsync();
        var r = await this.store.Query<Account>().Where(a => a.Permissions.HasFlag(Permissions.Delete)).ToList();
        Assert.Equal(["a3"], r.Select(a => a.Id));
    }
}
