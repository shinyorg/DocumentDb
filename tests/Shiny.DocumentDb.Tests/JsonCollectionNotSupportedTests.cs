using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Pins the NotSupported tier. JSON collections work against the shared
/// <c>Id / TypeName / Data / CreatedAt / UpdatedAt</c> envelope over ADO.NET, which only the relational
/// providers expose — so every other provider inherits the default-throwing interface members.
/// </summary>
/// <remarks>
/// Asserted per provider rather than assumed, so a store that later grows an override (or accidentally
/// satisfies the member some other way) shows up here instead of silently changing the supported tier.
/// </remarks>
public abstract class JsonCollectionNotSupportedTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    [Fact]
    public void Collection_ByName_Throws()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        Assert.Throws<NotSupportedException>(() => ((IDocumentStore)owner).Collection("orders"));
    }

    [Fact]
    public void Collection_ByType_Throws()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        Assert.Throws<NotSupportedException>(() => ((IDocumentStore)owner).Collection(typeof(User)));
    }
}
