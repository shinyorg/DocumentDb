using Shiny.DocumentDb.DocumentDb;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.DocumentDb;

// ── Inherited conformance suite (Amazon DocumentDB is a thin MongoDB-wire-compatible subclass) ──
// Mirrors MongoDbProviderTests: the same behaviour flows through DocumentDbDocumentStore unchanged.

[Collection("DocumentDB")]
public class DocumentStoreTests(DocumentDbDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("DocumentDB")]
public class QueryFilterTests(DocumentDbDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("DocumentDB")]
public class VersionMappingTests(DocumentDbDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("DocumentDB")]
public class TemporalTests(DocumentDbDatabaseFixture db) : TemporalTestsBase(db);

[Collection("DocumentDB")]
public class FlagEnumTests(DocumentDbDatabaseFixture db) : FlagEnumTestsBase(db);

[Collection("DocumentDB")]
public class SoundexStoredFieldTests(DocumentDbDatabaseFixture db) : SoundexStoredFieldTestsBase(db);

[Collection("DocumentDB")]
public class StringProjectionDocTests(DocumentDbDatabaseFixture db) : StringProjectionDocTestsBase(db);

/// <summary>
/// Amazon-DocumentDB-specific behaviour: the down-flagged full-text and vector capabilities throw a
/// DocumentDB-specific message, and CRUD / query / CAS work through the subclass over the wire-compatible
/// container.
/// </summary>
[Collection("DocumentDB")]
public class DocumentDbSpecificTests(DocumentDbDatabaseFixture db)
{
    [Fact]
    public void FullText_IsDownFlagged()
    {
        using var store = (IDisposable)db.CreateStore($"t{Guid.NewGuid():N}");
        Assert.False(((IDocumentStore)store).SupportsFullText);
    }

    [Fact]
    public async Task FullTextSearch_Throws_DocumentDbSpecificMessage()
    {
        using var store = (IDisposable)db.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            () => s.FullTextSearch<User>("hello"));
        Assert.Contains("DocumentDB", ex.Message);
        Assert.Contains("OpenSearch", ex.Message);
    }

    [Fact]
    public void Vector_IsDownFlagged()
    {
        using var store = (IDisposable)db.CreateStore($"t{Guid.NewGuid():N}");
        Assert.False(((IDocumentStore)store).SupportsVector);
    }

    [Fact]
    public async Task NearestVectors_Throws_DocumentDbSpecificMessage()
    {
        using var store = (IDisposable)db.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            () => s.NearestVectors<User>(new float[] { 0.1f, 0.2f }, 3));
        Assert.Contains("DocumentDB", ex.Message);
    }

    [Fact]
    public async Task Crud_And_Query_WorkThroughSubclass()
    {
        var store = db.CreateStore($"t{Guid.NewGuid():N}");
        using var disp = (IDisposable)store;

        await store.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });
        await store.Insert(new User { Id = "u2", Name = "Bob", Age = 40 });

        var alice = await store.Get<User>("u1");
        Assert.NotNull(alice);
        Assert.Equal("Alice", alice!.Name);

        var older = await store.Query<User>().Where(u => u.Age >= 35).ToList();
        Assert.Single(older);
        Assert.Equal("Bob", older[0].Name);

        Assert.Equal(2, await store.Count<User>());
    }

    [Fact]
    public async Task Cas_StaleVersion_Throws()
    {
        var store = db.CreateStoreWithVersion<VersionedUser>($"t{Guid.NewGuid():N}", u => u.Version);
        using var disp = (IDisposable)store;

        await store.Insert(new VersionedUser { Id = "u1", Name = "Alice", Age = 30 });

        var first = await store.Get<VersionedUser>("u1");
        var second = await store.Get<VersionedUser>("u1");

        first!.Age = 31;
        await store.Update(first);

        second!.Age = 32;
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.Update(second));
    }
}
