using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Unique indexes (<c>MapUniqueIndex</c>), asserted on every provider. The relational providers and MongoDB enforce
/// them with a native index and the rest keep their own index entries in step with each write, so this is where a
/// provider that misses a write path — an update that forgets to release the old value, a set-based update that
/// bypasses the check — gets caught.
/// </summary>
public abstract class UniqueIndexConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    public class UqUser
    {
        public string Id { get; set; } = "";
        public string? Email { get; set; }
        public string? Region { get; set; }
        public string Name { get; set; } = "";
        public bool IsDeleted { get; set; }
        public UqAddress? Address { get; set; }
    }

    public class UqAddress
    {
        public string? City { get; set; }
    }

    public class UqAdmin
    {
        public string Id { get; set; } = "";
        public string? Email { get; set; }
    }

    IDocumentStore CreateStore(Action<IDocumentStoreOptions> configure)
        => this.Fixture.CreateStore($"t{Guid.NewGuid():N}", configure);

    IDocumentStore EmailStore()
        => this.CreateStore(o => o.ConfigureDocument<UqUser>(cfg => cfg.MapUniqueIndex(x => x.Email)));

    static UqUser User(string id, string? email, string? region = null) => new() { Id = id, Email = email, Region = region, Name = id };

    [Fact]
    public async Task Insert_WithTakenValue_Throws_AndWritesNothing()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        var ex = await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(User("u2", "a@x.com")));

        Assert.Equal(typeof(UqUser), ex.DocumentType);
        Assert.Equal(["Email"], ex.PropertyNames);
        Assert.StartsWith("uq_", ex.IndexName);
        Assert.EndsWith("_Email", ex.IndexName);
        Assert.Null(await store.Get<UqUser>("u2"));
        Assert.Equal(1, await store.Query<UqUser>().Count());
    }

    [Fact]
    public async Task Insert_DistinctValues_Succeed()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await store.Insert(User("u2", "b@x.com"));

        Assert.Equal(2, await store.Query<UqUser>().Count());
    }

    [Fact]
    public async Task Values_CompareCaseSensitively()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await store.Insert(User("u2", "A@X.COM"));

        Assert.Equal(2, await store.Query<UqUser>().Count());
    }

    [Fact]
    public async Task NullValues_AreNotConstrained()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", null));
        await store.Insert(User("u2", null));
        await store.Insert(User("u3", null));

        Assert.Equal(3, await store.Query<UqUser>().Count());
    }

    [Fact]
    public async Task FailedInsert_DoesNotHoldTheValue()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(User("u2", "a@x.com")));
        await store.Remove<UqUser>("u1");

        await store.Insert(User("u3", "a@x.com"));
        Assert.Equal("a@x.com", (await store.Get<UqUser>("u3"))!.Email);
    }

    [Fact]
    public async Task Update_ToAnotherDocumentsValue_Throws_AndKeepsTheOriginal()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await store.Insert(User("u2", "b@x.com"));

        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Update(User("u2", "a@x.com")));

        Assert.Equal("b@x.com", (await store.Get<UqUser>("u2"))!.Email);
        // u2 still holds its own value.
        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(User("u3", "b@x.com")));
    }

    [Fact]
    public async Task Update_KeepingItsOwnValue_Succeeds()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        var user = User("u1", "a@x.com");
        user.Name = "renamed";
        await store.Update(user);

        Assert.Equal("renamed", (await store.Get<UqUser>("u1"))!.Name);
    }

    [Fact]
    public async Task Update_ReleasesThePreviousValue()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await store.Update(User("u1", "b@x.com"));

        await store.Insert(User("u2", "a@x.com"));
        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(User("u3", "b@x.com")));
    }

    [Fact]
    public async Task Remove_ReleasesTheValue()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        Assert.True(await store.Remove<UqUser>("u1"));

        await store.Insert(User("u2", "a@x.com"));
        Assert.NotNull(await store.Get<UqUser>("u2"));
    }

    [Fact]
    public async Task Clear_ReleasesEveryValue()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await store.Insert(User("u2", "b@x.com"));
        await store.Clear<UqUser>();

        await store.Insert(User("u3", "a@x.com"));
        await store.Insert(User("u4", "b@x.com"));
        Assert.Equal(2, await store.Query<UqUser>().Count());
    }

    [Fact]
    public async Task Upsert_NewDocumentWithTakenValue_Throws()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));

        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Upsert(User("u2", "a@x.com")));
        Assert.Null(await store.Get<UqUser>("u2"));
    }

    [Fact]
    public async Task Upsert_ExistingDocumentToTakenValue_Throws()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await store.Insert(User("u2", "b@x.com"));

        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Upsert(User("u2", "a@x.com")));
        Assert.Equal("b@x.com", (await store.Get<UqUser>("u2"))!.Email);
    }

    [Fact]
    public async Task SetProperty_ToTakenValue_Throws()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await store.Insert(User("u2", "b@x.com"));

        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.SetProperty<UqUser>("u2", x => x.Email!, "a@x.com"));
        Assert.Equal("b@x.com", (await store.Get<UqUser>("u2"))!.Email);
    }

    [Fact]
    public async Task ExecuteUpdate_ToTakenValue_Throws()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await store.Insert(User("u2", "b@x.com"));

        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Query<UqUser>().Where(x => x.Id == "u2").ExecuteUpdate(x => x.Email!, "a@x.com"));
        Assert.Equal("b@x.com", (await store.Get<UqUser>("u2"))!.Email);
    }

    [Fact]
    public async Task BatchInsert_WithDuplicateInsideTheBatch_Throws()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.BatchInsert([User("u1", "a@x.com"), User("u2", "a@x.com")]));
    }

    [Fact]
    public async Task BatchInsert_WithValueTakenByAStoredDocument_Throws()
    {
        var store = this.EmailStore();
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.BatchInsert([User("u2", "b@x.com"), User("u3", "a@x.com")]));
    }

    [Fact]
    public async Task Composite_OnlyTheWholeKeyIsUnique()
    {
        var store = this.CreateStore(o => o.ConfigureDocument<UqUser>(cfg => cfg.MapUniqueIndex(x => new { x.Region, x.Email })));
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com", "eu"));
        await store.Insert(User("u2", "a@x.com", "us"));
        await store.Insert(User("u3", "b@x.com", "eu"));

        var ex = await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(User("u4", "a@x.com", "eu")));
        Assert.Equal(["Region", "Email"], ex.PropertyNames);

        // A key with a null part is not constrained.
        await store.Insert(User("u5", "a@x.com"));
        await store.Insert(User("u6", "a@x.com"));
        Assert.Equal(5, await store.Query<UqUser>().Count());
    }

    [Fact]
    public async Task NestedProperty_IsUnique()
    {
        var store = this.CreateStore(o => o.ConfigureDocument<UqUser>(cfg => cfg.MapUniqueIndex(x => x.Address!.City)));
        using var _ = (IDisposable)store;

        await store.Insert(new UqUser { Id = "u1", Address = new UqAddress { City = "Toronto" } });
        await store.Insert(new UqUser { Id = "u2", Address = new UqAddress { City = "Calgary" } });
        await store.Insert(new UqUser { Id = "u3" });
        await store.Insert(new UqUser { Id = "u4" });

        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(new UqUser { Id = "u5", Address = new UqAddress { City = "Toronto" } }));
    }

    [Fact]
    public async Task PropertyBuilderUnique_IsTheSameIndex()
    {
        var store = this.CreateStore(o => o.ConfigureDocument<UqUser>(cfg => cfg.MapProperty(x => x.Email, p => p.Unique())));
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        var ex = await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(User("u2", "a@x.com")));
        Assert.EndsWith("_Email", ex.IndexName);
    }

    [Fact]
    public async Task Filter_DocumentsItRejectsAreNotConstrained()
    {
        var store = this.CreateStore(o => o.ConfigureDocument<UqUser>(cfg => cfg.MapUniqueIndex(x => x.Email, filter: x => !x.IsDeleted)));
        using var _ = (IDisposable)store;

        await store.Insert(new UqUser { Id = "u1", Email = "a@x.com", IsDeleted = true });
        await store.Insert(new UqUser { Id = "u2", Email = "a@x.com" });
        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(new UqUser { Id = "u3", Email = "a@x.com" }));

        // Bringing the excluded document back into the filter makes it collide.
        await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Update(new UqUser { Id = "u1", Email = "a@x.com", IsDeleted = false }));
        Assert.True((await store.Get<UqUser>("u1"))!.IsDeleted);

        // Leaving the filter releases the value.
        await store.Update(new UqUser { Id = "u2", Email = "a@x.com", IsDeleted = true });
        await store.Insert(new UqUser { Id = "u4", Email = "a@x.com" });
    }

    [Fact]
    public async Task Filter_WithSoftDelete_ReleasesTheValueOfARemovedDocument()
    {
        var store = this.CreateStore(o => o.ConfigureDocument<UqUser>(cfg =>
        {
            cfg.AddSoftDelete(x => x.IsDeleted);
            cfg.MapUniqueIndex(x => x.Email, filter: x => !x.IsDeleted);
        }));
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        Assert.True(await store.Remove<UqUser>("u1"));

        await store.Insert(User("u2", "a@x.com"));
        Assert.Equal(2, await store.Query<UqUser>().IncludeDeleted().Count());
    }

    [Fact]
    public async Task SharedTable_TheIndexIsScopedToItsType()
    {
        var store = this.CreateStore(o =>
        {
            o.ConfigureDocument<UqUser>(cfg => cfg.MapUniqueIndex(x => x.Email));
            o.ConfigureDocument<UqAdmin>(cfg => cfg.MapUniqueIndex(x => x.Email));
        });
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await store.Insert(new UqAdmin { Id = "a1", Email = "a@x.com" });

        var ex = await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(new UqAdmin { Id = "a2", Email = "a@x.com" }));
        Assert.Equal(typeof(UqAdmin), ex.DocumentType);
    }

    [Fact]
    public async Task SharedTable_TypesWithoutAnIndexAreUnconstrained()
    {
        var store = this.CreateStore(o => o.ConfigureDocument<UqUser>(cfg => cfg.MapUniqueIndex(x => x.Email)));
        using var _ = (IDisposable)store;

        await store.Insert(User("u1", "a@x.com"));
        await store.Insert(new UqAdmin { Id = "a1", Email = "a@x.com" });
        await store.Insert(new UqAdmin { Id = "a2", Email = "a@x.com" });

        Assert.Equal(2, await store.Query<UqAdmin>().Count());
    }

    [Fact]
    public async Task SeveralIndexes_AreEnforcedIndependently()
    {
        var store = this.CreateStore(o => o.ConfigureDocument<UqUser>(cfg =>
        {
            cfg.MapUniqueIndex(x => x.Email);
            cfg.MapUniqueIndex(x => x.Name);
        }));
        using var _ = (IDisposable)store;

        await store.Insert(new UqUser { Id = "u1", Email = "a@x.com", Name = "alice" });

        var byName = await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(new UqUser { Id = "u2", Email = "b@x.com", Name = "alice" }));
        Assert.Equal(["Name"], byName.PropertyNames);

        var byEmail = await Assert.ThrowsAsync<UniqueConstraintException>(() => store.Insert(new UqUser { Id = "u3", Email = "a@x.com", Name = "carol" }));
        Assert.Equal(["Email"], byEmail.PropertyNames);

        // Neither failed write left a value behind.
        await store.Insert(new UqUser { Id = "u4", Email = "b@x.com", Name = "carol" });
    }
}
