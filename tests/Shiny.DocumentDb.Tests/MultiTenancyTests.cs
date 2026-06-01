using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Exercises shared-table multi-tenancy: <c>TenantIdAccessor</c> tags inserts with the current
/// tenant, queries filter to the current tenant, and cross-tenant isolation holds for Get,
/// Update, Remove, and Count.
/// </summary>
public abstract class MultiTenancyTestsBase
{
    protected readonly ITenantDocumentStoreFixture Fixture;

    protected MultiTenancyTestsBase(ITenantDocumentStoreFixture fixture)
        => this.Fixture = fixture;

    [Fact]
    public async Task Insert_And_Query_RespectCurrentTenant()
    {
        var currentTenant = "tenantA";
        using var store = (IDisposable)this.Fixture.CreateStoreWithTenant(
            $"t{Guid.NewGuid():N}", () => currentTenant);
        var s = (IDocumentStore)store;

        await s.Insert(new User { Id = "u1", Name = "A1", Age = 1 });
        currentTenant = "tenantB";
        await s.Insert(new User { Id = "u2", Name = "B1", Age = 2 });

        currentTenant = "tenantA";
        var a = await s.Query<User>().ToList();
        Assert.Single(a);
        Assert.Equal("A1", a[0].Name);

        currentTenant = "tenantB";
        var b = await s.Query<User>().ToList();
        Assert.Single(b);
        Assert.Equal("B1", b[0].Name);
    }

    [Fact]
    public async Task Get_ReturnsNull_AcrossTenants()
    {
        var currentTenant = "tenantA";
        using var store = (IDisposable)this.Fixture.CreateStoreWithTenant(
            $"t{Guid.NewGuid():N}", () => currentTenant);
        var s = (IDocumentStore)store;

        await s.Insert(new User { Id = "u1", Name = "A1", Age = 1 });

        currentTenant = "tenantB";
        var got = await s.Get<User>("u1");
        Assert.Null(got);

        currentTenant = "tenantA";
        var ownerGot = await s.Get<User>("u1");
        Assert.NotNull(ownerGot);
        Assert.Equal("A1", ownerGot!.Name);
    }

    [Fact]
    public async Task Update_DoesNotCrossTenants()
    {
        var currentTenant = "tenantA";
        using var store = (IDisposable)this.Fixture.CreateStoreWithTenant(
            $"t{Guid.NewGuid():N}", () => currentTenant);
        var s = (IDocumentStore)store;

        await s.Insert(new User { Id = "u1", Name = "A1", Age = 1 });

        currentTenant = "tenantB";
        // Update from a different tenant should fail because tenant scoping prevents the WHERE from matching.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await s.Update(new User { Id = "u1", Name = "TamperedFromB", Age = 99 }));

        currentTenant = "tenantA";
        var got = await s.Get<User>("u1");
        Assert.Equal("A1", got!.Name);
    }

    [Fact]
    public async Task Remove_DoesNotCrossTenants()
    {
        var currentTenant = "tenantA";
        using var store = (IDisposable)this.Fixture.CreateStoreWithTenant(
            $"t{Guid.NewGuid():N}", () => currentTenant);
        var s = (IDocumentStore)store;

        await s.Insert(new User { Id = "u1", Name = "A1", Age = 1 });

        currentTenant = "tenantB";
        var removed = await s.Remove<User>("u1");
        Assert.False(removed);

        currentTenant = "tenantA";
        var got = await s.Get<User>("u1");
        Assert.NotNull(got);
    }

    [Fact]
    public async Task Count_IsPerTenant()
    {
        var currentTenant = "tenantA";
        using var store = (IDisposable)this.Fixture.CreateStoreWithTenant(
            $"t{Guid.NewGuid():N}", () => currentTenant);
        var s = (IDocumentStore)store;

        await s.Insert(new User { Id = "a1", Name = "x", Age = 1 });
        await s.Insert(new User { Id = "a2", Name = "y", Age = 2 });

        currentTenant = "tenantB";
        await s.Insert(new User { Id = "b1", Name = "z", Age = 3 });

        Assert.Equal(1, await s.Count<User>());

        currentTenant = "tenantA";
        Assert.Equal(2, await s.Count<User>());
    }

    [Fact]
    public async Task Clear_OnlyAffectsCurrentTenant()
    {
        var currentTenant = "tenantA";
        using var store = (IDisposable)this.Fixture.CreateStoreWithTenant(
            $"t{Guid.NewGuid():N}", () => currentTenant);
        var s = (IDocumentStore)store;

        await s.Insert(new User { Id = "a1", Name = "x", Age = 1 });
        await s.Insert(new User { Id = "a2", Name = "y", Age = 2 });

        currentTenant = "tenantB";
        await s.Insert(new User { Id = "b1", Name = "z", Age = 3 });
        var clearedB = await s.Clear<User>();
        Assert.Equal(1, clearedB);

        currentTenant = "tenantA";
        Assert.Equal(2, await s.Count<User>());
    }
}
