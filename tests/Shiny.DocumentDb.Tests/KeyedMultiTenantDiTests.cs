using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Exercises the keyed/named shared-table multi-tenant registration overload
/// (<c>AddDocumentStore(services, name, configure, multiTenant: true)</c>): the keyed store wires
/// <c>TenantIdAccessor</c> from a registered <see cref="ITenantResolver"/> and isolates documents per tenant.
/// </summary>
public class KeyedMultiTenantDiTests
{
    sealed class MutableTenantResolver : ITenantResolver
    {
        public string Tenant { get; set; } = "";
        public string GetCurrentTenant() => this.Tenant;
    }

    [Fact]
    public async Task Keyed_MultiTenant_IsolatesDocumentsByTenant()
    {
        var resolver = new MutableTenantResolver { Tenant = "A" };
        var table = $"t{Guid.NewGuid():N}";

        var services = new ServiceCollection();
        services.AddSingleton<ITenantResolver>(resolver);
        services.AddDocumentStore("orders", o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = table;
        }, multiTenant: true);

        using var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredKeyedService<IDocumentStore>("orders");

        await store.Insert(new User { Id = "u1", Name = "A1", Age = 1 });

        resolver.Tenant = "B";
        Assert.Equal(0, await store.Count<User>());      // a different tenant sees nothing
        Assert.Null(await store.Get<User>("u1"));

        resolver.Tenant = "A";
        Assert.Equal(1, await store.Count<User>());      // the owning tenant sees its document
    }

    [Fact]
    public async Task Keyed_MultiTenantFalse_BehavesLikePlainKeyedStore()
    {
        var table = $"t{Guid.NewGuid():N}";

        var services = new ServiceCollection();
        services.AddDocumentStore("orders", o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = table;
        }, multiTenant: false);   // no ITenantResolver registered — must not be required

        using var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredKeyedService<IDocumentStore>("orders");

        await store.Insert(new User { Id = "u1", Name = "A1", Age = 1 });
        Assert.Equal(1, await store.Count<User>());
    }
}
