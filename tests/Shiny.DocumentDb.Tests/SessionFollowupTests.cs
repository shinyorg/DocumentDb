using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Session-architecture follow-ups: A1 scope-aware tenancy, A2 scope-aware temporal actor, L1 consistent-read
// sessions, T1 unit-of-work span, T3 unit-of-work size metric.
public class SessionFollowupTests
{
    sealed class ScopedTenant : ITenantResolver
    {
        public string Tenant { get; set; } = "";
        public string GetCurrentTenant() => this.Tenant;
    }

    interface ICurrentUser { string Id { get; } }
    sealed class CurrentUser : ICurrentUser { public string Id { get; set; } = ""; }

    // ── A1: tenant resolves from the SESSION's scope (scoped ITenantResolver) ───────────────────────
    [Fact]
    public async Task A1_ScopedTenant_IsolatesPerSessionScope()
    {
        var services = new ServiceCollection();
        services.AddScoped<ITenantResolver, ScopedTenant>();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        }, multiTenant: true).AddScopedDocumentSession();
        await using var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<IDocumentStore>();

        await WriteAs(sp, "tenantA", "a1");
        await WriteAs(sp, "tenantB", "b1");

        // Each scope's session sees only its own tenant's document (tenant resolved from the scope).
        await using (var scope = sp.CreateAsyncScope())
        {
            ((ScopedTenant)scope.ServiceProvider.GetRequiredService<ITenantResolver>()).Tenant = "tenantA";
            var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
            Assert.Equal(1, await session.Count<User>());
            Assert.NotNull(await session.Get<User>("a1"));
            Assert.Null(await session.Get<User>("b1"));
        }

        static async Task WriteAs(IServiceProvider sp, string tenant, string id)
        {
            await using var scope = sp.CreateAsyncScope();
            ((ScopedTenant)scope.ServiceProvider.GetRequiredService<ITenantResolver>()).Tenant = tenant;
            var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
            session.Add(new User { Id = id, Name = id });
            await session.SaveChanges();
        }
    }

    // ── A2: temporal actor resolves from the SESSION's scope (scoped ICurrentUser) ──────────────────
    [Fact]
    public async Task A2_ScopedTemporalActor_ResolvesFromSessionScope()
    {
        var services = new ServiceCollection();
        services.AddScoped<ICurrentUser>(_ => new CurrentUser { Id = "alice@example.com" });
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
            o.MapTemporal<VersionedUser>(t => t.ResolveActor = s => s.GetService<ICurrentUser>()?.Id);
        }).AddScopedDocumentSession();
        await using var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<IDocumentStore>();

        await using (var scope = sp.CreateAsyncScope())
        {
            var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
            session.Add(new VersionedUser { Id = "u1", Name = "A", Age = 1 });
            await session.SaveChanges();
        }

        var history = await ((ITemporalDocumentStore)store).History<VersionedUser>("u1");
        Assert.Equal("alice@example.com", history[0].Actor);
    }

    // ── L1: consistent-read session at an explicit isolation level ──────────────────────────────────
    [Fact]
    public async Task L1_BeginTransaction_WithIsolationLevel_Works()
    {
        using var store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        });
        await using var session = ((IDocumentStore)store).OpenSession();
        await using var tx = await session.BeginTransaction(System.Data.IsolationLevel.Serializable);
        session.Add(new User { Id = "u1", Name = "A" });
        await session.SaveChanges();
        var read = await session.Get<User>("u1");   // consistent read within the tx
        Assert.NotNull(read);
        await tx.Commit();
        Assert.Equal(1, await ((IDocumentStore)store).Count<User>());
    }

    // ── T1: a unit-of-work parent span with child ops nested + db.session.id ────────────────────────
    [Fact]
    public async Task T1_UnitOfWork_Span_ParentsChildOps()
    {
        // Process-global ActivitySource: other tests run in parallel, so guard the list (the callback fires on
        // arbitrary threads) and assert on "some unit span has a nested child" rather than one arbitrary unit.
        var activities = new List<Activity>();
        void Capture(Activity a) { lock (activities) activities.Add(a); }
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == "Shiny.DocumentDb",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = Capture
        };
        ActivitySource.AddActivityListener(listener);

        using var store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        });
        await using (var session = ((IDocumentStore)store).OpenSession())
        {
            session.Add(new User { Id = "u1", Name = "A" });
            await session.SaveChanges();
            await session.Get<User>("u1");
        }

        Activity[] snapshot;
        lock (activities) snapshot = activities.ToArray();
        var units = snapshot.Where(a => a.OperationName.EndsWith("unit_of_work", StringComparison.Ordinal)).ToArray();
        Assert.NotEmpty(units);
        Assert.All(units, u => Assert.NotNull(u.GetTagItem("db.session.id")));        // every unit span is tagged
        Assert.Contains(units, u => snapshot.Any(a => a.ParentSpanId == u.SpanId));   // our unit has a nested child
    }

    // ── T3: SaveChanges records the unit-of-work size ───────────────────────────────────────────────
    [Fact]
    public async Task T3_SaveChanges_RecordsUnitSize()
    {
        var sizes = new List<long>();
        using var meterListener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (inst.Meter.Name == "Shiny.DocumentDb" && inst.Name == "db.client.unit_of_work.operations")
                    l.EnableMeasurementEvents(inst);
            }
        };
        meterListener.SetMeasurementEventCallback<long>((_, v, _, _) => { lock (sizes) sizes.Add(v); });
        meterListener.Start();

        using var store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        });
        await using var session = ((IDocumentStore)store).OpenSession();
        session.Add(new User { Id = "u1", Name = "A" }).Add(new User { Id = "u2", Name = "B" });
        await session.SaveChanges();

        Assert.Contains(2L, sizes);   // a unit of 2 buffered ops was recorded
    }
}
