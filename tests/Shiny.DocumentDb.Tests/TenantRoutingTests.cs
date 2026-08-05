using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Diagnostics;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.LiteDb;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Tenant-per-database routing (<c>AddMultiTenantDocumentStore</c>): physical isolation, the bounded store
/// cache (size + idle eviction with deferred disposal), per-tenant initialization, session wiring, telemetry
/// identity, and <see cref="ITenantStoreManager"/>.
/// </summary>
public class TenantRoutingTests : IDisposable
{
    readonly string dir = Directory.CreateDirectory(
        Path.Combine(Path.GetTempPath(), $"tenants_{Guid.NewGuid():N}")).FullName;

    public void Dispose()
    {
        if (Directory.Exists(this.dir))
            Directory.Delete(this.dir, true);
    }

    sealed class MutableTenantResolver : ITenantResolver
    {
        public string Tenant { get; set; } = "";
        public string GetCurrentTenant() => this.Tenant;
    }

    sealed class TestClock : TimeProvider
    {
        DateTimeOffset now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => this.now;
        public void Advance(TimeSpan by) => this.now += by;
    }

    /// <summary>
    /// A store that does nothing but count disposal — separating the calls the DI scope makes (which must be
    /// no-ops while the cache owns the store) from the real close the cache performs.
    /// </summary>
    sealed class TrackingStore : DocumentProviderBase, IDocumentStore, IDisposable
    {
        int disposeCalls;
        int realDisposes;

        public int DisposeCalls => Volatile.Read(ref this.disposeCalls);
        public int RealDisposes => Volatile.Read(ref this.realDisposes);

        protected override InterceptorPipeline Interceptors => new();
        protected override DocumentMappingRegistry Mappings { get; } = new();
        protected override IdAccessorCache IdCache { get; } = new();
        protected override JsonTypeInfo<T>? ResolveTypeInfo<T>(JsonTypeInfo<T>? provided) where T : class => provided;
        protected override string ResolveDocumentTypeName<T>() where T : class => typeof(T).Name;

        public void Dispose()
        {
            Interlocked.Increment(ref this.disposeCalls);
            if (!this.DisposeSuppressed)
                Interlocked.Increment(ref this.realDisposes);
        }

        const string Unused = "The tracking store exists to count disposal, not to store documents.";
        public IDocumentQuery<T> Query<T>(JsonTypeInfo<T>? jsonTypeInfo = null) where T : class => throw new NotSupportedException(Unused);
        public Task Insert<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public Task<int> BatchInsert<T>(IEnumerable<T> documents, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public Task Update<T>(T document, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public Task Upsert<T>(T patch, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public Task<bool> SetProperty<T>(object id, Expression<Func<T, object>> property, object? value, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public Task<bool> RemoveProperty<T>(object id, Expression<Func<T, object>> property, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public Task<T?> Get<T>(object id, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public Task<IReadOnlyList<T>> Query<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public IAsyncEnumerable<T> QueryStream<T>(string whereClause, JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
        public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    }

    /// <summary>Collects finished spans tagged with one <c>db.namespace</c> — the process-wide ActivitySource
    /// is shared by the whole suite, so every telemetry test filters by a unique store name.</summary>
    sealed class SpanCollector : IDisposable
    {
        readonly ActivityListener listener;
        readonly List<Activity> activities = new();

        public IReadOnlyList<Activity> Activities { get { lock (this.activities) return this.activities.ToList(); } }

        public SpanCollector(string storeName)
        {
            this.listener = new ActivityListener
            {
                ShouldListenTo = src => src.Name == DocumentStoreMetrics.Name,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = a =>
                {
                    if (a.GetTagItem("db.namespace") as string == storeName)
                        lock (this.activities) this.activities.Add(a);
                }
            };
            ActivitySource.AddActivityListener(this.listener);
        }

        public void Dispose() => this.listener.Dispose();
    }

    string SqlitePath(string tenant) => Path.Combine(this.dir, $"{tenant}.db");

    ServiceCollection Relational(MutableTenantResolver resolver, Action<TenantStoreOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITenantResolver>(resolver);
        services.AddMultiTenantDocumentStore(
            tenantId => new DocumentStoreOptions
            {
                DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.SqlitePath(tenantId)}")
            },
            configure
        );
        return services;
    }

    static async Task<T> InScope<T>(IServiceProvider sp, Func<IServiceProvider, Task<T>> work)
    {
        using var scope = sp.CreateScope();
        return await work(scope.ServiceProvider);
    }

    // ── Physical isolation ──────────────────────────────────────────────

    [Fact]
    public async Task EachTenantGetsItsOwnDatabase()
    {
        var resolver = new MutableTenantResolver { Tenant = "alpha" };
        using var sp = this.Relational(resolver).BuildServiceProvider();

        await InScope(sp, async services =>
        {
            await services.GetRequiredService<IDocumentStore>().Insert(new User { Id = "u1", Name = "Alice", Age = 30 });
            return 0;
        });

        resolver.Tenant = "beta";
        Assert.Equal(0, await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Count<User>()));
        Assert.Null(await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Get<User>("u1")));

        // Back to alpha through a brand new scope: the cached store survived the first scope's disposal.
        resolver.Tenant = "alpha";
        Assert.Equal(1, await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Count<User>()));
    }

    [Fact]
    public async Task ScopeDisposal_DoesNotCloseTheSharedStore()
    {
        var resolver = new MutableTenantResolver { Tenant = "t" };
        var store = new TrackingStore();
        var services = new ServiceCollection();
        services.AddSingleton<ITenantResolver>(resolver);
        services.AddMultiTenantDocumentStore(_ => store);

        using var sp = services.BuildServiceProvider();
        for (var i = 0; i < 3; i++)
        {
            using var scope = sp.CreateScope();
            Assert.Same(store, scope.ServiceProvider.GetRequiredService<IDocumentStore>());
        }

        Assert.Equal(3, store.DisposeCalls);      // the container did try, once per scope…
        Assert.Equal(0, store.RealDisposes);      // …and the cache's ownership made each one a no-op
        await Task.CompletedTask;
    }

    // ── Cache behavior ──────────────────────────────────────────────────

    [Fact]
    public async Task ColdStartRace_BuildsExactlyOneStore()
    {
        var built = 0;
        var resolver = new MutableTenantResolver { Tenant = "cold" };
        var services = new ServiceCollection();
        services.AddSingleton<ITenantResolver>(resolver);
        services.AddMultiTenantDocumentStore(_ =>
        {
            Interlocked.Increment(ref built);
            return new TrackingStore();
        });

        using var sp = services.BuildServiceProvider();
        var resolved = await Task.WhenAll(Enumerable.Range(0, 50).Select(_ => Task.Run(() =>
        {
            using var scope = sp.CreateScope();
            return scope.ServiceProvider.GetRequiredService<IDocumentStore>();
        })));

        Assert.Equal(1, built);
        Assert.All(resolved, s => Assert.Same(resolved[0], s));
        Assert.Equal(0, ((TrackingStore)resolved[0]).RealDisposes);   // no loser was built, so none was leaked
    }

    [Fact]
    public void ExceedingMaxCachedStores_EvictsAndDisposesTheLeastRecentlyUsed()
    {
        var clock = new TestClock();
        var stores = new ConcurrentDictionary<string, TrackingStore>();
        var resolver = new MutableTenantResolver();
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services.AddSingleton<ITenantResolver>(resolver);
        services.AddMultiTenantDocumentStore(
            tenantId => stores[tenantId] = new TrackingStore(),
            o =>
            {
                o.MaxCachedStores = 2;
                o.IdleTimeout = null;
            }
        );

        using var sp = services.BuildServiceProvider();
        foreach (var tenant in new[] { "a", "b", "c" })
        {
            resolver.Tenant = tenant;
            clock.Advance(TimeSpan.FromMinutes(1));
            using var scope = sp.CreateScope();
            scope.ServiceProvider.GetRequiredService<IDocumentStore>();
        }

        Assert.Equal(1, stores["a"].RealDisposes);   // LRU, closed exactly once
        Assert.Equal(0, stores["b"].RealDisposes);
        Assert.Equal(0, stores["c"].RealDisposes);
        Assert.Equal(new[] { "b", "c" }, sp.GetRequiredService<ITenantStoreManager>().ActiveTenants.Order());
    }

    [Fact]
    public async Task Eviction_WaitsForAnInFlightLease()
    {
        var store = new TrackingStore();
        var resolver = new MutableTenantResolver { Tenant = "busy" };
        var services = new ServiceCollection();
        services.AddSingleton<ITenantResolver>(resolver);
        services.AddMultiTenantDocumentStore(_ => store);

        using var sp = services.BuildServiceProvider();
        var scope = sp.CreateScope();
        Assert.Same(store, scope.ServiceProvider.GetRequiredService<IDocumentStore>());

        var eviction = sp.GetRequiredService<ITenantStoreManager>().EvictAsync("busy");
        await Task.Delay(100);
        Assert.False(eviction.IsCompleted);          // the request still holds it
        Assert.Equal(0, store.RealDisposes);

        scope.Dispose();                             // last lease returned
        await eviction.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, store.RealDisposes);
    }

    [Fact]
    public void IdleTimeout_EvictsOnSweep()
    {
        var clock = new TestClock();
        var store = new TrackingStore();
        var resolver = new MutableTenantResolver { Tenant = "idle" };
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services.AddSingleton<ITenantResolver>(resolver);
        services.AddMultiTenantDocumentStore(_ => store, o => o.IdleTimeout = TimeSpan.FromMinutes(20));

        using var sp = services.BuildServiceProvider();
        using (var scope = sp.CreateScope())
            scope.ServiceProvider.GetRequiredService<IDocumentStore>();

        var cache = sp.GetRequiredService<TenantDocumentStoreCache>();

        clock.Advance(TimeSpan.FromMinutes(19));
        cache.SweepIdle();
        Assert.Equal(0, store.RealDisposes);
        Assert.Single(sp.GetRequiredService<ITenantStoreManager>().ActiveTenants);

        clock.Advance(TimeSpan.FromMinutes(2));
        cache.SweepIdle();
        Assert.Equal(1, store.RealDisposes);
        Assert.Empty(sp.GetRequiredService<ITenantStoreManager>().ActiveTenants);
    }

    [Fact]
    public void ContainerDisposal_ClosesEveryTenantStore()
    {
        var stores = new ConcurrentDictionary<string, TrackingStore>();
        var resolver = new MutableTenantResolver();
        var services = new ServiceCollection();
        services.AddSingleton<ITenantResolver>(resolver);
        services.AddMultiTenantDocumentStore(tenantId => stores[tenantId] = new TrackingStore());

        var sp = services.BuildServiceProvider();
        foreach (var tenant in new[] { "x", "y" })
        {
            resolver.Tenant = tenant;
            using var scope = sp.CreateScope();
            scope.ServiceProvider.GetRequiredService<IDocumentStore>();
        }
        sp.Dispose();

        Assert.Equal(1, stores["x"].RealDisposes);
        Assert.Equal(1, stores["y"].RealDisposes);
    }

    // ── Per-tenant initialization ───────────────────────────────────────

    [Fact]
    public async Task OnTenantStoreCreated_RunsOncePerTenant_BeforeTheFirstOperation()
    {
        var initialized = new ConcurrentBag<string>();
        var resolver = new MutableTenantResolver { Tenant = "init1" };
        using var sp = this.Relational(resolver, o => o.OnTenantStoreCreated = async ctx =>
        {
            initialized.Add(ctx.TenantId);
            await ctx.Store.Insert(new User { Id = "seed", Name = ctx.TenantId, Age = 1 }, cancellationToken: ctx.CancellationToken);
        }).BuildServiceProvider();

        var seeded = await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Get<User>("seed"));
        Assert.NotNull(seeded);
        Assert.Equal("init1", seeded.Name);

        await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Count<User>());   // second resolution
        resolver.Tenant = "init2";
        await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Count<User>());

        Assert.Equal(new[] { "init1", "init2" }, initialized.Order());
    }

    [Fact]
    public async Task AThrowingInitHook_FailsTheResolutionAndCachesNothing()
    {
        var attempts = 0;
        var resolver = new MutableTenantResolver { Tenant = "flaky" };
        using var sp = this.Relational(resolver, o => o.OnTenantStoreCreated = _ =>
        {
            attempts++;
            return attempts == 1 ? Task.FromException(new InvalidOperationException("boom")) : Task.CompletedTask;
        }).BuildServiceProvider();

        using (var scope = sp.CreateScope())
        {
            Assert.Throws<InvalidOperationException>(() => scope.ServiceProvider.GetRequiredService<IDocumentStore>());
        }
        Assert.Empty(sp.GetRequiredService<ITenantStoreManager>().ActiveTenants);

        // Nothing broken was cached — the next resolution retries and succeeds.
        Assert.Equal(0, await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Count<User>()));
        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task SeedFromRegisteredSeeders_SeedsEachTenant()
    {
        var resolver = new MutableTenantResolver { Tenant = "seed1" };
        var services = this.Relational(resolver, o => o.SeedFromRegisteredSeeders());
        services.AddDocumentSeeder("users", 1, (store, ct) => store.Insert(new User { Id = "u1", Name = "Seeded", Age = 7 }, cancellationToken: ct));

        using var sp = services.BuildServiceProvider();
        Assert.Equal("Seeded", (await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Get<User>("u1")))!.Name);

        resolver.Tenant = "seed2";
        Assert.Equal("Seeded", (await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Get<User>("u1")))!.Name);
    }

    // ── Session wiring ──────────────────────────────────────────────────

    [Fact]
    public async Task DocumentSession_WritesToTheResolvedTenant()
    {
        var resolver = new MutableTenantResolver { Tenant = "sess1" };
        using var sp = this.Relational(resolver).BuildServiceProvider();

        using (var scope = sp.CreateScope())
        {
            var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
            session.Add(new User { Id = "u1", Name = "Alice", Age = 30 });
            await session.SaveChanges();
        }

        resolver.Tenant = "sess2";
        using (var scope = sp.CreateScope())
        {
            var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
            Assert.Equal(0, await session.Count<User>());
        }

        resolver.Tenant = "sess1";
        using (var scope = sp.CreateScope())
        {
            var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
            Assert.Equal(1, await session.Count<User>());
        }
    }

    [Fact]
    public async Task DocumentSessionFactory_OpensAgainstTheResolvedTenant()
    {
        var resolver = new MutableTenantResolver { Tenant = "fact1" };
        using var sp = this.Relational(resolver).BuildServiceProvider();

        await using (var session = sp.GetRequiredService<IDocumentSessionFactory>().OpenSession())
        {
            session.Add(new User { Id = "u1", Name = "Alice", Age = 30 });
            await session.SaveChanges();
        }

        resolver.Tenant = "fact2";
        await using (var session = sp.GetRequiredService<IDocumentSessionFactory>().OpenSession())
            Assert.Equal(0, await session.Count<User>());
    }

    // ── Telemetry identity ──────────────────────────────────────────────

    [Fact]
    public async Task TenantStoreTagsSpansWithItsOwnNamespace()
    {
        var tenant = $"t{Guid.NewGuid():N}";
        var resolver = new MutableTenantResolver { Tenant = tenant };
        using var sp = this.Relational(resolver).BuildServiceProvider();

        using var telemetry = new SpanCollector(tenant);
        await InScope(sp, async s =>
        {
            await s.GetRequiredService<IDocumentStore>().Insert(new User { Id = "u1", Name = "Alice", Age = 30 });
            return 0;
        });

        Assert.Contains("sqlite.insert", telemetry.Activities.Select(a => a.OperationName));
    }

    [Fact]
    public async Task StoreNameFactory_OverridesTheTelemetryNamespace()
    {
        var bucket = $"b{Guid.NewGuid():N}";
        var resolver = new MutableTenantResolver { Tenant = $"t{Guid.NewGuid():N}" };
        using var sp = this.Relational(resolver, o => o.StoreNameFactory = _ => bucket).BuildServiceProvider();

        using var telemetry = new SpanCollector(bucket);
        await InScope(sp, async s =>
        {
            await s.GetRequiredService<IDocumentStore>().Insert(new User { Id = "u1", Name = "Alice", Age = 30 });
            return 0;
        });

        Assert.Contains("sqlite.insert", telemetry.Activities.Select(a => a.OperationName));
    }

    // ── Non-relational routing ──────────────────────────────────────────

    [Fact]
    public async Task RoutesNonRelationalProviders()
    {
        var resolver = new MutableTenantResolver { Tenant = "lite1" };
        var services = new ServiceCollection();
        services.AddSingleton<ITenantResolver>(resolver);
        services.AddMultiTenantDocumentStore(tenantId => new LiteDbDocumentStore(new LiteDbDocumentStoreOptions
        {
            ConnectionString = Path.Combine(this.dir, $"{tenantId}.litedb")
        }));

        using var sp = services.BuildServiceProvider();
        await InScope(sp, async s =>
        {
            await s.GetRequiredService<IDocumentStore>().Insert(new User { Id = "u1", Name = "Alice", Age = 30 });
            return 0;
        });

        resolver.Tenant = "lite2";
        Assert.Equal(0, await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Count<User>()));

        resolver.Tenant = "lite1";
        Assert.Equal(1, await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Count<User>()));
    }

    // ── ITenantStoreManager ─────────────────────────────────────────────

    [Fact]
    public async Task Manager_WarmsEvictsAndLists()
    {
        var initialized = new ConcurrentBag<string>();
        var resolver = new MutableTenantResolver { Tenant = "warm" };
        using var sp = this.Relational(resolver, o => o.OnTenantStoreCreated = ctx =>
        {
            initialized.Add(ctx.TenantId);
            return Task.CompletedTask;
        }).BuildServiceProvider();

        var manager = sp.GetRequiredService<ITenantStoreManager>();
        Assert.Empty(manager.ActiveTenants);

        await manager.WarmAsync("warm");
        Assert.Equal(new[] { "warm" }, manager.ActiveTenants);
        Assert.Equal(new[] { "warm" }, initialized);

        await manager.WarmAsync("warm");                       // idempotent — no second build
        Assert.Single(initialized);

        await manager.EvictAsync("warm");
        Assert.Empty(manager.ActiveTenants);
        await manager.EvictAsync("warm");                      // idempotent — unknown tenant is a no-op
        await manager.EvictAsync("never-seen");

        // A later resolution simply builds it again.
        Assert.Equal(0, await InScope(sp, s => s.GetRequiredService<IDocumentStore>().Count<User>()));
        Assert.Equal(2, initialized.Count);
    }

    [Fact]
    public void InvalidOptionsAreRejectedAtRegistration()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentOutOfRangeException>(
            () => services.AddMultiTenantDocumentStore(_ => new TrackingStore(), o => o.MaxCachedStores = 0));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => services.AddMultiTenantDocumentStore(_ => new TrackingStore(), o => o.IdleTimeout = TimeSpan.Zero));
    }
}
