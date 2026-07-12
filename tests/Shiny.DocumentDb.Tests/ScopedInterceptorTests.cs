using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Phase 1 of the DI-scoped-interceptors feature: ctx.Services (scoped DI resolution) and ctx.Store
// (transaction-visible, deadlock-free session store). Shared-mode in-memory SQLite exercises the
// RequiresSingleConnection path where a naive top-level-store call from a hook would deadlock.
public class ScopedInterceptorTests
{
    public sealed class OutboxEntry
    {
        public string Id { get; set; } = "";
        public string Event { get; set; } = "";
    }

    sealed class Probe : IDisposable
    {
        public Guid Id { get; } = Guid.NewGuid();
        public bool Disposed { get; private set; }
        public void Dispose() => this.Disposed = true;
    }

    sealed class ScopeCapture
    {
        public readonly List<Guid> ProbeIds = new();
        public readonly List<Probe> Probes = new();
        public bool BeforeServicesSet;
    }

    // Resolves a *scoped* Probe from ctx.Services in both hooks — proving scope resolution + intra-write identity.
    sealed class ScopeProbeInterceptor : IScopedDocumentInterceptor
    {
        readonly ScopeCapture cap;
        public ScopeProbeInterceptor(ScopeCapture cap) => this.cap = cap;

        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.cap.BeforeServicesSet = ctx.Services != null;
            var p = ctx.Services!.GetRequiredService<Probe>();
            this.cap.ProbeIds.Add(p.Id);
            this.cap.Probes.Add(p);
            return Task.CompletedTask;
        }

        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            var p = ctx.Services!.GetRequiredService<Probe>();
            this.cap.ProbeIds.Add(p.Id);
            return Task.CompletedTask;
        }
    }

    static (IServiceProvider Sp, IDocumentStore Store) BuildStore(Action<IServiceCollection> configure)
    {
        var services = new ServiceCollection();
        configure(services);
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        var sp = services.BuildServiceProvider();
        return (sp, sp.GetRequiredService<IDocumentStore>());
    }

    [Fact]
    public async Task FallbackScope_ResolvesScopedService_SameInstanceWithinWrite_DisposedAfter()
    {
        var cap = new ScopeCapture();
        var (sp, store) = BuildStore(s =>
        {
            s.AddScoped<Probe>();
            s.AddSingleton(cap);
            s.AddSingleton<IDocumentInterceptor>(x => new ScopeProbeInterceptor(x.GetRequiredService<ScopeCapture>()));
        });
        using (sp as IDisposable)
        {
            await store.Insert(new User { Id = "u1", Name = "A" });

            Assert.True(cap.BeforeServicesSet);                 // ctx.Services populated (fallback child scope)
            Assert.Equal(2, cap.ProbeIds.Count);
            Assert.Equal(cap.ProbeIds[0], cap.ProbeIds[1]);     // same scoped instance across Before + After
            Assert.True(cap.Probes[0].Disposed);               // fallback scope disposed after AfterWrite
        }
    }

    [Fact]
    public async Task FallbackScope_IsFreshPerWrite()
    {
        var cap = new ScopeCapture();
        var (sp, store) = BuildStore(s =>
        {
            s.AddScoped<Probe>();
            s.AddSingleton(cap);
            s.AddSingleton<IDocumentInterceptor>(x => new ScopeProbeInterceptor(x.GetRequiredService<ScopeCapture>()));
        });
        using (sp as IDisposable)
        {
            await store.Insert(new User { Id = "u1", Name = "A" });
            await store.Insert(new User { Id = "u2", Name = "B" });

            // Each write opens its own child scope → the two writes see different scoped Probe instances.
            Assert.NotEqual(cap.ProbeIds[0], cap.ProbeIds[2]);
        }
    }

    // ── ctx.Store — transaction visibility + no deadlock on a shared-connection provider ─────────────

    sealed class StoreVisibilityInterceptor : IDocumentInterceptor
    {
        public bool BeforeSawRow;
        public bool AfterSawRow;
        public int Invocations;

        public async Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.Invocations++;
            if (ctx.DocumentType != typeof(User)) return;
            // Read through ctx.Store in shared (single-connection) mode — must not deadlock.
            var id = ((User)ctx.Document!).Id;
            this.BeforeSawRow = await ctx.Store.Get<User>(id, cancellationToken: ct) != null; // prior state: not yet written
        }

        public async Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            if (ctx.DocumentType != typeof(User)) return;
            this.AfterSawRow = await ctx.Store.Get<User>((string)ctx.Id!, cancellationToken: ct) != null; // uncommitted row, visible
            // Side-effect write on the same transaction; suppress so it does not re-enter this interceptor.
            using (ctx.Store.SuppressInterceptors())
                await ctx.Store.Insert(new OutboxEntry { Id = Guid.NewGuid().ToString("N"), Event = "UserWritten" }, cancellationToken: ct);
        }
    }

    [Fact]
    public async Task CtxStore_ReadYourWrites_NoDeadlock_AtomicOutbox_NoReentry()
    {
        var interceptor = new StoreVisibilityInterceptor();
        var (sp, store) = BuildStore(s => s.AddSingleton<IDocumentInterceptor>(interceptor));
        using (sp as IDisposable)
        {
            await store.Insert(new User { Id = "u1", Name = "A" });

            Assert.False(interceptor.BeforeSawRow);            // BeforeWrite: row not there yet
            Assert.True(interceptor.AfterSawRow);              // AfterWrite: sees its own uncommitted row
            Assert.Equal(1, interceptor.Invocations);          // suppressed outbox insert did NOT re-fire
            Assert.Single(await store.Query<OutboxEntry>().ToList()); // outbox row committed atomically
        }
    }

    sealed class ThrowingOutboxInterceptor : IDocumentInterceptor
    {
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;

        public async Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            if (ctx.DocumentType != typeof(User)) return;
            using (ctx.Store.SuppressInterceptors())
                await ctx.Store.Insert(new OutboxEntry { Id = "ob1", Event = "UserWritten" }, cancellationToken: ct);
            throw new InvalidOperationException("boom");        // aborts the implicit unit → everything rolls back
        }
    }

    [Fact]
    public async Task CtxStore_AfterWriteThrows_RollsBackTriggeringWriteAndSideEffect()
    {
        var (sp, store) = BuildStore(s => s.AddSingleton<IDocumentInterceptor>(new ThrowingOutboxInterceptor()));
        using (sp as IDisposable)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => store.Insert(new User { Id = "u1", Name = "A" }));

            Assert.Null(await store.Get<User>("u1"));           // triggering write rolled back
            Assert.Empty(await store.Query<OutboxEntry>().ToList()); // side-effect rolled back atomically
        }
    }

    // ── Order sequencing ─────────────────────────────────────────────────────────────────────────

    sealed class OrderedInterceptor : IDocumentInterceptor
    {
        readonly List<int> sink;
        readonly int order;
        public OrderedInterceptor(List<int> sink, int order) { this.sink = sink; this.order = order; }
        public int Order => this.order;
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct) { this.sink.Add(this.order); return Task.CompletedTask; }
        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [Fact]
    public async Task Order_SequencesInterceptorsAscending()
    {
        var seen = new List<int>();
        var (sp, store) = BuildStore(s =>
        {
            s.AddSingleton<IDocumentInterceptor>(new OrderedInterceptor(seen, 30));
            s.AddSingleton<IDocumentInterceptor>(new OrderedInterceptor(seen, 10));
            s.AddSingleton<IDocumentInterceptor>(new OrderedInterceptor(seen, 20));
        });
        using (sp as IDisposable)
        {
            await store.Insert(new User { Id = "u1", Name = "A" });
            Assert.Equal(new[] { 10, 20, 30 }, seen);
        }
    }

    // ── Mode A: DocumentContext flows the caller's request scope (not a fresh child scope) ──────────

    sealed class CapturingScopedInterceptor : IScopedDocumentInterceptor
    {
        readonly Action<Guid> onBefore;
        public CapturingScopedInterceptor(Action<Guid> onBefore) => this.onBefore = onBefore;
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            if (ctx.DocumentType == typeof(User))
                this.onBefore(ctx.Services!.GetRequiredService<Probe>().Id);
            return Task.CompletedTask;
        }
        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [Fact]
    public async Task DocumentContext_FlowsRequestScope_InterceptorSeesSameScopedInstance()
    {
        Guid? seenInInterceptor = null;
        var services = new ServiceCollection();
        services.AddScoped<Probe>();
        services.AddSingleton<IDocumentInterceptor>(new CapturingScopedInterceptor(id => seenInInterceptor = id));
        services.AddSampleDocumentContext(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
            o.UseReflectionFallback = false;
        });
        await using var sp = services.BuildServiceProvider();
        using var scope = sp.CreateScope();
        var ctx = scope.ServiceProvider.GetRequiredService<SampleDocumentContext>();
        var requestProbe = scope.ServiceProvider.GetRequiredService<Probe>();

        await ctx.Users.Insert(new User { Id = "u1", Name = "A" });

        // The interceptor resolved the *request's* Probe via ctx.Services — proving the request scope flowed in,
        // not a fresh child scope (which would have a different Guid).
        Assert.Equal(requestProbe.Id, seenInInterceptor);
    }

    // A plain (non-scoped) interceptor must NOT get a fallback scope — ctx.Services stays null, hot path preserved.
    sealed class PlainServicesInterceptor : IDocumentInterceptor
    {
        public bool? ServicesWasNull;
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct) { this.ServicesWasNull = ctx.Services == null; return Task.CompletedTask; }
        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [Fact]
    public async Task PlainInterceptor_GetsNoFallbackScope()
    {
        var interceptor = new PlainServicesInterceptor();
        var (sp, store) = BuildStore(s =>
        {
            s.AddScoped<Probe>();
            s.AddSingleton<IDocumentInterceptor>(interceptor);
        });
        using (sp as IDisposable)
        {
            await store.Insert(new User { Id = "u1", Name = "A" });
            Assert.True(interceptor.ServicesWasNull);           // no IScopedDocumentInterceptor → no scope opened
        }
    }
}
