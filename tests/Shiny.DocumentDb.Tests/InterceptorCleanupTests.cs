using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Interceptor cleanup v2 (store-as-connection): scoped interceptors need no marker and resolve the caller's
// own session scope; ctx.Session is the transaction-bound re-entrancy handle; ctx.Services is never null.
public class InterceptorCleanupTests
{
    public sealed class Outbox { public string Id { get; set; } = ""; public string Event { get; set; } = ""; }

    sealed class RequestId { public Guid Value { get; } = Guid.NewGuid(); }

    // ── P1: a scoped interceptor resolves the SESSION's scope (not a fresh fallback) ─────────────────
    sealed class CaptureScopedInterceptor : IDocumentInterceptor
    {
        readonly List<Guid> seen;
        public CaptureScopedInterceptor(List<Guid> seen) => this.seen = seen;
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            this.seen.Add(ctx.Services.GetRequiredService<RequestId>().Value);   // no marker; Services non-null
            return Task.CompletedTask;
        }
        public Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
    }

    [Fact]
    public async Task ScopedInterceptor_ResolvesTheSessionScope_NoMarker()
    {
        var seen = new List<Guid>();
        var services = new ServiceCollection();
        services.AddScoped<RequestId>();
        services.AddSingleton(seen);
        services.AddSingleton<IDocumentInterceptor>(sp => new CaptureScopedInterceptor(sp.GetRequiredService<List<Guid>>()));
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        }).AddScopedDocumentSession();
        await using var sp = services.BuildServiceProvider();

        Guid requestScopeId;
        await using (var scope = sp.CreateAsyncScope())
        {
            requestScopeId = scope.ServiceProvider.GetRequiredService<RequestId>().Value;
            var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
            session.Add(new User { Id = "u1", Name = "A" });
            await session.SaveChanges();   // interceptor runs on this session's scope
        }

        Assert.Single(seen);
        Assert.Equal(requestScopeId, seen[0]);   // resolved the request scope's RequestId, not a fresh one
    }

    // ── P2: ctx.Session re-entrant write joins the write's transaction ───────────────────────────────
    sealed class OutboxInterceptor : IDocumentInterceptor
    {
        public Task BeforeWrite(DocumentWriteContext ctx, CancellationToken ct) => Task.CompletedTask;
        public async Task AfterWrite(DocumentWriteContext ctx, CancellationToken ct)
        {
            if (ctx.DocumentType != typeof(User)) return;
            using (ctx.Session.SuppressInterceptors())      // don't recurse
            {
                ctx.Session.Add(new Outbox { Id = $"ob-{ctx.Id}", Event = "user-written" });
                await ctx.Session.SaveChanges(ct);          // flushes into the SAME transaction
            }
        }
    }

    [Fact]
    public async Task CtxSession_ReentrantWrite_CommitsAtomically()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IDocumentInterceptor, OutboxInterceptor>();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        await using var sp = services.BuildServiceProvider();
        var store = sp.GetRequiredService<IDocumentStore>();

        await store.Insert(new User { Id = "u1", Name = "A" });

        Assert.Equal(1, await store.Count<User>());
        Assert.Equal(1, await store.Count<Outbox>());                 // outbox row committed with the user
        Assert.NotNull(await store.Get<Outbox>("ob-u1"));
    }
}
