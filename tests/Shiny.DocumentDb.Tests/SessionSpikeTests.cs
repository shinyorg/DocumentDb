using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Store-as-connection spike (docs/plans/store-as-connection.md): validates the IDocumentSession unit-of-work,
/// explicit IDocumentTransaction, IDocumentSessionFactory scope-ownership, and AddScopedDocumentSession over SQLite.
/// </summary>
public class SessionSpikeTests
{
    static ServiceProvider BuildProvider(Action<IServiceCollection>? extra = null)
    {
        var services = new ServiceCollection();
        // A shared in-memory SQLite DB (one connection for the singleton store's lifetime).
        services.AddDocumentStore(o =>
            o.DatabaseProvider = new SqliteDatabaseProvider(
                $"Data Source=spike_{Guid.NewGuid():N};Mode=Memory;Cache=Shared"));
        services.AddScopedDocumentSession();
        extra?.Invoke(services);
        return services.BuildServiceProvider();
    }

    static User U(string id, int age = 0) => new() { Id = id, Name = id.ToUpperInvariant(), Age = age };

    // ── Unit of work: buffer then commit ───────────────────────────────────

    [Fact]
    public async Task Buffered_Writes_Not_Visible_Until_SaveChanges()
    {
        await using var sp = BuildProvider();
        var store = sp.GetRequiredService<IDocumentStore>();
        var factory = sp.GetRequiredService<IDocumentSessionFactory>();

        await using var session = factory.OpenSession();
        session.Add(U("a")).Add(U("b"));

        Assert.Equal(2, session.PendingCount);
        Assert.Null(await session.Get<User>("a"));          // reads don't see the un-flushed buffer
        Assert.Equal(0, await store.Count<User>());

        await session.SaveChanges();

        Assert.Equal(0, session.PendingCount);
        Assert.Equal(2, await store.Count<User>());          // committed atomically
        Assert.NotNull(await session.Get<User>("a"));
    }

    [Fact]
    public async Task ClearPending_Discards_Buffer()
    {
        await using var sp = BuildProvider();
        var store = sp.GetRequiredService<IDocumentStore>();
        await using var session = sp.GetRequiredService<IDocumentSessionFactory>().OpenSession();

        session.Add(U("a")).Add(U("b"));
        session.ClearPending();
        await session.SaveChanges();

        Assert.Equal(0, session.PendingCount);
        Assert.Equal(0, await store.Count<User>());
    }

    // ── Explicit transactions ──────────────────────────────────────────────

    [Fact]
    public async Task Transaction_Commit_Persists_And_SaveChanges_Joins_It()
    {
        await using var sp = BuildProvider();
        var store = sp.GetRequiredService<IDocumentStore>();
        await using var session = sp.GetRequiredService<IDocumentSessionFactory>().OpenSession();

        await using var tx = await session.BeginTransaction();
        Assert.Same(tx, session.CurrentTransaction);
        Assert.True(tx.IsActive);

        session.Add(U("a", 1));
        await session.SaveChanges();                          // flush INTO the tx, not committed yet
        // Visible within the same transaction (same pinned connection):
        var inside = await session.Get<User>("a", LockMode.Update);
        Assert.NotNull(inside);

        await tx.Commit();
        Assert.False(tx.IsActive);
        Assert.Null(session.CurrentTransaction);
        Assert.Equal(1, await store.Count<User>());
    }

    [Fact]
    public async Task Transaction_Rollback_Discards()
    {
        await using var sp = BuildProvider();
        var store = sp.GetRequiredService<IDocumentStore>();
        await using var session = sp.GetRequiredService<IDocumentSessionFactory>().OpenSession();

        var tx = await session.BeginTransaction();
        session.Add(U("a"));
        await session.SaveChanges();
        await tx.Rollback();

        Assert.Null(session.CurrentTransaction);
        Assert.Equal(0, await store.Count<User>());
    }

    [Fact]
    public async Task Transaction_Dispose_Without_Commit_RollsBack()
    {
        await using var sp = BuildProvider();
        var store = sp.GetRequiredService<IDocumentStore>();
        await using var session = sp.GetRequiredService<IDocumentSessionFactory>().OpenSession();

        await using (var tx = await session.BeginTransaction())
        {
            session.Add(U("a"));
            await session.SaveChanges();
            // no Commit → dispose rolls back
        }

        Assert.Null(session.CurrentTransaction);
        Assert.Equal(0, await store.Count<User>());
    }

    [Fact]
    public async Task Only_One_Transaction_Active_At_A_Time()
    {
        await using var sp = BuildProvider();
        await using var session = sp.GetRequiredService<IDocumentSessionFactory>().OpenSession();

        await using var tx = await session.BeginTransaction();
        await Assert.ThrowsAsync<InvalidOperationException>(() => session.BeginTransaction());
    }

    [Fact]
    public async Task LockingRead_Outside_Transaction_Throws()
    {
        await using var sp = BuildProvider();
        await using var session = sp.GetRequiredService<IDocumentSessionFactory>().OpenSession();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => session.Get<User>("x", LockMode.Update));
    }

    // ── DI lifetimes ───────────────────────────────────────────────────────

    [Fact]
    public async Task Scoped_Session_Is_Injectable_And_Persists()
    {
        await using var sp = BuildProvider();
        var store = sp.GetRequiredService<IDocumentStore>();

        await using (var scope = sp.CreateAsyncScope())   // IDocumentSession is IAsyncDisposable
        {
            var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
            session.Add(U("a"));
            await session.SaveChanges();
        }

        Assert.Equal(1, await store.Count<User>());
    }

    [Fact]
    public async Task Factory_Session_Owns_And_Disposes_Its_Scope()
    {
        await using var sp = BuildProvider(s => s.AddScoped<ScopedProbe>());
        var factory = sp.GetRequiredService<IDocumentSessionFactory>();

        ScopedProbe probe;
        await using (var session = factory.OpenSession())
        {
            probe = session.Services.GetRequiredService<ScopedProbe>();   // resolved from the session's owned scope
            Assert.False(probe.Disposed);
        }
        Assert.True(probe.Disposed);                                       // disposing the session disposed the scope
    }

    sealed class ScopedProbe : IDisposable
    {
        public bool Disposed { get; private set; }
        public void Dispose() => this.Disposed = true;
    }
}
