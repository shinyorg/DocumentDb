using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// The RunInTransaction extension + the "SaveChanges never commits a transaction it didn't create" contract.
public class RunInTransactionTests
{
    static SqliteDocumentStore NewStore() => new(new DocumentStoreOptions
    {
        DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
        TableName = $"t{Guid.NewGuid():N}"
    });

    [Fact]
    public async Task RunInTransaction_Commits()
    {
        using var store = NewStore();
        await using var session = ((IDocumentStore)store).OpenSession();

        await session.RunInTransaction(async () =>
        {
            session.Add(new User { Id = "u1", Name = "A" });
            session.Add(new User { Id = "u2", Name = "B" });
            await Task.CompletedTask;
        });

        Assert.Null(session.CurrentTransaction);                 // transaction closed
        Assert.Equal(2, await ((IDocumentStore)store).Count<User>());
    }

    [Fact]
    public async Task RunInTransaction_RollsBack_OnWorkerException()
    {
        using var store = NewStore();
        await using var session = ((IDocumentStore)store).OpenSession();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            session.RunInTransaction(async () =>
            {
                session.Add(new User { Id = "u1", Name = "A" });
                await session.SaveChanges();                     // flushed into the tx (not committed)
                throw new InvalidOperationException("boom");      // → dispose rolls the tx back
            }));

        Assert.Null(session.CurrentTransaction);
        Assert.Equal(0, await ((IDocumentStore)store).Count<User>());   // nothing committed
    }

    [Fact]
    public async Task RunInTransaction_AutoSaveChangesFalse_DoesNotFlushBuffer()
    {
        using var store = NewStore();
        await using var session = ((IDocumentStore)store).OpenSession();

        // Worker buffers but never flushes; autoSaveChanges:false → the buffer is not flushed before commit.
        await session.RunInTransaction(
            () => { session.Add(new User { Id = "u1", Name = "A" }); return Task.CompletedTask; },
            autoSaveChanges: false);

        Assert.Equal(0, await ((IDocumentStore)store).Count<User>());
        Assert.Equal(1, session.PendingCount);                   // still buffered
    }

    [Fact]
    public async Task RunInTransaction_AutoSaveChangesFalse_WorkerFlushesItself()
    {
        using var store = NewStore();
        await using var session = ((IDocumentStore)store).OpenSession();

        await session.RunInTransaction(
            async () => { session.Add(new User { Id = "u1", Name = "A" }); await session.SaveChanges(); },
            autoSaveChanges: false);

        Assert.Equal(1, await ((IDocumentStore)store).Count<User>());
    }

    // The contract: SaveChanges must NOT commit a transaction it didn't create.
    [Fact]
    public async Task SaveChanges_DoesNotCommit_AnExplicitTransaction()
    {
        using var store = NewStore();
        await using var session = ((IDocumentStore)store).OpenSession();

        await using (var tx = await session.BeginTransaction())
        {
            session.Add(new User { Id = "u1", Name = "A" });
            await session.SaveChanges();                         // flush into tx — must NOT commit
            // no tx.Commit() → leaving the using disposes → rollback
        }

        Assert.Equal(0, await ((IDocumentStore)store).Count<User>());   // proves SaveChanges didn't commit
    }

    // ── Open-a-session convenience overloads (factory + store hand the session to the worker) ────────

    [Fact]
    public async Task StoreRunInTransaction_HandsSession_Commits()
    {
        using var store = NewStore();

        await ((IDocumentStore)store).RunInTransaction(async session =>
        {
            session.Add(new User { Id = "u1", Name = "A" });
            await Task.CompletedTask;
        });

        Assert.Equal(1, await ((IDocumentStore)store).Count<User>());
    }

    [Fact]
    public async Task FactoryRunInTransaction_HandsSession_Commits()
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider($"Data Source=rt_{Guid.NewGuid():N};Mode=Memory;Cache=Shared");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        await using var sp = services.BuildServiceProvider();
        var factory = sp.GetRequiredService<IDocumentSessionFactory>();
        var store = sp.GetRequiredService<IDocumentStore>();

        await factory.RunInTransaction(async session =>
        {
            session.Add(new User { Id = "u1", Name = "A" }).Add(new User { Id = "u2", Name = "B" });
            await Task.CompletedTask;
        });

        Assert.Equal(2, await store.Count<User>());
    }

    [Fact]
    public async Task FactoryRunInTransaction_RollsBack_OnException()
    {
        var services = new ServiceCollection();
        services.AddDocumentStore(o =>
        {
            o.DatabaseProvider = new SqliteDatabaseProvider($"Data Source=rt_{Guid.NewGuid():N};Mode=Memory;Cache=Shared");
            o.TableName = $"t{Guid.NewGuid():N}";
        });
        await using var sp = services.BuildServiceProvider();
        var factory = sp.GetRequiredService<IDocumentSessionFactory>();
        var store = sp.GetRequiredService<IDocumentStore>();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            factory.RunInTransaction(async session =>
            {
                session.Add(new User { Id = "u1", Name = "A" });
                await session.SaveChanges();
                throw new InvalidOperationException("boom");
            }));

        Assert.Equal(0, await store.Count<User>());   // rolled back; session disposed
    }
}
