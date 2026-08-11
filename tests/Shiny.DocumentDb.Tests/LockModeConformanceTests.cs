using System.Diagnostics;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Proves <see cref="LockMode"/> actually locks. The dialect strings are covered by
/// <see cref="LockModeSqlTests"/>; this suite exists because a lock clause that parses but does not block is
/// indistinguishable from a working one until production. Everything downstream of pessimistic locking —
/// counter rows, leases, the Orleans stream sequencer — is only correct if these pass.
/// </summary>
public abstract class LockModeConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    /// <summary>A shared counter row — the shape every caller of a locking read actually wants.</summary>
    public class Counter
    {
        public string Id { get; set; } = null!;
        public long Value { get; set; }
    }

    [Fact]
    public async Task LockingRead_OutsideTransaction_Throws()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var store = (IDocumentStore)owner;
        await using var session = store.OpenSession();

        // A lock with no transaction to hold it would be released the moment the statement ended, so the
        // API refuses rather than handing back a false guarantee.
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => session.Get<Counter>("c1", LockMode.Update));
    }

    [Fact]
    public async Task LockingRead_ReturnsTheDocument()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var store = (IDocumentStore)owner;
        await store.Insert(new Counter { Id = "c1", Value = 7 });

        await using var session = store.OpenSession();
        await using var tx = await session.BeginTransaction();
        var read = await session.Get<Counter>("c1", LockMode.Update);

        // Guards the mechanical failure: a hint spliced into the wrong place, or a clause landing before a
        // global filter, produces a SQL error or an empty read rather than a locked row.
        Assert.NotNull(read);
        Assert.Equal(7, read!.Value);
        await tx.Commit();
    }

    [Fact]
    public async Task LockingRead_BlocksASecondLockerUntilCommit()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var store = (IDocumentStore)owner;
        await store.Insert(new Counter { Id = "c1", Value = 0 });

        var firstHasLock = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondAcquiredAt = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var committedAt = 0L;

        var first = Task.Run(async () =>
        {
            await using var session = store.OpenSession();
            await using var tx = await session.BeginTransaction();
            var doc = await session.Get<Counter>("c1", LockMode.Update);
            firstHasLock.SetResult();

            await releaseFirst.Task;
            doc!.Value = 1;
            session.Update(doc);
            await session.SaveChanges();
            committedAt = Stopwatch.GetTimestamp();
            await tx.Commit();
        });

        var second = Task.Run(async () =>
        {
            await firstHasLock.Task;
            await using var session = store.OpenSession();
            await using var tx = await session.BeginTransaction();
            var doc = await session.Get<Counter>("c1", LockMode.Update);
            secondAcquiredAt.SetResult(Stopwatch.GetTimestamp());
            await tx.Commit();
            return doc;
        });

        // Hold the lock long enough that an unlocked read would demonstrably win the race.
        await firstHasLock.Task;
        await Task.Delay(750);
        Assert.False(secondAcquiredAt.Task.IsCompleted, "The second locking read completed while the first transaction still held the lock — the lock is not being applied.");

        releaseFirst.SetResult();
        await first;
        var acquired = await secondAcquiredAt.Task;
        await second;

        // The second reader must have got in only after the first committed.
        Assert.True(acquired >= committedAt, "The second locking read acquired before the first transaction committed.");
    }

    [Fact]
    public async Task LockedCounter_SerializesConcurrentIncrements()
    {
        // The end-to-end reason this feature exists: N racing increments through a locking read must produce
        // exactly N, with no lost update. Without a real lock this lands short and does so intermittently.
        using var owner = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var store = (IDocumentStore)owner;
        await store.Insert(new Counter { Id = "c1", Value = 0 });

        const int workers = 8;
        await Task.WhenAll(Enumerable.Range(0, workers).Select(_ => Task.Run(async () =>
        {
            await using var session = store.OpenSession();
            await using var tx = await session.BeginTransaction();
            var doc = await session.Get<Counter>("c1", LockMode.Update);
            doc!.Value++;
            session.Update(doc);
            await session.SaveChanges();
            await tx.Commit();
        })));

        var final = await store.Get<Counter>("c1");
        Assert.Equal(workers, final!.Value);
    }
}

[Collection("PostgreSQL")]
public class PostgreSqlLockModeTests(PostgreSqlDatabaseFixture db) : LockModeConformanceTestsBase(db);

[Collection("CockroachDB")]
public class CockroachDbLockModeTests(CockroachDbDatabaseFixture db) : LockModeConformanceTestsBase(db);

[Collection("MSSQL")]
public class SqlServerLockModeTests(MsSqlDatabaseFixture db) : LockModeConformanceTestsBase(db);

[Collection("MySQL")]
public class MySqlLockModeTests(MySqlDatabaseFixture db) : LockModeConformanceTestsBase(db);

[Collection("MariaDB")]
public class MariaDbLockModeTests(MariaDbDatabaseFixture db) : LockModeConformanceTestsBase(db);

[Collection("Oracle")]
public class OracleLockModeTests(OracleDatabaseFixture db) : LockModeConformanceTestsBase(db);
