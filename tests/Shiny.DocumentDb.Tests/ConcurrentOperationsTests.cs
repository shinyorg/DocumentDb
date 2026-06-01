using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Asserts a single <see cref="IDocumentStore"/> instance handles concurrent operations from
/// multiple callers safely. For shared-connection providers (SQLite, DuckDB) this proves the
/// store-level semaphore serializes work without deadlocking; for pooled providers
/// (PostgreSQL/MySQL/SQL Server) it proves the per-op connection model actually multiplexes
/// callers through the ADO.NET driver pool.
/// </summary>
public abstract class ConcurrentOperationsTestsBase
{
    protected readonly IDocumentStoreFixture Fixture;

    protected ConcurrentOperationsTestsBase(IDocumentStoreFixture fixture)
        => this.Fixture = fixture;

    [Fact]
    public async Task ParallelInserts_DistinctIds_AllSucceed()
    {
        using var store = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;

        const int n = 50;
        var inserts = Enumerable.Range(0, n)
            .Select(i => s.Insert(new User { Id = $"u{i}", Name = $"User{i}", Age = i }))
            .ToArray();

        await Task.WhenAll(inserts);

        var count = await s.Count<User>();
        Assert.Equal(n, count);
    }

    [Fact]
    public async Task ParallelInserts_SameId_OneWinsOthersThrow()
    {
        using var store = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;

        const int n = 20;
        var inserts = Enumerable.Range(0, n)
            .Select(_ => Task.Run(async () =>
            {
                try
                {
                    await s.Insert(new User { Id = "same", Name = "X", Age = 1 });
                    return true;
                }
                catch (InvalidOperationException)
                {
                    return false;
                }
            }))
            .ToArray();

        var results = await Task.WhenAll(inserts);
        Assert.Equal(1, results.Count(r => r));
        Assert.Equal(1, await s.Count<User>());
    }

    [Fact]
    public async Task ParallelReads_AfterSeed_AllReturnDocument()
    {
        using var store = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "u1", Name = "Alice", Age = 30 });

        const int n = 50;
        var reads = Enumerable.Range(0, n)
            .Select(_ => s.Get<User>("u1"))
            .ToArray();

        var results = await Task.WhenAll(reads);
        Assert.All(results, r =>
        {
            Assert.NotNull(r);
            Assert.Equal("Alice", r!.Name);
        });
    }

    [Fact]
    public async Task ParallelUpdates_DistinctDocs_AllPersist()
    {
        using var store = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;

        const int n = 30;
        for (var i = 0; i < n; i++)
            await s.Insert(new User { Id = $"u{i}", Name = $"User{i}", Age = i });

        var updates = Enumerable.Range(0, n)
            .Select(i => s.Update(new User { Id = $"u{i}", Name = $"Updated{i}", Age = i + 100 }))
            .ToArray();
        await Task.WhenAll(updates);

        for (var i = 0; i < n; i++)
        {
            var got = await s.Get<User>($"u{i}");
            Assert.NotNull(got);
            Assert.Equal($"Updated{i}", got!.Name);
            Assert.Equal(i + 100, got.Age);
        }
    }

    [Fact]
    public async Task ParallelFirstTouchOfTable_TableInitRunsOnce()
    {
        // The Lazy<Task>-backed table-init cache must let many concurrent first-touch callers
        // share a single init. If init races, "CREATE TABLE IF NOT EXISTS" would still succeed
        // but secondary index/column DDL on some providers (e.g. MySQL CREATE INDEX) is not
        // idempotent and would surface a duplicate error. This test would fail loudly in that
        // case; it would pass trivially today, so its real value is regression protection.
        using var store = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;

        var ops = Enumerable.Range(0, 20)
            .Select(i => s.Insert(new User { Id = $"u{i}", Name = $"U{i}", Age = i }))
            .ToArray();
        await Task.WhenAll(ops);

        Assert.Equal(20, await s.Count<User>());
    }
}
