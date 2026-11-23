using Microsoft.Data.Sqlite;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.Sqlite;

/// <summary>
/// SqliteDatabaseProvider.ConcurrentReads: reads on read-only connections beside the single writer, for a WAL file
/// database, and the single connection everywhere else.
/// </summary>
public class SqliteConcurrentReadTests : IDisposable
{
    readonly string dir = Path.Combine(Path.GetTempPath(), "docdb-reads", Guid.NewGuid().ToString("N"));

    public SqliteConcurrentReadTests() => Directory.CreateDirectory(this.dir);

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        Directory.Delete(this.dir, true);
    }

    DocumentStore FileStore(bool concurrentReads) => new(new DocumentStoreOptions
    {
        DatabaseProvider = new SqliteDatabaseProvider($"Data Source={Path.Combine(this.dir, "test.db")}") { ConcurrentReads = concurrentReads },
        TableName = "docs"
    });

    static readonly TimeSpan Blocked = TimeSpan.FromMilliseconds(300);

    // Opens a unit that has written u1 = "after" and holds the writer until the returned commit runs.
    static async Task<(IDocumentSession Session, IDocumentTransaction Tx)> HoldWriterAsync(IDocumentStore store)
    {
        var session = store.OpenSession();
        var tx = await session.BeginTransaction();
        session.Update(new User { Id = "u1", Name = "after" });
        await session.SaveChanges();
        return (session, tx);
    }

    [Fact]
    public async Task Reads_do_not_wait_for_an_open_transaction_and_see_committed_state()
    {
        using var store = this.FileStore(concurrentReads: true);
        await store.Insert(new User { Id = "u1", Name = "before" });

        var (session, tx) = await HoldWriterAsync(store);
        try
        {
            var get = store.Get<User>("u1");
            var list = store.Query<User>().Where(x => x.Name == "before").ToList();
            var count = store.Query<User>().Count();

            await Task.WhenAll(get, list, count).WaitAsync(Blocked);
            Assert.Equal("before", (await get)!.Name);
            Assert.Single(await list);
            Assert.Equal(1, await count);

            // inside the unit, reads go to the unit's connection and see its uncommitted write
            Assert.Equal("after", (await session.Get<User>("u1"))!.Name);
        }
        finally
        {
            await tx.Commit();
            await session.DisposeAsync();
        }

        Assert.Equal("after", (await store.Get<User>("u1"))!.Name);
    }

    [Fact]
    public async Task Without_the_option_reads_wait_for_the_writer()
    {
        using var store = this.FileStore(concurrentReads: false);
        await store.Insert(new User { Id = "u1", Name = "before" });

        var (session, tx) = await HoldWriterAsync(store);
        var get = store.Get<User>("u1");
        await Task.Delay(Blocked);
        Assert.False(get.IsCompleted);

        await tx.Commit();
        await session.DisposeAsync();
        Assert.Equal("after", (await get)!.Name);
    }

    [Fact]
    public async Task An_in_memory_database_keeps_the_single_connection()
    {
        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:") { ConcurrentReads = true },
            TableName = "docs"
        });
        await store.Insert(new User { Id = "u1", Name = "before" });

        var (session, tx) = await HoldWriterAsync(store);
        var get = store.Get<User>("u1");
        await Task.Delay(Blocked);
        Assert.False(get.IsCompleted);

        await tx.Commit();
        await session.DisposeAsync();
        Assert.Equal("after", (await get)!.Name);
    }

    [Fact]
    public async Task A_read_first_on_a_new_database_creates_it_through_the_writer()
    {
        using var store = this.FileStore(concurrentReads: true);

        Assert.Empty(await store.Query<User>().ToList());
        Assert.Null(await store.Get<User>("missing"));
        await store.Insert(new User { Id = "u1", Name = "a" });
        Assert.Single(await store.Query<User>().ToList());
    }

    [Fact]
    public async Task Reads_and_writes_interleave_under_load()
    {
        using var store = this.FileStore(concurrentReads: true);
        const int writers = 4, perWriter = 100;

        var writes = Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
        {
            foreach (var i in Enumerable.Range(0, perWriter))
                await store.Insert(new User { Id = $"w{w}-{i}", Name = $"n{i}", Age = i });
        }));

        var reads = Enumerable.Range(0, 6).Select(r => Task.Run(async () =>
        {
            var seen = 0L;
            foreach (var i in Enumerable.Range(0, 50))
            {
                var count = await store.Query<User>().Count();
                Assert.True(count >= seen, "a later read saw fewer committed rows");
                seen = count;

                var streamed = 0;
                await foreach (var _ in store.Query<User>().Where(x => x.Age < 10).ToAsyncEnumerable())
                    streamed++;
                Assert.True(streamed <= writers * 10);
            }
        }));

        await Task.WhenAll(writes.Concat(reads));
        Assert.Equal(writers * perWriter, await store.Query<User>().Count());
    }

    [Fact]
    public async Task Writes_through_query_terminals_stay_on_the_writer()
    {
        using var store = this.FileStore(concurrentReads: true);
        await store.Insert(new User { Id = "u1", Name = "a", Age = 1 });
        await store.Insert(new User { Id = "u2", Name = "b", Age = 2 });

        Assert.Equal(1, await store.Query<User>().Where(x => x.Age == 1).ExecuteDelete());
        Assert.Equal(1, await store.Query<User>().Count());
    }
}
