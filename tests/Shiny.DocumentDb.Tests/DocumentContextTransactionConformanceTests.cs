using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Issue #10: a <see cref="DocumentContext"/>'s typed sets must run inside the context's explicit transaction.
/// They used to go to the root store — on SQLite/DuckDB that waited on the single connection the transaction
/// holds (a hang), and on the pooled providers it committed outside the caller's transaction.
/// </summary>
public abstract class DocumentContextTransactionConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    // A regression is a hang on the shared-connection providers — fail instead of stalling the suite.
    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    static User NewUser(string id, string name, int age = 30) => new() { Id = id, Name = name, Age = age };

    [Fact]
    public async Task TypedSets_JoinTheTransaction_AndCommit()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var store = (IDocumentStore)owner;
        await store.Insert(NewUser("old1", "Old 1"));
        await store.Insert(NewUser("old2", "Old 2"));

        await RunAsync(async () =>
        {
            await using var db = new SampleDocumentContext(store.OpenSession());
            await using var tx = await db.BeginTransaction();

            // The issue's exact sequence.
            Assert.Equal(2, await db.Users.Clear());
            Assert.Equal(3, await db.Users.BatchInsert([NewUser("u1", "A"), NewUser("u2", "B"), NewUser("u3", "C")]));

            // Every other set member reads/writes through the same transaction.
            await db.Users.Insert(NewUser("u4", "D"));
            await db.Users.Update(NewUser("u1", "A2"));
            await db.Users.Upsert(NewUser("u5", "E"));
            Assert.True(await db.Users.Remove("u2"));
            await db.Users.BatchUpsert([NewUser("u6", "F")]);
            await db.Users.BatchUpdate([NewUser("u3", "C2")]);
            Assert.Equal(1, await db.Users.BatchRemove(["u6"]));

            Assert.Equal("A2", (await db.Users.Get("u1"))!.Name);
            Assert.Equal(4, await db.Users.Count());
            Assert.Equal(4, (await db.Users.ToList()).Count);
            Assert.Single(await db.Users.Where(u => u.Name == "C2").ToList());

            // Buffered writes flushed by SaveChanges land in the same transaction as the immediate set calls.
            db.Add(NewUser("u7", "G"));
            await db.SaveChanges();
            Assert.Equal(5, await db.Users.Count());

            await tx.Commit();
        });

        var all = await store.Query<User>().ToList();
        Assert.Equal(["u1", "u3", "u4", "u5", "u7"], all.Select(u => u.Id).Order().ToArray());
    }

    [Fact]
    public async Task TypedSets_RollBackWithTheTransaction()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var store = (IDocumentStore)owner;
        await store.Insert(NewUser("old1", "Old 1"));
        await store.Insert(NewUser("old2", "Old 2"));

        await RunAsync(async () =>
        {
            await using var db = new SampleDocumentContext(store.OpenSession());
            await using var tx = await db.BeginTransaction();

            await db.Users.Clear();
            await db.Users.BatchInsert([NewUser("u1", "A"), NewUser("u2", "B")]);
            await db.Users.Insert(NewUser("u3", "C"));
            Assert.Equal(3, await db.Users.Count());

            await tx.Rollback();
        });

        // Nothing the sets did escaped the transaction.
        var all = await store.Query<User>().ToList();
        Assert.Equal(["old1", "old2"], all.Select(u => u.Id).Order().ToArray());
    }

    [Fact]
    public async Task TypedSets_AfterTheTransactionCloses_GoBackToTheRootStore()
    {
        using var owner = (IDisposable)this.Fixture.CreateStore($"t{Guid.NewGuid():N}");
        var store = (IDocumentStore)owner;

        await RunAsync(async () =>
        {
            await using var db = new SampleDocumentContext(store.OpenSession());
            await using (var tx = await db.BeginTransaction())
            {
                await db.Users.Insert(NewUser("u1", "A"));
                await tx.Commit();
            }

            // No transaction now — immediate writes auto-commit against the root store.
            await db.Users.Insert(NewUser("u2", "B"));
        });

        Assert.Equal(2, await store.Count<User>());
    }

    static Task RunAsync(Func<Task> body) => body().WaitAsync(Timeout);
}
