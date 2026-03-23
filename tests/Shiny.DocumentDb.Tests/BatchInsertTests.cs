using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class BatchInsertTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly DocumentStore store;

    protected BatchInsertTestsBase(IDatabaseFixture fixture)
    {
        this.Fixture = fixture;
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose() => this.store.Dispose();

    [Fact]
    public async Task BatchInsert_InsertsAll_StringIds()
    {
        var users = Enumerable.Range(1, 100).Select(i => new User
        {
            Id = $"user-{i}",
            Name = $"User {i}",
            Age = 20 + i
        }).ToList();

        var count = await this.store.BatchInsert(users);

        Assert.Equal(100, count);
        Assert.Equal(100, await this.store.Count<User>());
    }

    [Fact]
    public async Task BatchInsert_AutoGenerates_GuidIds()
    {
        var models = Enumerable.Range(1, 50).Select(i => new GuidIdModel
        {
            Name = $"Item {i}"
        }).ToList();

        var count = await this.store.BatchInsert(models);

        Assert.Equal(50, count);
        Assert.All(models, m => Assert.NotEqual(Guid.Empty, m.Id));
        // All IDs should be unique
        Assert.Equal(50, models.Select(m => m.Id).Distinct().Count());
    }

    [Fact]
    public async Task BatchInsert_AutoGenerates_IntIds()
    {
        var models = Enumerable.Range(1, 25).Select(i => new IntIdModel
        {
            Name = $"Item {i}"
        }).ToList();

        var count = await this.store.BatchInsert(models);

        Assert.Equal(25, count);
        Assert.All(models, m => Assert.NotEqual(0, m.Id));
        // Sequential IDs starting from 1
        Assert.Equal(1, models.First().Id);
        Assert.Equal(25, models.Last().Id);
    }

    [Fact]
    public async Task BatchInsert_AutoGenerates_LongIds()
    {
        var models = Enumerable.Range(1, 10).Select(i => new LongIdModel
        {
            Name = $"Item {i}"
        }).ToList();

        var count = await this.store.BatchInsert(models);

        Assert.Equal(10, count);
        Assert.Equal(1L, models.First().Id);
        Assert.Equal(10L, models.Last().Id);
    }

    [Fact]
    public async Task BatchInsert_RollsBack_OnDuplicate()
    {
        await this.store.Insert(new User { Id = "existing", Name = "Existing", Age = 30 });

        var users = new[]
        {
            new User { Id = "new-1", Name = "New 1", Age = 25 },
            new User { Id = "existing", Name = "Duplicate", Age = 40 }, // will fail
            new User { Id = "new-2", Name = "New 2", Age = 35 }
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => this.store.BatchInsert(users));

        // Only the original should remain — batch was rolled back
        Assert.Equal(1, await this.store.Count<User>());
        var original = await this.store.Get<User>("existing");
        Assert.Equal("Existing", original!.Name);
    }

    [Fact]
    public async Task BatchInsert_EmptyCollection_ReturnsZero()
    {
        var count = await this.store.BatchInsert(Array.Empty<User>());
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task BatchInsert_WorksWithTablePerType()
    {
        using var mappedStore = new DocumentStore(
            new DocumentStoreOptions
            {
                DatabaseProvider = Fixture.CreateProvider()
            }.MapTypeToTable<User>($"users_{Guid.NewGuid():N}")
        );

        var users = Enumerable.Range(1, 10).Select(i => new User
        {
            Id = $"u-{i}", Name = $"User {i}", Age = 20 + i
        }).ToList();

        var count = await mappedStore.BatchInsert(users);

        Assert.Equal(10, count);
        Assert.Equal(10, await mappedStore.Count<User>());
    }

    [Fact]
    public async Task BatchInsert_InsideTransaction_UsesExistingTransaction()
    {
        await this.store.RunInTransaction(async tx =>
        {
            var users = Enumerable.Range(1, 5).Select(i => new User
            {
                Id = $"tx-{i}", Name = $"TxUser {i}", Age = 30
            }).ToList();

            var count = await tx.BatchInsert(users);
            Assert.Equal(5, count);
        });

        Assert.Equal(5, await this.store.Count<User>());
    }

    [Fact]
    public async Task BatchInsert_InsideTransaction_RollsBackWithTransaction()
    {
        try
        {
            await this.store.RunInTransaction(async tx =>
            {
                await tx.BatchInsert(Enumerable.Range(1, 5).Select(i => new User
                {
                    Id = $"tx-{i}", Name = $"TxUser {i}", Age = 30
                }));

                throw new Exception("Simulated failure");
            });
        }
        catch { }

        Assert.Equal(0, await this.store.Count<User>());
    }

    [Fact]
    public async Task BatchInsert_IntIds_ContinuesFromExisting()
    {
        // Pre-insert some items
        await this.store.Insert(new IntIdModel { Name = "Existing 1" });
        await this.store.Insert(new IntIdModel { Name = "Existing 2" });

        var newModels = Enumerable.Range(1, 3).Select(i => new IntIdModel
        {
            Name = $"New {i}"
        }).ToList();

        await this.store.BatchInsert(newModels);

        Assert.Equal(3, newModels.First().Id);
        Assert.Equal(5, newModels.Last().Id);
        Assert.Equal(5, await this.store.Count<IntIdModel>());
    }
}
