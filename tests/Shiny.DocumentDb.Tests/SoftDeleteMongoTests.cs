using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.MongoDb;

// Soft delete on a server-backed document provider — the third distinct code path after the relational store
// (SoftDeleteTests) and an embedded document store (LiteDB, in SoftDeleteTests).
[Collection("MongoDB")]
public class SoftDeleteMongoTests(MongoDbDatabaseFixture db)
{
    public class MongoSoftDoc
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public int Age { get; set; }
        public bool IsDeleted { get; set; }
    }

    [Fact]
    public async Task Remove_And_SetBased_SoftDelete()
    {
        using var store = db.CreateConfiguredStore(
            $"sd{Guid.NewGuid():N}",
            o => o.ConfigureDocument<MongoSoftDoc>(cfg => cfg.AddSoftDelete(x => x.IsDeleted)));

        await store.Insert(new MongoSoftDoc { Id = "m1", Name = "Alice", Age = 30 });
        await store.Insert(new MongoSoftDoc { Id = "m2", Name = "Bob", Age = 40 });
        await store.Insert(new MongoSoftDoc { Id = "m3", Name = "Carol", Age = 50 });

        // Per-document: Remove becomes a flag update.
        Assert.True(await store.Remove<MongoSoftDoc>("m1"));
        Assert.Null(await store.Get<MongoSoftDoc>("m1"));
        Assert.Equal(2, await store.Query<MongoSoftDoc>().Count());
        Assert.Equal(3, await store.Query<MongoSoftDoc>().IncludeDeleted().Count());

        // Set-based: ExecuteDelete becomes ExecuteUpdate over the same predicate.
        Assert.Equal(1, await store.Query<MongoSoftDoc>().Where(x => x.Age >= 50).ExecuteDelete());
        Assert.Equal(1, await store.Query<MongoSoftDoc>().Count());

        // Restore and purge.
        Assert.Equal(1, await store.Restore<MongoSoftDoc>(x => x.Id == "m1"));
        Assert.Equal(2, await store.Query<MongoSoftDoc>().Count());
        Assert.Equal(1, await store.PurgeDeleted<MongoSoftDoc>());
        Assert.Equal(2, await store.Query<MongoSoftDoc>().IncludeDeleted().Count());
    }
}
