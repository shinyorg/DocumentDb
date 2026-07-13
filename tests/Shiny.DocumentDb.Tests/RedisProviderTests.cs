using Shiny.DocumentDb.Redis;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.Redis;

[Collection("Redis")]
public class DocumentStoreTests(RedisDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("Redis")]
public class QueryFilterTests(RedisDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("Redis")]
public class VersionMappingTests(RedisDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("Redis")]
public class FlagEnumTests(RedisDatabaseFixture db) : FlagEnumTestsBase(db);

[Collection("Redis")]
public class ConcurrentOperationsTests(RedisDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("Redis")]
public class RedisSpecificTests(RedisDatabaseFixture db) : IDisposable
{
    readonly IDocumentStore store = db.CreateStore($"t{Guid.NewGuid():N}");

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public async Task IntId_AutoGen_Succeeds_ViaIncr()
    {
        // Redis is a differentiator: Int/Long ids auto-generate via a per-type INCR counter
        // (every other NoSQL provider throws NotSupportedException here).
        var doc = new IntIdModel { Name = "incr-a" };
        await this.store.Insert(doc);
        Assert.True(doc.Id > 0);

        var doc2 = new IntIdModel { Name = "incr-b" };
        await this.store.Insert(doc2);
        Assert.True(doc2.Id > doc.Id);

        var fetched = await this.store.Get<IntIdModel>(doc.Id);
        Assert.NotNull(fetched);
        Assert.Equal("incr-a", fetched!.Name);
    }

    [Fact]
    public async Task LongId_AutoGen_Succeeds_ViaIncr()
    {
        var doc = new LongIdModel { Name = "incr-long" };
        await this.store.Insert(doc);
        Assert.True(doc.Id > 0);
    }

    [Fact]
    public void ModuleProbe_Capabilities_ReflectMappings()
    {
        // A plain store maps nothing → the rich-search capabilities are all off.
        Assert.False(this.store.SupportsFullText);
        Assert.False(this.store.SupportsVector);
        Assert.False(this.store.SupportsSpatial);
    }

    [Fact]
    public async Task ServerSideGroupBy_ViaFtAggregate()
    {
        // The Redis differentiator: GROUPBY + REDUCE COUNT/SUM push down to FT.AGGREGATE server-side.
        using var groupStore = (IDisposable)db.CreateGroupByStore($"t{Guid.NewGuid():N}");
        var s = (RedisDocumentStore)groupStore;

        await s.Insert(new User { Id = "u1", Name = "Ada", Age = 30 });
        await s.Insert(new User { Id = "u2", Name = "Ada", Age = 40 });
        await s.Insert(new User { Id = "u3", Name = "Bob", Age = 25 });

        var rows = await s.ServerSideGroupByAsync<User>(u => u.Name, u => u.Age, CancellationToken.None);
        var byName = rows.ToDictionary(r => r.Key, r => (r.Count, r.Sum));

        Assert.Equal(2, byName["Ada"].Count);
        Assert.Equal(70, byName["Ada"].Sum);
        Assert.Equal(1, byName["Bob"].Count);
        Assert.Equal(25, byName["Bob"].Sum);
    }

    [Fact]
    public async Task IndexedPredicate_PushesDown_ToQueryString()
    {
        using var idxStore = (IDisposable)db.CreateStoreWithIndexed<User>($"t{Guid.NewGuid():N}", u => u.Age);
        var s = (IDocumentStore)idxStore;
        await s.Insert(new User { Id = "a", Name = "A", Age = 20 });
        await s.Insert(new User { Id = "b", Name = "B", Age = 40 });
        await s.Insert(new User { Id = "c", Name = "C", Age = 60 });

        // Age is a NUMERIC index field → the predicate lowers to an FT.SEARCH range query.
        var qs = s.Query<User>().Where(u => u.Age > 30).ToQueryString();
        Assert.Contains("@age:[(30 +inf]", qs.Sql);

        var over30 = await s.Query<User>().Where(u => u.Age > 30).OrderBy(u => u.Age).ToList();
        Assert.Equal(["B", "C"], over30.Select(u => u.Name));
    }
}
