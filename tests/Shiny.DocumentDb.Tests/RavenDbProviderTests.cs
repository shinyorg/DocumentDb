using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.RavenDb;

[Collection("RavenDB")]
public class DocumentStoreTests(RavenDbDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("RavenDB")]
public class QueryFilterTests(RavenDbDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("RavenDB")]
public class VersionMappingTests(RavenDbDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("RavenDB")]
public class FlagEnumTests(RavenDbDatabaseFixture db) : FlagEnumTestsBase(db);

[Collection("RavenDB")]
public class ConcurrentOperationsTests(RavenDbDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("RavenDB")]
public class RavenDbSpecificTests(RavenDbDatabaseFixture db) : IDisposable
{
    readonly IDocumentStore store = db.CreateStore($"t{Guid.NewGuid():N}");

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public void UnsupportedCapabilities_AreCorrect()
    {
        Assert.False(this.store.SupportsSpatial);
        Assert.False(this.store.SupportsVector);
        Assert.False(this.store.SupportsFullText);
        Assert.False(this.store is ITemporalDocumentStore);
        Assert.False(this.store is IChangeFeedDocumentStore);
        Assert.True(this.store is IObservableDocumentStore);
        Assert.True(this.store is IDocumentMaintenance);
    }

    [Fact]
    public void ToQueryString_ProducesRql_WithPrefixAndPredicate()
    {
        var qs = this.store.Query<User>().Where(u => u.Age > 18).ToQueryString();
        Assert.Contains("from \"RavenDbDocuments\"", qs.Sql);
        Assert.Contains("startsWith(id(), 'User/')", qs.Sql);
        Assert.Contains("age > 18", qs.Sql);
    }

    [Fact]
    public void ToQueryString_IncludesOrderBy()
    {
        var qs = this.store.Query<User>().OrderByDescending(u => u.Age).ToQueryString();
        Assert.Contains("order by age desc", qs.Sql);
    }

    [Fact]
    public async Task StringQuery_OverDocumentFields_Throws()
        => await Assert.ThrowsAsync<NotSupportedException>(() => this.store.Query<User>("Age > 30"));

    [Fact]
    public async Task IntId_AutoGenerates_ViaMaxScan()
    {
        var a = new IntIdModel { Name = "A" };
        var b = new IntIdModel { Name = "B" };
        await this.store.Insert(a);
        await this.store.Insert(b);

        Assert.Equal(1, a.Id);
        Assert.Equal(2, b.Id);
        Assert.Equal(2, await this.store.Count<IntIdModel>());
    }

    [Fact]
    public async Task Query_WhereOrderByPaginate_RunsClientSide()
    {
        for (var i = 0; i < 10; i++)
            await this.store.Insert(new User { Id = $"u{i}", Name = $"User{i}", Age = i });

        var page = await this.store.Query<User>()
            .Where(u => u.Age >= 2)
            .OrderByDescending(u => u.Age)
            .Paginate(1, 3)
            .ToList();

        Assert.Equal(3, page.Count);
        Assert.Equal([8, 7, 6], page.Select(u => u.Age));
    }

    [Fact]
    public async Task NotifyOnChange_ObservesInsert()
    {
        Assert.True(this.store is IObservableDocumentStore);
        var observable = (IObservableDocumentStore)this.store;

        var observed = new List<DocumentChange<User>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var consumer = Task.Run(async () =>
        {
            await foreach (var c in observable.NotifyOnChange<User>(cts.Token))
            {
                observed.Add(c);
                break;
            }
        });

        await Task.Delay(250);
        await this.store.Insert(new User { Id = "obs-1", Name = "Observed" });
        await consumer;

        Assert.Single(observed);
        Assert.Equal(DocumentChangeType.Inserted, observed[0].ChangeType);
        Assert.Equal("obs-1", observed[0].Id);
    }

    [Fact]
    public async Task ExecuteUpdate_And_ExecuteDelete_RunOverFilter()
    {
        for (var i = 0; i < 6; i++)
            await this.store.Insert(new User { Id = $"u{i}", Name = $"User{i}", Age = i });

        var updated = await this.store.Query<User>().Where(u => u.Age < 3).ExecuteUpdate(u => u.Name, "young");
        Assert.Equal(3, updated);
        Assert.Equal("young", (await this.store.Get<User>("u0"))!.Name);

        var deleted = await this.store.Query<User>().Where(u => u.Age >= 3).ExecuteDelete();
        Assert.Equal(3, deleted);
        Assert.Equal(3, await this.store.Count<User>());
    }
}

[Collection("RavenDB")]
public class DocumentQueryConformanceTests(RavenDbDatabaseFixture db) : DocumentQueryConformanceTestsBase(db);

[Collection("RavenDB")]
public class JsonCollectionNotSupportedTests(RavenDbDatabaseFixture db) : JsonCollectionNotSupportedTestsBase(db);

[Collection("RavenDB")]
public class SoftDeleteConformanceTests(RavenDbDatabaseFixture db) : SoftDeleteConformanceTestsBase(db);
