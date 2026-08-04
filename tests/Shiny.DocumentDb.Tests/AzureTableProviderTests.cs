using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.AzureTable;

[Collection("AzureTable")]
public class DocumentStoreTests(AzureTableDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("AzureTable")]
public class QueryFilterTests(AzureTableDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("AzureTable")]
public class VersionMappingTests(AzureTableDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("AzureTable")]
public class FlagEnumTests(AzureTableDatabaseFixture db) : FlagEnumTestsBase(db);

[Collection("AzureTable")]
public class ConcurrentOperationsTests(AzureTableDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("AzureTable")]
public class AzureTableSpecificTests(AzureTableDatabaseFixture db) : IDisposable
{
    readonly IDocumentStore store = db.CreateStore($"t{Guid.NewGuid():N}");

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public async Task IntId_AutoGen_Throws_SteeringToGuidOrString()
    {
        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            () => this.store.Insert(new IntIdModel { Name = "no-id" }));
        Assert.Contains("Guid or string", ex.Message);
    }

    [Fact]
    public async Task LongId_AutoGen_Throws()
        => await Assert.ThrowsAsync<NotSupportedException>(
            () => this.store.Insert(new LongIdModel { Name = "no-id" }));

    [Fact]
    public async Task IntId_Explicit_RoundTrips()
    {
        await this.store.Insert(new IntIdModel { Id = 42, Name = "explicit" });
        var got = await this.store.Get<IntIdModel>(42);
        Assert.NotNull(got);
        Assert.Equal("explicit", got.Name);
    }

    [Fact]
    public async Task BatchInsert_AcrossTransactionChunkBoundary()
    {
        // 100 is the Table transaction cap — 250 exercises multi-chunk waves.
        var users = Enumerable.Range(1, 250)
            .Select(i => new User { Id = $"u{i:D4}", Name = $"User {i}", Age = i % 40 })
            .ToList();

        var count = await this.store.BatchInsert(users);

        Assert.Equal(250, count);
        Assert.Equal(250, await this.store.Count<User>());
    }

    [Fact]
    public async Task OversizedBody_Throws_ClearError()
    {
        var big = new User { Id = "big", Name = new string('x', 40 * 1024) };
        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => this.store.Insert(big));
        Assert.Contains("64 KB", ex.Message);
    }

    [Fact]
    public void UnsupportedCapabilities_AreFalse()
    {
        Assert.False(this.store.SupportsSpatial);
        Assert.False(this.store.SupportsVector);
        Assert.False(this.store.SupportsFullText);
        Assert.False(this.store is ITemporalDocumentStore);
    }
}

[Collection("AzureTable")]
public class AzureTablePromotedTests(AzureTableDatabaseFixture db)
{
    [Fact]
    public async Task PromotedColumn_LinqPushdown_ReturnsCorrectResults()
    {
        using var store = (IDisposable)db.CreateStoreWithIndexed<User>($"t{Guid.NewGuid():N}", u => u.Age);
        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "a", Name = "A", Age = 20 });
        await s.Insert(new User { Id = "b", Name = "B", Age = 40 });
        await s.Insert(new User { Id = "c", Name = "C", Age = 60 });

        // Age is promoted → the predicate is pushed to the OData $filter; the full predicate still runs client-side.
        var over30 = await s.Query<User>().Where(u => u.Age > 30).OrderBy(u => u.Age).ToList();
        Assert.Equal(["B", "C"], over30.Select(u => u.Name));
    }

    [Fact]
    public async Task StringQuery_OverPromotedColumn()
    {
        using var store = (IDisposable)db.CreateStoreWithIndexed<User>($"t{Guid.NewGuid():N}", u => u.Age);
        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "a", Name = "A", Age = 20 });
        await s.Insert(new User { Id = "b", Name = "B", Age = 40 });

        var result = await s.Query<User>("Age gt 30");
        Assert.Single(result);
        Assert.Equal("B", result[0].Name);

        var count = await s.Count<User>("Age gt 30");
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task ToQueryString_IncludesPartitionAndPushdown()
    {
        using var store = (IDisposable)db.CreateStoreWithIndexed<User>($"t{Guid.NewGuid():N}", u => u.Age);
        var s = (IDocumentStore)store;
        var qs = s.Query<User>().Where(u => u.Age > 18).ToQueryString();
        Assert.Contains("PartitionKey eq", qs.Sql);
        Assert.Contains("idx_age gt 18", qs.Sql);
    }

    [Fact]
    public async Task NotifyOnChange_ObservesInsert()
    {
        using var store = (IDisposable)db.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;
        Assert.True(s is IObservableDocumentStore);

        var observed = new List<DocumentChange<User>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var consumer = Task.Run(async () =>
        {
            await foreach (var c in ((IObservableDocumentStore)s).NotifyOnChange<User>(cts.Token))
            {
                observed.Add(c);
                break;
            }
        });

        await Task.Delay(200);
        await s.Insert(new User { Id = "obs-1", Name = "Observed" });
        await consumer;

        Assert.Single(observed);
        Assert.Equal(DocumentChangeType.Inserted, observed[0].ChangeType);
        Assert.Equal("obs-1", observed[0].Id);
    }
}

[Collection("AzureTable")]
public class DocumentQueryConformanceTests(AzureTableDatabaseFixture db) : DocumentQueryConformanceTestsBase(db);

[Collection("AzureTable")]
public class JsonCollectionNotSupportedTests(AzureTableDatabaseFixture db) : JsonCollectionNotSupportedTestsBase(db);

[Collection("AzureTable")]
public class SoftDeleteConformanceTests(AzureTableDatabaseFixture db) : SoftDeleteConformanceTestsBase(db);

[Collection("AzureTable")]
public class OutboxUnsupportedTests(AzureTableDatabaseFixture db) : OutboxUnsupportedTestsBase(db);
