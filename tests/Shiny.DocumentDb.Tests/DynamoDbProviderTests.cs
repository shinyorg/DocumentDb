using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.DynamoDb;

[Collection("DynamoDB")]
public class DocumentStoreTests(DynamoDbDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("DynamoDB")]
public class QueryFilterTests(DynamoDbDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("DynamoDB")]
public class VersionMappingTests(DynamoDbDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("DynamoDB")]
public class FlagEnumTests(DynamoDbDatabaseFixture db) : FlagEnumTestsBase(db);

[Collection("DynamoDB")]
public class ConcurrentOperationsTests(DynamoDbDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("DynamoDB")]
public class DynamoDbSpecificTests(DynamoDbDatabaseFixture db) : IDisposable
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
    public async Task BatchInsert_AcrossBatchWriteChunkBoundary()
    {
        // 25 is the BatchWriteItem cap — 80 exercises multi-chunk waves.
        var users = Enumerable.Range(1, 80)
            .Select(i => new User { Id = $"u{i:D4}", Name = $"User {i}", Age = i % 40 })
            .ToList();

        var count = await this.store.BatchInsert(users);

        Assert.Equal(80, count);
        Assert.Equal(80, await this.store.Count<User>());
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

[Collection("DynamoDB")]
public class DynamoDbPromotedTests(DynamoDbDatabaseFixture db)
{
    [Fact]
    public async Task PromotedAttribute_LinqPushdown_ReturnsCorrectResults()
    {
        using var store = (IDisposable)db.CreateStoreWithIndexed<User>($"t{Guid.NewGuid():N}", u => u.Age);
        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "a", Name = "A", Age = 20 });
        await s.Insert(new User { Id = "b", Name = "B", Age = 40 });
        await s.Insert(new User { Id = "c", Name = "C", Age = 60 });

        // Age is promoted → the predicate is pushed to a FilterExpression; the full predicate still runs client-side.
        var over30 = await s.Query<User>().Where(u => u.Age > 30).OrderBy(u => u.Age).ToList();
        Assert.Equal(["B", "C"], over30.Select(u => u.Name));
    }

    [Fact]
    public async Task StringQuery_PartiQL_OverPromotedAttribute()
    {
        using var store = (IDisposable)db.CreateStoreWithIndexed<User>($"t{Guid.NewGuid():N}", u => u.Age);
        var s = (IDocumentStore)store;
        await s.Insert(new User { Id = "a", Name = "A", Age = 20 });
        await s.Insert(new User { Id = "b", Name = "B", Age = 40 });

        var result = await s.Query<User>("Age > 30");
        Assert.Single(result);
        Assert.Equal("B", result[0].Name);

        var count = await s.Count<User>("Age > 30");
        Assert.Equal(1, count);
    }

    [Fact]
    public void ToQueryString_IncludesPartitionAndPushdown()
    {
        using var store = (IDisposable)db.CreateStoreWithIndexed<User>($"t{Guid.NewGuid():N}", u => u.Age);
        var s = (IDocumentStore)store;
        var qs = s.Query<User>().Where(u => u.Age > 18).ToQueryString();
        Assert.Contains("pk =", qs.Sql);
        // The promoted attribute name is carried in the ExpressionAttributeNames placeholder map.
        Assert.Contains("idx_age", qs.Parameters.Values.Select(v => v?.ToString()));
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
    }

    [Fact]
    public async Task SubscribeChanges_Streams_ObservesInsert()
    {
        using var store = (IDisposable)db.CreateStore($"t{Guid.NewGuid():N}");
        var s = (IDocumentStore)store;
        Assert.True(s is IChangeFeedDocumentStore);

        // Ensure the table (and its stream) exists before subscribing.
        await s.Insert(new User { Id = "seed", Name = "Seed" });

        var observed = new List<DocumentChange<User>>();
        var gate = new TaskCompletionSource();
        var sub = await ((IChangeFeedDocumentStore)s).SubscribeChanges<User>((change, _) =>
        {
            if (change.Id == "streamed")
            {
                observed.Add(change);
                gate.TrySetResult();
            }
            return Task.CompletedTask;
        });

        await using (sub)
        {
            await Task.Delay(500); // let the shard iterator settle on LATEST
            await s.Insert(new User { Id = "streamed", Name = "Streamed", Age = 7 });
            await gate.Task.WaitAsync(TimeSpan.FromSeconds(30));
        }

        Assert.Single(observed);
        Assert.Equal("streamed", observed[0].Id);
        Assert.Equal("Streamed", observed[0].Document?.Name);
    }
}

[Collection("DynamoDB")]
public class DocumentQueryConformanceTests(DynamoDbDatabaseFixture db) : DocumentQueryConformanceTestsBase(db);

[Collection("DynamoDB")]
public class SoftDeleteConformanceTests(DynamoDbDatabaseFixture db) : SoftDeleteConformanceTestsBase(db);
