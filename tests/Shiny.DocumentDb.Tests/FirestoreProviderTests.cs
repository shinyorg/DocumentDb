using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.Firestore;

[Collection("Firestore")]
public class DocumentStoreTests(FirestoreDatabaseFixture db) : DocumentStoreTestsBase(db);

[Collection("Firestore")]
public class QueryFilterTests(FirestoreDatabaseFixture db) : QueryFilterTestsBase(db);

[Collection("Firestore")]
public class VersionMappingTests(FirestoreDatabaseFixture db) : VersionMappingTestsBase(db);

[Collection("Firestore")]
public class FlagEnumTests(FirestoreDatabaseFixture db) : FlagEnumTestsBase(db);

[Collection("Firestore")]
public class ConcurrentOperationsTests(FirestoreDatabaseFixture db) : ConcurrentOperationsTestsBase(db);

[Collection("Firestore")]
public class FirestoreSpecificTests(FirestoreDatabaseFixture db) : IDisposable
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
    public void UnsupportedCapabilities_AreFalse()
    {
        Assert.False(this.store.SupportsSpatial);
        Assert.False(this.store.SupportsVector);
        Assert.False(this.store.SupportsFullText);
        Assert.False(this.store is ITemporalDocumentStore);
    }

    [Fact]
    public async Task StringQuery_Throws()
        => await Assert.ThrowsAsync<NotSupportedException>(() => this.store.Query<User>("age > 30"));

    [Fact]
    public async Task NativeCursorPaging_WalksAllPages_WithStartAfter()
    {
        var users = Enumerable.Range(1, 25)
            .Select(i => new User { Id = $"u{i:D3}", Name = $"User {i}", Age = i })
            .ToList();
        await this.store.BatchInsert(users);

        var seen = new List<string>();
        string? cursor = null;
        var pages = 0;
        do
        {
            var page = await this.store.Query<User>().OrderBy(u => u.Age).ToCursorPage(cursor, 10);
            seen.AddRange(page.Items.Select(u => u.Id));
            cursor = page.NextCursor;
            pages++;
            Assert.True(pages <= 5, "cursor paging did not terminate");
        }
        while (cursor != null);

        Assert.Equal(25, seen.Count);
        Assert.Equal(25, seen.Distinct().Count());
        // Ordered by Age ascending, so the first id is the youngest.
        Assert.Equal("u001", seen[0]);
    }

    [Fact]
    public async Task NotifyOnChange_ObservesInsert()
    {
        Assert.True(this.store is IObservableDocumentStore);

        var observed = new List<DocumentChange<User>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var consumer = Task.Run(async () =>
        {
            await foreach (var c in ((IObservableDocumentStore)this.store).NotifyOnChange<User>(cts.Token))
            {
                observed.Add(c);
                break;
            }
        });

        await Task.Delay(1500); // let the snapshot listener establish its baseline
        await this.store.Insert(new User { Id = "obs-1", Name = "Observed" });
        await consumer;

        Assert.Single(observed);
        Assert.Equal(DocumentChangeType.Inserted, observed[0].ChangeType);
        Assert.Equal("obs-1", observed[0].Id);
    }

    [Fact]
    public async Task SubscribeChanges_SnapshotListener_ObservesInsert()
    {
        Assert.True(this.store is IChangeFeedDocumentStore);

        var observed = new List<DocumentChange<User>>();
        var gate = new TaskCompletionSource();
        var sub = await ((IChangeFeedDocumentStore)this.store).SubscribeChanges<User>((change, _) =>
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
            await Task.Delay(1500); // let the listener settle past the initial snapshot
            await this.store.Insert(new User { Id = "streamed", Name = "Streamed", Age = 7 });
            await gate.Task.WaitAsync(TimeSpan.FromSeconds(30));
        }

        Assert.Single(observed);
        Assert.Equal("streamed", observed[0].Id);
        Assert.Equal("Streamed", observed[0].Document?.Name);
    }
}
