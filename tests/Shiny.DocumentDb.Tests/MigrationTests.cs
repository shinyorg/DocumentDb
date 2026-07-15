using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class MigrationTests : IDisposable
{
    readonly SqliteDocumentStore store;
    readonly string path;

    public MigrationTests()
    {
        this.path = Path.Combine(Path.GetTempPath(), $"migrate_{Guid.NewGuid():N}.db");
        this.store = new SqliteDocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={this.path}")
        });
    }

    public void Dispose()
    {
        this.store.Dispose();
        if (File.Exists(this.path))
            File.Delete(this.path);
    }

    sealed class RecordingMigration(long version, string name, List<string> order) : IDocumentMigration
    {
        public long Version { get; } = version;
        public string Name { get; } = name;

        public Task Up(IDocumentStore store, CancellationToken cancellationToken)
        {
            order.Add(this.Name);
            return store.Upsert(
                new User { Id = this.Name, Name = this.Name, Age = (int)(this.Version % 1000) },
                cancellationToken: cancellationToken);
        }

        public Task Down(IDocumentStore store, CancellationToken cancellationToken)
            => store.Remove<User>(this.Name, cancellationToken);
    }

    [Fact]
    public async Task RunsInAscendingVersionOrderAndRecordsLedger()
    {
        var order = new List<string>();
        var applied = await DocumentMigrationRunner.RunAsync(this.store,
        [
            new RecordingMigration(20260101000000, "b", order),
            new RecordingMigration(20250101000000, "a", order)
        ]);

        Assert.Equal(["a", "b"], order);
        Assert.Equal(["a", "b"], applied);

        var ledger = await DocumentMigrationRunner.GetAppliedAsync(this.store);
        Assert.Equal([20250101000000, 20260101000000], ledger.Select(r => r.Version));
        Assert.Equal(20260101000000, await DocumentMigrationRunner.GetCurrentVersionAsync(this.store));
    }

    [Fact]
    public async Task DoesNotReRunAppliedMigration()
    {
        var order = new List<string>();
        var m = new RecordingMigration(20250101000000, "a", order);
        await DocumentMigrationRunner.RunAsync(this.store, [m]);

        var second = await DocumentMigrationRunner.RunAsync(this.store, [m]);

        Assert.Empty(second);
        Assert.Single(order);
    }

    [Fact]
    public async Task LaterAddedLowerVersionStillApplies()
    {
        // Membership (not high-water-mark) decides — a migration authored out of order still runs.
        var order = new List<string>();
        await DocumentMigrationRunner.RunAsync(this.store,
            [new RecordingMigration(20260101000000, "late-high", order)]);

        var applied = await DocumentMigrationRunner.RunAsync(this.store,
        [
            new RecordingMigration(20260101000000, "late-high", order),
            new RecordingMigration(20250101000000, "earlier-low", order) // lower version, added afterwards
        ]);

        Assert.Equal(["earlier-low"], applied);
        var ledger = await DocumentMigrationRunner.GetAppliedAsync(this.store);
        Assert.Equal(2, ledger.Count);
    }

    [Fact]
    public async Task FailedMigrationLeavesNoRecordAndReRuns()
    {
        var attempts = 0;
        IDocumentMigration flaky = new DelegateDocumentMigration(20250101000000, "flaky", (s, ct) =>
        {
            attempts++;
            if (attempts == 1)
                throw new InvalidOperationException("boom");
            return s.Upsert(new User { Id = "u", Name = "ok" }, cancellationToken: ct);
        });

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => DocumentMigrationRunner.RunAsync(this.store, [flaky]));
        Assert.Equal(0, await DocumentMigrationRunner.GetCurrentVersionAsync(this.store));

        var applied = await DocumentMigrationRunner.RunAsync(this.store, [flaky]);
        Assert.Equal(["flaky"], applied);
        Assert.Equal(2, attempts);
        Assert.Equal(20250101000000, await DocumentMigrationRunner.GetCurrentVersionAsync(this.store));
    }

    [Fact]
    public async Task RollbackRevertsNewerMigrationsInDescendingOrder()
    {
        var order = new List<string>();
        IDocumentMigration[] migrations =
        [
            new RecordingMigration(20250101000000, "a", order),
            new RecordingMigration(20260101000000, "b", order),
            new RecordingMigration(20270101000000, "c", order)
        ];
        await DocumentMigrationRunner.RunAsync(this.store, migrations);

        var downOrder = new List<string>();
        var trackingMigrations = migrations.Select(m => (IDocumentMigration)new TrackingDown(m, downOrder)).ToArray();

        var reverted = await DocumentMigrationRunner.RollbackToAsync(this.store, trackingMigrations, 20250101000000);

        Assert.Equal(["c", "b"], reverted);
        Assert.Equal(["c", "b"], downOrder);
        Assert.Equal(20250101000000, await DocumentMigrationRunner.GetCurrentVersionAsync(this.store));
        Assert.Equal(1, await this.store.Count<User>()); // only "a" remains
    }

    [Fact]
    public async Task RollbackWithoutDownThrows()
    {
        var m = new DelegateDocumentMigration(20250101000000, "no-down",
            (s, ct) => s.Upsert(new User { Id = "u", Name = "x" }, cancellationToken: ct));
        await DocumentMigrationRunner.RunAsync(this.store, [m]);

        await Assert.ThrowsAsync<NotSupportedException>(
            () => DocumentMigrationRunner.RollbackToAsync(this.store, [m], 0));
    }

    // wraps a migration so Down records call order without altering Up behaviour
    sealed class TrackingDown(IDocumentMigration inner, List<string> downOrder) : IDocumentMigration
    {
        public long Version => inner.Version;
        public string Name => inner.Name;
        public Task Up(IDocumentStore store, CancellationToken ct) => inner.Up(store, ct);
        public Task Down(IDocumentStore store, CancellationToken ct)
        {
            downOrder.Add(this.Name);
            return inner.Down(store, ct);
        }
    }
}
