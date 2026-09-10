using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb.Aspire.Hosting;
using Shiny.DocumentDb.Aspire.Hosting.Internal;

namespace Shiny.DocumentDb.Aspire.Tests;

/// <summary>
/// The seed gate runs on exactly two triggers — first-time setup and destructive recreation — and it
/// decides which by reading a marker kept inside the database. SQLite is the honest test bed: the whole
/// database is one file, so "somebody destroyed the data" is a File.Delete.
/// </summary>
public class SeedGateTests : IDisposable
{
    readonly string dbPath = Path.Combine(Path.GetTempPath(), $"seedgate-{Guid.NewGuid():N}.db");

    string ConnectionString => $"Data Source={this.dbPath}";

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (File.Exists(this.dbPath))
            File.Delete(this.dbPath);
    }

    async Task<List<bool>> RunAsync(
        DocumentStoreSeedMode mode,
        int times,
        string storeName = "orders",
        Func<DocumentStoreSeedContext, Task>? onSeed = null)
    {
        // Each entry is one invocation of the callback; the value is that run's Recreate flag.
        var invocations = new List<bool>();

        for (var i = 0; i < times; i++)
        {
            await DocumentStoreSeedGate.RunAsync(
                storeName,
                DocumentProviderKind.Sqlite,
                this.ConnectionString,
                mode,
                async (ctx, _) =>
                {
                    invocations.Add(ctx.Recreate);
                    if (onSeed is not null)
                        await onSeed(ctx);
                },
                NullLogger.Instance,
                TestContext.Current.CancellationToken
            );
        }

        return invocations;
    }

    [Fact]
    public async Task FirstTimeOnly_SeedsOnceThenSkipsForever()
    {
        var runs = await this.RunAsync(DocumentStoreSeedMode.FirstTimeOnly, times: 3);

        Assert.Equal([false], runs);
    }

    [Fact]
    public async Task Recreate_SeedsEveryStartAndFlagsTheRebuilds()
    {
        var runs = await this.RunAsync(DocumentStoreSeedMode.Recreate, times: 3);

        // First start is a first-time setup (nothing to wipe); the rest are destructive recreations.
        Assert.Equal([false, true, true], runs);
    }

    [Fact]
    public async Task DestroyingTheDatabase_MakesTheNextStartAFirstTimeSetupAgain()
    {
        var first = await this.RunAsync(DocumentStoreSeedMode.FirstTimeOnly, times: 2);
        Assert.Equal([false], first);

        // The marker lives in the data, so wiping the data (volume drop / file delete) wipes the marker.
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        File.Delete(this.dbPath);

        var second = await this.RunAsync(DocumentStoreSeedMode.FirstTimeOnly, times: 1);
        Assert.Equal([false], second);
    }

    [Fact]
    public async Task AFailedSeed_LeavesNoMarker_SoTheNextStartRetries()
    {
        var attempts = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(() => this.RunAsync(
            DocumentStoreSeedMode.FirstTimeOnly,
            times: 1,
            onSeed: _ =>
            {
                attempts++;
                throw new InvalidOperationException("seed blew up");
            }
        ));

        var retry = await this.RunAsync(DocumentStoreSeedMode.FirstTimeOnly, times: 1);

        Assert.Equal(1, attempts);
        Assert.Equal([false], retry);
    }

    [Fact]
    public async Task MarkersAreScopedPerStoreName()
    {
        var orders = await this.RunAsync(DocumentStoreSeedMode.FirstTimeOnly, times: 2, storeName: "orders");
        var catalog = await this.RunAsync(DocumentStoreSeedMode.FirstTimeOnly, times: 2, storeName: "catalog");

        // Two stores sharing one database each get their own first-time setup.
        Assert.Equal([false], orders);
        Assert.Equal([false], catalog);
    }

    [Fact]
    public async Task TheCallbackReceivesTheStoreIdentityAndConnectionString()
    {
        DocumentStoreSeedContext? captured = null;

        await this.RunAsync(
            DocumentStoreSeedMode.FirstTimeOnly,
            times: 1,
            onSeed: ctx =>
            {
                captured = ctx;
                return Task.CompletedTask;
            }
        );

        Assert.NotNull(captured);
        Assert.Equal("orders", captured.StoreName);
        Assert.Equal(DocumentProviderKind.Sqlite, captured.Provider);
        Assert.Equal(this.ConnectionString, captured.ConnectionString);
    }

    [Fact]
    public async Task ReadMarker_ReturnsTheTimestampWrittenAfterASuccessfulSeed()
    {
        var marker = new SeedMarkerStore(DocumentProviderKind.Sqlite, this.ConnectionString);
        var ct = TestContext.Current.CancellationToken;

        Assert.Null(await marker.ReadMarkerAsync("orders", ct));

        await this.RunAsync(DocumentStoreSeedMode.FirstTimeOnly, times: 1);

        var seededAt = await marker.ReadMarkerAsync("orders", ct);
        Assert.NotNull(seededAt);
        Assert.True(DateTimeOffset.TryParse(seededAt, out _));
    }
}
