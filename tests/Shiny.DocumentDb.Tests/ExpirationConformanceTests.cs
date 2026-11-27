using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class CacheEntry
{
    public string Id { get; set; } = "";
    public string Value { get; set; } = "";
    public DateTimeOffset? ExpiresAt { get; set; }
}

public class LegacyCacheEntry
{
    public string Id { get; set; } = "";
    public DateTime? ExpiresUtc { get; set; }
}

public sealed class ManualClock(DateTimeOffset start) : TimeProvider
{
    public DateTimeOffset Now { get; set; } = start;
    public override DateTimeOffset GetUtcNow() => this.Now;
    public void Advance(TimeSpan by) => this.Now += by;
}

/// <summary>
/// <c>MapExpiration</c> asserted identically on every provider: expired documents disappear from reads, the
/// default time to live is stamped on insert, and <c>PurgeExpired</c> deletes only what has expired.
/// </summary>
public abstract class ExpirationConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    static readonly DateTimeOffset Start = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    (IDocumentStore Store, ManualClock Clock) NewStore(TimeSpan? defaultTtl = null)
    {
        var clock = new ManualClock(Start);
        var store = this.Fixture.CreateStore($"e{Guid.NewGuid():N}", o =>
        {
            o.ConfigureDocument<CacheEntry>(cfg => cfg.MapExpiration(x => x.ExpiresAt, defaultTtl, clock));
            o.ConfigureDocument<LegacyCacheEntry>(cfg => cfg.MapExpiration(x => x.ExpiresUtc, timeProvider: clock));
        });
        return (store, clock);
    }

    async Task<(IDocumentStore Store, ManualClock Clock)> SeededAsync()
    {
        var (store, clock) = NewStore();
        await store.Insert(new CacheEntry { Id = "forever", Value = "f" });
        await store.Insert(new CacheEntry { Id = "soon", Value = "s", ExpiresAt = Start.AddMinutes(5) });
        await store.Insert(new CacheEntry { Id = "later", Value = "l", ExpiresAt = Start.AddHours(1) });
        return (store, clock);
    }

    [Fact]
    public async Task LiveDocuments_AreVisible()
    {
        var (store, _) = await SeededAsync();
        Assert.Equal(3, await store.Query<CacheEntry>().Count());
        Assert.NotNull(await store.Get<CacheEntry>("soon"));
    }

    [Fact]
    public async Task ExpiredDocuments_AreHiddenFromGetAndQueries()
    {
        var (store, clock) = await SeededAsync();
        clock.Advance(TimeSpan.FromMinutes(10));

        Assert.Null(await store.Get<CacheEntry>("soon"));
        var ids = (await store.Query<CacheEntry>().ToList()).Select(x => x.Id).OrderBy(x => x).ToList();
        Assert.Equal(["forever", "later"], ids);
        Assert.Equal(2, await store.Query<CacheEntry>().Count());
    }

    [Fact]
    public async Task IncludeExpired_SeesPastTheFilter()
    {
        var (store, clock) = await SeededAsync();
        clock.Advance(TimeSpan.FromMinutes(10));

        Assert.Equal(3, await store.Query<CacheEntry>().IncludeExpired().Count());
        Assert.Equal("soon", Assert.Single(await store.Query<CacheEntry>().IncludeExpired().Where(x => x.ExpiresAt <= clock.Now).ToList()).Id);
    }

    [Fact]
    public async Task PurgeExpired_DeletesOnlyExpiredDocuments()
    {
        var (store, clock) = await SeededAsync();
        clock.Advance(TimeSpan.FromMinutes(10));

        Assert.Equal(1, await store.PurgeExpired<CacheEntry>());
        Assert.Equal(2, await store.Query<CacheEntry>().IncludeExpired().Count());

        clock.Advance(TimeSpan.FromHours(2));
        Assert.Equal(1, await store.PurgeExpired<CacheEntry>());
        Assert.Equal("forever", Assert.Single(await store.Query<CacheEntry>().IncludeExpired().ToList()).Id);
    }

    [Fact]
    public async Task DefaultTimeToLive_IsStampedOnInsert_WhenTheExpiryIsNull()
    {
        var (store, clock) = NewStore(TimeSpan.FromMinutes(30));
        var entry = new CacheEntry { Id = "a", Value = "v" };
        await store.Insert(entry);
        await store.Insert(new CacheEntry { Id = "b", Value = "v", ExpiresAt = Start.AddDays(1) });

        Assert.Equal(Start.AddMinutes(30), entry.ExpiresAt);
        Assert.Equal(Start.AddMinutes(30), (await store.Get<CacheEntry>("a"))!.ExpiresAt);

        clock.Advance(TimeSpan.FromHours(1));
        Assert.Null(await store.Get<CacheEntry>("a"));
        Assert.NotNull(await store.Get<CacheEntry>("b"));
    }

    [Fact]
    public async Task DateTimeExpiry_Works()
    {
        var (store, clock) = NewStore();
        await store.Insert(new LegacyCacheEntry { Id = "x", ExpiresUtc = Start.UtcDateTime.AddMinutes(1) });

        Assert.NotNull(await store.Get<LegacyCacheEntry>("x"));
        clock.Advance(TimeSpan.FromMinutes(2));
        Assert.Null(await store.Get<LegacyCacheEntry>("x"));
        Assert.Equal(1, await store.PurgeExpired<LegacyCacheEntry>());
    }

    [Fact]
    public async Task SetBasedWrites_SkipExpiredDocuments()
    {
        var (store, clock) = await SeededAsync();
        clock.Advance(TimeSpan.FromMinutes(10));

        var updated = await store.Query<CacheEntry>().ExecuteUpdate(x => x.Value, "touched");
        Assert.Equal(2, updated);
    }
}

public class ExpirationUnitTests
{
    public class NotNullable
    {
        public string Id { get; set; } = "";
        public DateTimeOffset ExpiresAt { get; set; }
    }

    [Fact]
    public void NonNullableExpiry_IsRejected()
    {
        var options = new DocumentStoreOptions { DatabaseProvider = new Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider("Data Source=:memory:") };
        Assert.Throws<ArgumentException>(() =>
            options.ConfigureDocument<NotNullable>(cfg => cfg.MapExpiration(x => (DateTimeOffset?)x.ExpiresAt)));
    }

    [Fact]
    public void NonPositiveTtl_IsRejected()
    {
        var options = new DocumentStoreOptions { DatabaseProvider = new Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider("Data Source=:memory:") };
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            options.ConfigureDocument<CacheEntry>(cfg => cfg.MapExpiration(x => x.ExpiresAt, TimeSpan.Zero)));
    }

    [Fact]
    public async Task Sweeper_PurgesEveryMappedType()
    {
        var clock = new ManualClock(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var options = new DocumentStoreOptions { DatabaseProvider = new Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider("Data Source=:memory:"), TableName = "sweep" };
        options.ConfigureDocument<CacheEntry>(cfg => cfg.MapExpiration(x => x.ExpiresAt, timeProvider: clock));
        var store = new DocumentStore(options);
        await store.Insert(new CacheEntry { Id = "old", ExpiresAt = clock.Now.AddMinutes(-1) });
        await store.Insert(new CacheEntry { Id = "new", ExpiresAt = clock.Now.AddMinutes(1) });

        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        Microsoft.Extensions.DependencyInjection.ServiceCollectionServiceExtensions.AddSingleton<IDocumentStore>(services, store);
        var provider = Microsoft.Extensions.DependencyInjection.ServiceCollectionContainerBuilderExtensions.BuildServiceProvider(services);
        var sweeper = new DocumentExpirationSweeper(provider, TimeSpan.FromMinutes(1), null);

        await sweeper.SweepAsync(null, CancellationToken.None);

        Assert.Equal("new", Assert.Single(await store.Query<CacheEntry>().IncludeExpired().ToList()).Id);
    }
}
