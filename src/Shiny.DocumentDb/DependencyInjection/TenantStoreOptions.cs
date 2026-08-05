namespace Shiny.DocumentDb;

/// <summary>
/// What a tenant's store gets when it is built, for the initialization hook
/// (<see cref="TenantStoreOptions.OnTenantStoreCreated"/>).
/// </summary>
/// <param name="TenantId">The tenant the store was built for.</param>
/// <param name="Store">The freshly built store, before it is handed to any caller.</param>
/// <param name="Services">The root container — resolve seeders, migrators or loggers from it.</param>
/// <param name="CancellationToken">Cancels the initialization.</param>
public sealed record TenantStoreInitContext(
    string TenantId,
    IDocumentStore Store,
    IServiceProvider Services,
    CancellationToken CancellationToken
);

/// <summary>
/// Tunes the tenant-per-database store cache built by
/// <c>AddMultiTenantDocumentStore</c>. The cache is bounded: a store that falls out of it is disposed once
/// every in-flight request holding it has finished, so long-lived captured references to
/// <see cref="IDocumentStore"/> are not safe under tenant routing — resolve per scope.
/// </summary>
public sealed class TenantStoreOptions
{
    /// <summary>
    /// Maximum tenant stores held open at once. Resolving one more evicts the least recently used.
    /// Default 100; set to <see cref="int.MaxValue"/> to never evict on size.
    /// </summary>
    public int MaxCachedStores { get; set; } = 100;

    /// <summary>
    /// Evicts a tenant store that has not been resolved for this long. Default 20 minutes;
    /// <c>null</c> disables idle eviction (and the background sweeper with it).
    /// </summary>
    public TimeSpan? IdleTimeout { get; set; } = TimeSpan.FromMinutes(20);

    /// <summary>
    /// Runs exactly once per tenant, after the store is built and <b>before</b> it is handed to the first
    /// caller — seeding, migrations, warmup. A throwing hook fails that resolution and is retried on the next
    /// one (nothing broken is cached). Use <c>SeedFromRegisteredSeeders()</c> for the common case.
    /// </summary>
    public Func<TenantStoreInitContext, Task>? OnTenantStoreCreated { get; set; }

    /// <summary>
    /// Names the tenant's store for telemetry — its result becomes <c>db.namespace</c> on that tenant's spans
    /// and metrics. Defaults to the tenant id, which is what you want at tens of tenants; at thousands that is
    /// unbounded metric cardinality, so bucket it (or return a constant) instead.
    /// <para>
    /// Only applies to the relational convenience overload, which builds the store itself. The
    /// <c>Func&lt;string, IDocumentStore&gt;</c> overload builds its own stores, so name them there.
    /// </para>
    /// </summary>
    public Func<string, string>? StoreNameFactory { get; set; }

    internal void Validate()
    {
        if (this.MaxCachedStores < 1)
            throw new ArgumentOutOfRangeException(nameof(this.MaxCachedStores), this.MaxCachedStores, "At least one tenant store must be cacheable.");

        if (this.IdleTimeout is { } idle && idle <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(this.IdleTimeout), idle, "IdleTimeout must be positive, or null to disable idle eviction.");
    }

    // How often the sweeper looks for idle stores. A quarter of the timeout keeps the eviction lag under
    // 25% of it without waking up constantly on a long timeout.
    internal TimeSpan SweepInterval
    {
        get
        {
            var idle = this.IdleTimeout ?? TimeSpan.FromMinutes(20);
            var quarter = TimeSpan.FromTicks(idle.Ticks / 4);
            return quarter < TimeSpan.FromSeconds(1) ? TimeSpan.FromSeconds(1) : quarter;
        }
    }
}
