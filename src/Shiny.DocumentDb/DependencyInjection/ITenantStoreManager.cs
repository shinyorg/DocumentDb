namespace Shiny.DocumentDb;

/// <summary>
/// Operational control over the tenant-per-database store cache registered by
/// <c>AddMultiTenantDocumentStore</c>. Resolve it from DI to warm a tenant ahead of its first request, to
/// close one down at offboarding, or to see what is currently held open.
/// </summary>
public interface ITenantStoreManager
{
    /// <summary>A snapshot of the tenants whose stores are currently cached.</summary>
    IReadOnlyCollection<string> ActiveTenants { get; }

    /// <summary>
    /// Builds and initializes a tenant's store ahead of its first request, so that request does not pay for
    /// creation (and, with <see cref="TenantStoreOptions.OnTenantStoreCreated"/>, seeding). Idempotent.
    /// </summary>
    Task WarmAsync(string tenantId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes a tenant's store and drops it from the cache, waiting for any in-flight request holding it to
    /// finish first. Idempotent — evicting an unknown or already-evicted tenant does nothing. A later
    /// resolution simply builds the store again.
    /// </summary>
    Task EvictAsync(string tenantId, CancellationToken cancellationToken = default);
}
