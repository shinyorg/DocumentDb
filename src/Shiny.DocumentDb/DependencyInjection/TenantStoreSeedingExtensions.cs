using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb;

/// <summary>
/// Bridges seeding into tenant-per-database routing. Startup seeding runs against the registered store once,
/// which a tenant whose database is first touched at 3pm never sees — so seeders run per tenant, when that
/// tenant's store is built.
/// </summary>
public static class TenantStoreSeedingExtensions
{
    /// <summary>
    /// Runs every seeder registered with <c>AddDocumentSeeder</c> against each tenant's store the first time
    /// it is built, with the same versioned run-once semantics as startup seeding (each tenant's database
    /// carries its own marker). Composes with an existing
    /// <see cref="TenantStoreOptions.OnTenantStoreCreated"/> hook, which runs first.
    /// </summary>
    public static TenantStoreOptions SeedFromRegisteredSeeders(this TenantStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var previous = options.OnTenantStoreCreated;
        options.OnTenantStoreCreated = async ctx =>
        {
            if (previous != null)
                await previous(ctx).ConfigureAwait(false);

            var seeders = ctx.Services
                .GetServices<DocumentSeederRegistration>()
                .Where(x => x.StoreName == null)      // keyed stores are not tenant-routed
                .Select(x => x.Seeder)
                .ToList();

            if (seeders.Count > 0)
            {
                await DocumentSeedRunner
                    .RunAsync(ctx.Store, seeders, cancellationToken: ctx.CancellationToken)
                    .ConfigureAwait(false);
            }
        };
        return options;
    }
}
