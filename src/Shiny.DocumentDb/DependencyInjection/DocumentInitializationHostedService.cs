using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Shiny.DocumentDb;

/// <summary>
/// Runs registered data initialization once at host startup, grouped by target store: pending
/// <see cref="IDocumentMigration"/>s first (so existing data is at the current shape), then
/// <see cref="IDocumentSeeder"/>s. Registered automatically by <c>AddDocumentMigration</c> and
/// <c>AddDocumentSeeder</c> (deduped via <c>TryAddEnumerable</c>). Hosts without a generic host (e.g.
/// MAUI) should call <see cref="DocumentMigrationRunner.RunAsync"/> and
/// <see cref="DocumentSeedRunner.RunAsync"/> directly instead.
/// </summary>
sealed class DocumentInitializationHostedService : IHostedService
{
    readonly IServiceProvider services;

    public DocumentInitializationHostedService(IServiceProvider services) => this.services = services;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // A scope lets us resolve a scoped IDocumentStore (the tenant-per-database registration).
        await using var scope = this.services.CreateAsyncScope();
        var sp = scope.ServiceProvider;

        var migrationRegistrations = sp.GetServices<DocumentMigrationRegistration>();
        var seederRegistrations = sp.GetServices<DocumentSeederRegistration>();

        // Every store referenced by either a migration or a seeder, so ordering holds even when only
        // one of the two is registered against a given store.
        var storeNames = migrationRegistrations
            .Select(r => r.StoreName)
            .Concat(seederRegistrations.Select(r => r.StoreName))
            .Distinct();

        foreach (var storeName in storeNames)
        {
            var store = storeName == null
                ? sp.GetRequiredService<IDocumentStore>()
                : sp.GetRequiredKeyedService<IDocumentStore>(storeName);

            var migrations = migrationRegistrations
                .Where(r => r.StoreName == storeName)
                .Select(r => r.Migration)
                .ToList();
            if (migrations.Count > 0)
            {
                await DocumentMigrationRunner
                    .RunAsync(store, migrations, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }

            var seeders = seederRegistrations
                .Where(r => r.StoreName == storeName)
                .Select(r => r.Seeder)
                .ToList();
            if (seeders.Count > 0)
            {
                await DocumentSeedRunner
                    .RunAsync(store, seeders, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
