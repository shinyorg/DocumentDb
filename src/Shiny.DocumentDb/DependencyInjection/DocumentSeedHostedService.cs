using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Shiny.DocumentDb;

/// <summary>
/// Runs all registered seeders once at host startup, grouped by their target store. Registered
/// automatically by <c>AddDocumentSeeder</c>. Hosts without a generic host (e.g. MAUI) should call
/// <see cref="DocumentSeedRunner.RunAsync"/> directly instead.
/// </summary>
sealed class DocumentSeedHostedService : IHostedService
{
    readonly IServiceProvider services;

    public DocumentSeedHostedService(IServiceProvider services) => this.services = services;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // A scope lets us resolve a scoped IDocumentStore (the tenant-per-database registration).
        await using var scope = this.services.CreateAsyncScope();
        var registrations = scope.ServiceProvider.GetServices<DocumentSeederRegistration>();

        foreach (var group in registrations.GroupBy(r => r.StoreName))
        {
            var store = group.Key == null
                ? scope.ServiceProvider.GetRequiredService<IDocumentStore>()
                : scope.ServiceProvider.GetRequiredKeyedService<IDocumentStore>(group.Key);

            await DocumentSeedRunner
                .RunAsync(store, group.Select(r => r.Seeder), cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
