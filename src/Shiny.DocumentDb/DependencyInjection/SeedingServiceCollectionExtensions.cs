using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Shiny.DocumentDb;

public static class SeedingServiceCollectionExtensions
{
    /// <summary>
    /// Registers an <see cref="IDocumentSeeder"/> that runs once (per <see cref="IDocumentSeeder.Version"/>)
    /// at host startup. The seeder is resolved from the container, so it can take constructor dependencies.
    /// </summary>
    /// <param name="storeName">
    /// The keyed store (registered via <c>AddDocumentStore(name, ...)</c>) to seed, or null for the default store.
    /// </param>
    public static IServiceCollection AddDocumentSeeder<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TSeeder>(
        this IServiceCollection services,
        string? storeName = null)
        where TSeeder : class, IDocumentSeeder
    {
        services.TryAddSingleton<TSeeder>();
        services.AddSingleton(sp => new DocumentSeederRegistration
        {
            StoreName = storeName,
            Seeder = sp.GetRequiredService<TSeeder>()
        });
        AddSeedHostedService(services);
        return services;
    }

    /// <summary>
    /// Registers a pre-built <see cref="IDocumentSeeder"/> instance to run at host startup.
    /// </summary>
    /// <param name="storeName">The keyed store to seed, or null for the default store.</param>
    public static IServiceCollection AddDocumentSeeder(
        this IServiceCollection services,
        IDocumentSeeder seeder,
        string? storeName = null)
    {
        ArgumentNullException.ThrowIfNull(seeder);
        services.AddSingleton(new DocumentSeederRegistration { StoreName = storeName, Seeder = seeder });
        AddSeedHostedService(services);
        return services;
    }

    /// <summary>
    /// Registers an inline seeder to run at host startup. <paramref name="name"/> identifies the run-once
    /// marker; bump <paramref name="version"/> to re-run after changing the seed data.
    /// </summary>
    /// <param name="storeName">The keyed store to seed, or null for the default store.</param>
    public static IServiceCollection AddDocumentSeeder(
        this IServiceCollection services,
        string name,
        int version,
        Func<IDocumentStore, CancellationToken, Task> seed,
        string? storeName = null)
        => services.AddDocumentSeeder(new DelegateDocumentSeeder(name, version, seed), storeName);

    static void AddSeedHostedService(IServiceCollection services)
        => services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, DocumentInitializationHostedService>());
}
