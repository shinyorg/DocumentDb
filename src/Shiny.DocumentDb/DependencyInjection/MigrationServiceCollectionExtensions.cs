using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Shiny.DocumentDb;

public static class MigrationServiceCollectionExtensions
{
    /// <summary>
    /// Registers an <see cref="IDocumentMigration"/> that runs once (per <see cref="IDocumentMigration.Version"/>)
    /// at host startup, before any seeders. The migration is resolved from the container, so it can take
    /// constructor dependencies.
    /// </summary>
    /// <param name="storeName">
    /// The keyed store (registered via <c>AddDocumentStore(name, ...)</c>) to migrate, or null for the default store.
    /// </param>
    public static IServiceCollection AddDocumentMigration<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TMigration>(
        this IServiceCollection services,
        string? storeName = null)
        where TMigration : class, IDocumentMigration
    {
        services.TryAddSingleton<TMigration>();
        services.AddSingleton(sp => new DocumentMigrationRegistration
        {
            StoreName = storeName,
            Migration = sp.GetRequiredService<TMigration>()
        });
        AddMigrationInfrastructure(services, storeName);
        return services;
    }

    /// <summary>
    /// Registers a pre-built <see cref="IDocumentMigration"/> instance to run at host startup, before seeders.
    /// </summary>
    /// <param name="storeName">The keyed store to migrate, or null for the default store.</param>
    public static IServiceCollection AddDocumentMigration(
        this IServiceCollection services,
        IDocumentMigration migration,
        string? storeName = null)
    {
        ArgumentNullException.ThrowIfNull(migration);
        services.AddSingleton(new DocumentMigrationRegistration { StoreName = storeName, Migration = migration });
        AddMigrationInfrastructure(services, storeName);
        return services;
    }

    /// <summary>
    /// Registers an inline migration to run at host startup, before seeders. <paramref name="version"/>
    /// (an EF-style UTC timestamp such as <c>20260714093000</c>) is the run-once identity; pass
    /// <paramref name="down"/> to make it reversible.
    /// </summary>
    /// <param name="storeName">The keyed store to migrate, or null for the default store.</param>
    public static IServiceCollection AddDocumentMigration(
        this IServiceCollection services,
        long version,
        string name,
        Func<IDocumentStore, CancellationToken, Task> up,
        Func<IDocumentStore, CancellationToken, Task>? down = null,
        string? storeName = null)
        => services.AddDocumentMigration(new DelegateDocumentMigration(version, name, up, down), storeName);

    static void AddMigrationInfrastructure(IServiceCollection services, string? storeName)
    {
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, DocumentInitializationHostedService>());

        if (storeName == null)
        {
            services.TryAddSingleton<IDocumentMigrator>(sp => new DocumentMigrator(
                sp.GetRequiredService<IDocumentStore>(),
                MigrationsFor(sp, null)));
        }
        else
        {
            services.TryAddKeyedSingleton<IDocumentMigrator>(storeName, (sp, key) => new DocumentMigrator(
                sp.GetRequiredKeyedService<IDocumentStore>(key),
                MigrationsFor(sp, (string)key!)));
        }
    }

    static IEnumerable<IDocumentMigration> MigrationsFor(IServiceProvider sp, string? storeName)
        => sp.GetServices<DocumentMigrationRegistration>()
            .Where(r => r.StoreName == storeName)
            .Select(r => r.Migration);
}
