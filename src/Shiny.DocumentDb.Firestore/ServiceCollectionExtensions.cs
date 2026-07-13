using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shiny.DocumentDb.Firestore;

namespace Shiny.DocumentDb;

public static class FirestoreServiceCollectionExtensions
{
    /// <summary>
    /// Registers a Google Firestore <see cref="IDocumentStore"/> as a singleton, along with
    /// <see cref="IDocumentMaintenance"/> pointing at the same instance.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configures the store options (FirestoreDb or ProjectId, collection/id/version mappings).</param>
    public static IServiceCollection AddFirestoreDocumentStore(
        this IServiceCollection services,
        Action<FirestoreDocumentStoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new FirestoreDocumentStoreOptions();
        configure(options);

        // Factory (not an eager instance) so the SP-taking ctor can wire DI-registered interceptors.
        services.AddSingleton<IDocumentStore>(sp => new FirestoreDocumentStore(options, sp));
        services.TryAddSingleton<IDocumentMaintenance>(sp => (IDocumentMaintenance)sp.GetRequiredService<IDocumentStore>());
        return services;
    }
}
