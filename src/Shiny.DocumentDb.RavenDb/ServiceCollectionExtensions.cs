using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shiny.DocumentDb.RavenDb;

namespace Shiny.DocumentDb;

public static class RavenDbServiceCollectionExtensions
{
    /// <summary>
    /// Registers a RavenDB <see cref="IDocumentStore"/> as a singleton, along with
    /// <see cref="IDocumentMaintenance"/> pointing at the same instance.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configures the store options (RavenDB store / URLs + database + certificate, mappings).</param>
    public static IServiceCollection AddRavenDbDocumentStore(
        this IServiceCollection services,
        Action<RavenDbDocumentStoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new RavenDbDocumentStoreOptions();
        configure(options);

        // Factory (not an eager instance) so the SP-taking ctor can wire DI-registered interceptors.
        services.AddSingleton<IDocumentStore>(sp => new RavenDbDocumentStore(options, sp));
        services.TryAddSingleton<IDocumentMaintenance>(sp => (IDocumentMaintenance)sp.GetRequiredService<IDocumentStore>());
        return services;
    }
}
