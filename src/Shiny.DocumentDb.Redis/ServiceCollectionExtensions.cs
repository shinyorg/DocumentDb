using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shiny.DocumentDb.Redis;

namespace Shiny.DocumentDb;

public static class RedisServiceCollectionExtensions
{
    /// <summary>
    /// Registers a Redis Stack (RedisJSON + RediSearch) <see cref="IDocumentStore"/> as a singleton,
    /// along with <see cref="IDocumentMaintenance"/> pointing at the same instance.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configures the store options (connection multiplexer / connection string, mappings).</param>
    public static IServiceCollection AddRedisDocumentStore(
        this IServiceCollection services,
        Action<RedisDocumentStoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new RedisDocumentStoreOptions();
        configure(options);

        // Factory (not an eager instance) so the SP-taking ctor can wire DI-registered interceptors.
        services.AddSingleton<IDocumentStore>(sp => new RedisDocumentStore(options, sp));
        services.TryAddSingleton<IDocumentMaintenance>(sp => (IDocumentMaintenance)sp.GetRequiredService<IDocumentStore>());
        return services;
    }
}
