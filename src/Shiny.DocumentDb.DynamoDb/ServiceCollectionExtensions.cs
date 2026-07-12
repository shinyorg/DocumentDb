using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shiny.DocumentDb.DynamoDb;

namespace Shiny.DocumentDb;

public static class DynamoDbServiceCollectionExtensions
{
    /// <summary>
    /// Registers an Amazon DynamoDB <see cref="IDocumentStore"/> as a singleton, along with
    /// <see cref="IDocumentMaintenance"/> pointing at the same instance.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configures the store options (credentials / region / service URL, table name, mappings).</param>
    public static IServiceCollection AddDynamoDbDocumentStore(
        this IServiceCollection services,
        Action<DynamoDbDocumentStoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new DynamoDbDocumentStoreOptions();
        configure(options);

        // Factory (not an eager instance) so the SP-taking ctor can wire DI-registered interceptors.
        services.AddSingleton<IDocumentStore>(sp => new DynamoDbDocumentStore(options, sp));
        services.TryAddSingleton<IDocumentMaintenance>(sp => (IDocumentMaintenance)sp.GetRequiredService<IDocumentStore>());
        return services;
    }
}
