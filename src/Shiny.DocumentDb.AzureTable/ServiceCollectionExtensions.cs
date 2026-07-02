using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shiny.DocumentDb.AzureTable;

namespace Shiny.DocumentDb;

public static class AzureTableServiceCollectionExtensions
{
    /// <summary>
    /// Registers an Azure Table Storage (or Cosmos DB Table API) <see cref="IDocumentStore"/> as a
    /// singleton, along with <see cref="IDocumentMaintenance"/> pointing at the same instance.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configures the store options (connection string / credentials, table name, mappings).</param>
    public static IServiceCollection AddAzureTableDocumentStore(
        this IServiceCollection services,
        Action<AzureTableDocumentStoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new AzureTableDocumentStoreOptions();
        configure(options);

        var store = new AzureTableDocumentStore(options);

        services.AddSingleton<IDocumentStore>(store);
        services.TryAddSingleton<IDocumentMaintenance>(sp => (IDocumentMaintenance)sp.GetRequiredService<IDocumentStore>());
        return services;
    }
}
