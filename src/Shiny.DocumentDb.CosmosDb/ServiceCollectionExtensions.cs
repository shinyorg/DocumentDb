using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb.CosmosDb;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddCosmosDbDocumentStore(this IServiceCollection services, string connectionString, string databaseName)
    {
        return services.AddCosmosDbDocumentStore(opts =>
        {
            opts.ConnectionString = connectionString;
            opts.DatabaseName = databaseName;
        });
    }

    public static IServiceCollection AddCosmosDbDocumentStore(this IServiceCollection services, Action<CosmosDbDocumentStoreOptions> configure)
    {
        var options = new CosmosDbDocumentStoreOptions { ConnectionString = null!, DatabaseName = null! };
        configure(options);

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            throw new ArgumentException("ConnectionString must be set.", nameof(configure));

        if (string.IsNullOrWhiteSpace(options.DatabaseName))
            throw new ArgumentException("DatabaseName must be set.", nameof(configure));

        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore, CosmosDbDocumentStore>();
        return services;
    }
}
