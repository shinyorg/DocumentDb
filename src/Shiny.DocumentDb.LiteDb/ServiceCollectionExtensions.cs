using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb.LiteDb;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddLiteDbDocumentStore(this IServiceCollection services, string connectionString)
    {
        return services.AddLiteDbDocumentStore(opts => opts.ConnectionString = connectionString);
    }

    public static IServiceCollection AddLiteDbDocumentStore(this IServiceCollection services, Action<LiteDbDocumentStoreOptions> configure)
    {
        var options = new LiteDbDocumentStoreOptions { ConnectionString = null! };
        configure(options);

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            throw new ArgumentException("ConnectionString must be set.", nameof(configure));

        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore, LiteDbDocumentStore>();
        return services;
    }
}
