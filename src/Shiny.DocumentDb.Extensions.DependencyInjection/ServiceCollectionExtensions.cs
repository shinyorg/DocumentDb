using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDocumentStore(this IServiceCollection services, Action<DocumentStoreOptions> configure)
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = null!
        };
        configure(options);

        if (options.DatabaseProvider == null)
            throw new ArgumentException("DatabaseProvider must be set.", nameof(configure));

        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore, DocumentStore>();
        return services;
    }
}
