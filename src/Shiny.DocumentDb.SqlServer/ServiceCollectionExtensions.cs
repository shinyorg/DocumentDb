using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb.SqlServer;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSqlServerDocumentStore(this IServiceCollection services, string connectionString)
    {
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqlServerDatabaseProvider(connectionString)
        };
        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore, DocumentStore>();
        return services;
    }

    public static IServiceCollection AddSqlServerDocumentStore(this IServiceCollection services, Action<DocumentStoreOptions> configure)
    {
        var options = new DocumentStoreOptions { DatabaseProvider = null! };
        configure(options);

        if (options.DatabaseProvider is null)
            throw new ArgumentException("DatabaseProvider must be set.", nameof(configure));

        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore, DocumentStore>();
        return services;
    }
}
