using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb.Sqlite;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSqliteDocumentStore(this IServiceCollection services, string connectionString)
    {
        return services.AddSqliteDocumentStore(opts => opts.DatabaseProvider = new SqliteDatabaseProvider(connectionString));
    }

    public static IServiceCollection AddSqliteDocumentStore(this IServiceCollection services, Action<DocumentStoreOptions> configure)
    {
        var options = new DocumentStoreOptions { DatabaseProvider = null! };
        configure(options);

        if (options.DatabaseProvider is null)
            throw new ArgumentException("DatabaseProvider must be set. Use AddSqliteDocumentStore(connectionString) or set DatabaseProvider to a SqliteDatabaseProvider.", nameof(configure));

        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore>(sp => new SqliteDocumentStore(sp.GetRequiredService<DocumentStoreOptions>()));
        return services;
    }
}
