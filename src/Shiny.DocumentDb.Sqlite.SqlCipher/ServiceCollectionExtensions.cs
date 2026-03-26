using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb.Sqlite.SqlCipher;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSqlCipherDocumentStore(this IServiceCollection services, string filePath, string password)
    {
        return services.AddSqlCipherDocumentStore(opts => opts.DatabaseProvider = new SqlCipherDatabaseProvider(filePath, password));
    }

    public static IServiceCollection AddSqlCipherDocumentStore(this IServiceCollection services, Action<DocumentStoreOptions> configure)
    {
        var options = new DocumentStoreOptions { DatabaseProvider = null! };
        configure(options);

        if (options.DatabaseProvider is null)
            throw new ArgumentException("DatabaseProvider must be set. Use AddSqlCipherDocumentStore(filePath, password) or set DatabaseProvider to a SqlCipherDatabaseProvider.", nameof(configure));

        services.AddSingleton(options);
        services.AddSingleton<IDocumentStore>(sp => new SqlCipherDocumentStore(sp.GetRequiredService<DocumentStoreOptions>()));
        return services;
    }
}
