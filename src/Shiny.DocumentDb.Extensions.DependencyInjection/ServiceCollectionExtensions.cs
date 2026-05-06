using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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

    /// <summary>
    /// Registers a shared-table multi-tenant document store.
    /// A TenantId column is added to the table schema and all queries are automatically
    /// filtered by the current tenant resolved via ITenantResolver.
    /// </summary>
    public static IServiceCollection AddDocumentStore(this IServiceCollection services, Action<DocumentStoreOptions> configure, bool multiTenant)
    {
        if (!multiTenant)
            return AddDocumentStore(services, configure);

        var options = new DocumentStoreOptions
        {
            DatabaseProvider = null!
        };
        configure(options);

        if (options.DatabaseProvider == null)
            throw new ArgumentException("DatabaseProvider must be set.", nameof(configure));

        services.AddSingleton<IDocumentStore>(sp =>
        {
            options.TenantIdAccessor = () => sp.GetRequiredService<ITenantResolver>().GetCurrentTenant();
            return new DocumentStore(options);
        });
        return services;
    }

    public static IServiceCollection AddDocumentStore(this IServiceCollection services, string name, Action<DocumentStoreOptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var options = new DocumentStoreOptions
        {
            DatabaseProvider = null!
        };
        configure(options);

        if (options.DatabaseProvider == null)
            throw new ArgumentException("DatabaseProvider must be set.", nameof(configure));

        services.AddKeyedSingleton<IDocumentStore>(name, (_, _) => new DocumentStore(options));
        services.TryAddSingleton<IDocumentStoreProvider, DocumentStoreProvider>();
        return services;
    }

    /// <summary>
    /// Registers a tenant-per-database document store. Each tenant gets a separate database
    /// created lazily on first access. The current tenant is resolved via ITenantResolver.
    /// IDocumentStore is registered as scoped — inject it normally and it resolves to the
    /// correct tenant's store automatically.
    /// </summary>
    public static IServiceCollection AddMultiTenantDocumentStore(
        this IServiceCollection services,
        Func<string, DocumentStoreOptions> optionsFactory)
    {
        ArgumentNullException.ThrowIfNull(optionsFactory);

        services.AddSingleton(new MultiTenantDocumentStoreFactory(optionsFactory));
        services.AddScoped<IDocumentStore>(sp =>
        {
            var resolver = sp.GetRequiredService<ITenantResolver>();
            var factory = sp.GetRequiredService<MultiTenantDocumentStoreFactory>();
            return factory.GetStore(resolver.GetCurrentTenant());
        });
        return services;
    }
}
