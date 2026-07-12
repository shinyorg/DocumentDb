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
        services.AddSingleton<IDocumentStore>(sp =>
        {
            return new DocumentStore(options, sp);
        });
        // The session factory is universally safe (MAUI/desktop/background + ASP.NET).
        services.TryAddSingleton<IDocumentSessionFactory>(
            sp => new DocumentSessionFactory(sp, sp.GetRequiredService<IServiceScopeFactory>()));

        return services;
    }

    /// <summary>
    /// Adds a request-scoped <see cref="IDocumentSession"/> (unit of work) on top of <c>AddDocumentStore</c> — the
    /// opt-in for hosts with an ambient scope (ASP.NET Core). The session rides the resolving (request) scope and
    /// is disposed with it. Not for MAUI/desktop/background, which have no ambient scope — use
    /// <see cref="IDocumentSessionFactory"/> there. Mirrors EF's <c>AddDbContext</c> vs <c>AddDbContextFactory</c>
    /// split (§4a).
    /// </summary>
    public static IServiceCollection AddScopedDocumentSession(this IServiceCollection services)
    {
        services.AddScoped<IDocumentSession>(sp =>
            new DocumentSession(sp.GetRequiredService<IDocumentStore>(), sp, ownedScope: null));   // rides the request scope; does not own it
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
            options.TenantIdAccessor = () => (DocumentOperationScope.CurrentServices ?? sp).GetRequiredService<ITenantResolver>().GetCurrentTenant();
            return new DocumentStore(options, sp);
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

        options.StoreName = name;   // tags embedded metrics/spans with db.namespace = name
        services.AddKeyedSingleton<IDocumentStore>(name, (sp, _) =>
        {
            return new DocumentStore(options, sp);
        });
        services.TryAddSingleton<IDocumentSessionFactory>(sp => new DocumentSessionFactory(sp, sp.GetRequiredService<IServiceScopeFactory>()));

        return services;
    }

    /// <summary>
    /// Registers a keyed/named shared-table multi-tenant document store. A TenantId column is added
    /// to the table schema and all queries are automatically filtered by the current tenant resolved
    /// via <see cref="ITenantResolver"/>. The keyed equivalent of
    /// <see cref="AddDocumentStore(IServiceCollection, Action{DocumentStoreOptions}, bool)"/>.
    /// </summary>
    public static IServiceCollection AddDocumentStore(this IServiceCollection services, string name, Action<DocumentStoreOptions> configure, bool multiTenant)
    {
        ArgumentNullException.ThrowIfNull(configure);

        if (!multiTenant)
            return AddDocumentStore(services, name, configure);

        return AddDocumentStore(services, name, (sp, options) =>
        {
            configure(options);
            options.TenantIdAccessor = () => (DocumentOperationScope.CurrentServices ?? sp).GetRequiredService<ITenantResolver>().GetCurrentTenant();
        });
    }

    /// <summary>
    /// Registers a keyed/named document store whose options are configured with access to the
    /// resolved <see cref="IServiceProvider"/>. Use this overload when the configuration depends on
    /// other registered services — for example wiring shared-table multi-tenancy
    /// (<c>o.TenantIdAccessor = () =&gt; sp.GetRequiredService&lt;ITenantResolver&gt;().GetCurrentTenant()</c>)
    /// or resolving an interceptor instance from the container. The callback runs once when the
    /// keyed store is first resolved. Embedded telemetry from this store is tagged <c>db.namespace = name</c>.
    /// </summary>
    public static IServiceCollection AddDocumentStore(this IServiceCollection services, string name, Action<IServiceProvider, DocumentStoreOptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddKeyedSingleton<IDocumentStore>(name, (sp, _) =>
        {
            var options = new DocumentStoreOptions
            {
                DatabaseProvider = null!,
                StoreName = name   // tags embedded metrics/spans with db.namespace = name
            };
            configure(sp, options);

            if (options.DatabaseProvider == null)
                throw new ArgumentException("DatabaseProvider must be set.", nameof(configure));

            return new DocumentStore(options, sp);
        });
        services.TryAddSingleton<IDocumentSessionFactory>(sp => new DocumentSessionFactory(sp, sp.GetRequiredService<IServiceScopeFactory>()));
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

        services.AddSingleton(sp => new MultiTenantDocumentStoreFactory(sp, optionsFactory));
        services.AddScoped<IDocumentStore>(sp =>
        {
            var resolver = sp.GetRequiredService<ITenantResolver>();
            var factory = sp.GetRequiredService<MultiTenantDocumentStoreFactory>();
            return factory.GetStore(resolver.GetCurrentTenant());
        });
        return services;
    }
}
