using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

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
    /// Registers a tenant-per-database document store over <b>any</b> provider — the factory returns a built
    /// store, so tenants can live on Mongo, Cosmos, LiteDB or anything else implementing
    /// <see cref="IDocumentStore"/>. Each tenant's store is built lazily on first access and cached; the
    /// current tenant is resolved via <see cref="ITenantResolver"/>.
    /// <para>
    /// <see cref="IDocumentStore"/>, <see cref="IDocumentSession"/> and <see cref="IDocumentSessionFactory"/>
    /// are all wired to the current tenant. The cache is bounded (see <see cref="TenantStoreOptions"/>), so a
    /// store can be closed while the process lives — resolve <see cref="IDocumentStore"/> per scope rather
    /// than capturing it in a singleton.
    /// </para>
    /// </summary>
    /// <remarks>
    /// The cache owns each store's lifetime. Stores built by this library know that and ignore the DI scope's
    /// disposal; a hand-rolled <see cref="IDocumentStore"/> that is <see cref="IDisposable"/> and does not
    /// derive from <see cref="DocumentProviderBase"/> will be closed by the first scope that resolves it.
    /// </remarks>
    public static IServiceCollection AddMultiTenantDocumentStore(
        this IServiceCollection services,
        Func<string, IDocumentStore> storeFactory,
        Action<TenantStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(storeFactory);
        return AddTenantRouting(services, _ => storeFactory, configure);
    }

    /// <summary>
    /// Registers a tenant-per-database <b>relational</b> document store: the factory supplies each tenant's
    /// <see cref="DocumentStoreOptions"/> (typically the same provider with a per-tenant connection string)
    /// and the store is built for you, tagged for telemetry with <c>db.namespace</c> = the tenant id (see
    /// <see cref="TenantStoreOptions.StoreNameFactory"/>).
    /// </summary>
    public static IServiceCollection AddMultiTenantDocumentStore(
        this IServiceCollection services,
        Func<string, DocumentStoreOptions> optionsFactory,
        Action<TenantStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(optionsFactory);

        return AddTenantRouting(
            services,
            sp => tenantId =>
            {
                var options = optionsFactory(tenantId);
                if (options.DatabaseProvider == null)
                    throw new ArgumentException("DatabaseProvider must be set.", nameof(optionsFactory));

                var tenantOptions = sp.GetRequiredService<TenantDocumentStoreCache>().Options;
                options.StoreName ??= tenantOptions.StoreNameFactory?.Invoke(tenantId) ?? tenantId;
                return new DocumentStore(options, sp);
            },
            configure
        );
    }

    static IServiceCollection AddTenantRouting(
        IServiceCollection services,
        Func<IServiceProvider, Func<string, IDocumentStore>> storeFactoryBuilder,
        Action<TenantStoreOptions>? configure)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new TenantStoreOptions();
        configure?.Invoke(options);
        options.Validate();

        services.AddSingleton(sp => new TenantDocumentStoreCache(sp, storeFactoryBuilder(sp), options));
        services.AddSingleton<ITenantStoreManager>(sp => sp.GetRequiredService<TenantDocumentStoreCache>());

        // The lease — not the store — is what the DI scope owns: the container disposes it when the request
        // ends, which is what tells the cache the store is free to close.
        services.AddScoped(sp => sp
            .GetRequiredService<TenantDocumentStoreCache>()
            .Lease(sp.GetRequiredService<ITenantResolver>().GetCurrentTenant()));
        services.AddScoped<IDocumentStore>(sp => sp.GetRequiredService<TenantStoreLease>().Store);
        services.TryAddScoped<IDocumentSession>(sp =>
            new DocumentSession(sp.GetRequiredService<IDocumentStore>(), sp, ownedScope: null));
        services.TryAddSingleton<IDocumentSessionFactory>(
            sp => new DocumentSessionFactory(sp, sp.GetRequiredService<IServiceScopeFactory>()));

        if (options.IdleTimeout != null)
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, TenantStoreIdleSweeper>());

        return services;
    }
}
