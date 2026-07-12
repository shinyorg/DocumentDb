using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Shiny.DocumentDb;

/// <summary>
/// Registration helpers the source-generated <c>Add&lt;Context&gt;</c> / <c>Add&lt;Context&gt;Factory</c>
/// methods delegate to. Each context gets its <b>own</b> store, keyed by the context type, so multiple
/// <see cref="DocumentContext"/> types coexist in one container without shadowing one another (a plain
/// <see cref="ServiceCollectionExtensions.AddDocumentStore(IServiceCollection, Action{DocumentStoreOptions})"/>
/// registers an un-keyed singleton, so a second one would win every <see cref="IDocumentStore"/> resolve).
/// </summary>
public static class DocumentContextServiceCollectionExtensions
{
    /// <summary>
    /// Registers a per-context document store and the context itself as <b>scoped</b> — the request-scoped
    /// registration for ASP.NET Core. In an app with no ambient scope (MAUI, Blazor, desktop) resolve the
    /// context through <see cref="AddDocumentContextFactory{TContext}"/> instead.
    /// </summary>
    public static IServiceCollection AddDocumentContext<TContext>(
        this IServiceCollection services,
        Action<DocumentStoreOptions> configure,
        Func<IDocumentSession, TContext> activator) where TContext : DocumentContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        ArgumentNullException.ThrowIfNull(activator);

        RegisterStore<TContext>(services, configure);
        services.AddScoped(sp =>
        {
            // The session rides the resolving (request) scope (does not own it) so DocumentSet writes carry the
            // caller's own scoped services to write-time interceptors. The context owns/disposes the session.
            var store = sp.GetRequiredKeyedService<IDocumentStore>(typeof(TContext));
            var session = new DocumentSession(store, sp, ownedScope: null);
            return activator(session);
        });
        return services;
    }

    /// <summary>
    /// Registers an <see cref="IDocumentContextFactory{TContext}"/> (singleton) that creates short-lived
    /// contexts on demand — the registration for apps with no ambient DI scope (MAUI, Blazor, desktop,
    /// background services). Inject the factory anywhere, even into singletons, and call
    /// <see cref="IDocumentContextFactory{TContext}.Create"/> per unit of work.
    /// </summary>
    public static IServiceCollection AddDocumentContextFactory<TContext>(
        this IServiceCollection services,
        Action<DocumentStoreOptions> configure,
        Func<IDocumentSession, TContext> activator) where TContext : DocumentContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        ArgumentNullException.ThrowIfNull(activator);

        RegisterStore<TContext>(services, configure);
        services.TryAddSingleton<IDocumentContextFactory<TContext>>(
            sp => new DocumentContextFactory<TContext>(
                sp.GetRequiredKeyedService<IDocumentStore>(typeof(TContext)),
                sp.GetRequiredService<IServiceScopeFactory>(),
                activator));
        return services;
    }

    // One store per context, keyed by the context type. TryAdd makes it idempotent, so Add<Context> and
    // Add<Context>Factory can both be called for the same context without building two stores (the store
    // config comes from whichever registers first). The first context also becomes the default un-keyed
    // IDocumentStore so extension packages that inject it (AppDataSync, Diagnostics, AI, OData, seeding)
    // keep resolving.
    static void RegisterStore<TContext>(IServiceCollection services, Action<DocumentStoreOptions> configure)
        where TContext : DocumentContext
    {
        services.TryAddKeyedSingleton<IDocumentStore>(typeof(TContext), (sp, _) =>
        {
            var options = new DocumentStoreOptions { DatabaseProvider = null! };
            configure(options);
            if (options.DatabaseProvider == null)
                throw new ArgumentException("DatabaseProvider must be set.", nameof(configure));

            return new DocumentStore(options, sp);
        });
        services.TryAddSingleton<IDocumentStore>(sp => sp.GetRequiredKeyedService<IDocumentStore>(typeof(TContext)));
    }
}
