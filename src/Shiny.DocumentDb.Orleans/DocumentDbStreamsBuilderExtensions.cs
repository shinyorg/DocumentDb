using Microsoft.Extensions.DependencyInjection;
using Orleans.Streams;
using Shiny.DocumentDb.Orleans;

namespace Orleans.Hosting;

/// <summary>
/// Registers the DocumentDb persistent stream provider — durable, inspectable Orleans streams on the database
/// the cluster already uses for membership, grain storage and reminders.
/// </summary>
public static class DocumentDbStreamsBuilderExtensions
{
    /// <summary>
    /// Adds a DocumentDb-backed persistent stream provider to a silo.
    /// </summary>
    /// <remarks>
    /// Requires a backend with real row-level locking — PostgreSQL, SQL Server, MySQL, MariaDB, Oracle or
    /// CockroachDB. The silo fails to start on anything else rather than deferring the failure to the first
    /// event, because the alternative is a cluster that boots and then loses events under load.
    /// </remarks>
    public static ISiloBuilder AddDocumentDbStreams(
        this ISiloBuilder builder,
        string name,
        Action<DocumentDbStreamOptions> configure
    ) => builder.AddDocumentDbStreams(name, configure, _ => { });

    /// <summary>
    /// Adds a DocumentDb-backed persistent stream provider to a silo, with access to the pulling-agent and
    /// lifecycle configuration Orleans exposes for persistent streams.
    /// </summary>
    public static ISiloBuilder AddDocumentDbStreams(
        this ISiloBuilder builder,
        string name,
        Action<DocumentDbStreamOptions> configure,
        Action<ISiloPersistentStreamConfigurator> configureStream
    ) => builder
        .AddPersistentStreams(name, DocumentDbQueueAdapterFactory.Create, configureStream)
        .ConfigureServices(services =>
        {
            services.AddOptions<DocumentDbStreamOptions>(name).Configure(configure);
            services.AddStreamAdmin(name);
        });

    /// <summary>
    /// Registers <see cref="IStreamAdmin"/> keyed by provider name, sharing the adapter factory's store so
    /// the operational view does not open a second connection pool onto the same table.
    /// </summary>
    static IServiceCollection AddStreamAdmin(this IServiceCollection services, string name)
    {
        services.AddKeyedSingleton<IStreamAdmin>(name, (sp, key) =>
        {
            var providerName = (string)key!;
            // The adapter factory is registered by AddPersistentStreams under the same key. Resolving it here
            // rather than rebuilding one keeps a single store per provider; the fallback covers a host that
            // asks for the admin view before the stream provider has been constructed.
            var factory = sp.GetKeyedService<IQueueAdapterFactory>(providerName) as DocumentDbQueueAdapterFactory
                ?? DocumentDbQueueAdapterFactory.Create(sp, providerName);

            return new StreamAdmin(factory.Store, providerName, factory.ServiceId);
        });
        return services;
    }

    /// <summary>
    /// Adds a DocumentDb-backed persistent stream provider to an external client, so the client can produce
    /// to streams directly.
    /// </summary>
    /// <remarks>
    /// The client enqueues through the same locked-counter path as a silo, which means it needs a database
    /// connection of its own. That is reasonable for a trusted-network client and wrong for anything
    /// internet-facing — send through a grain instead.
    /// </remarks>
    public static IClientBuilder AddDocumentDbStreams(
        this IClientBuilder builder,
        string name,
        Action<DocumentDbStreamOptions> configure
    ) => builder
        .AddPersistentStreams(name, DocumentDbQueueAdapterFactory.Create, _ => { })
        .ConfigureServices(services => services.AddOptions<DocumentDbStreamOptions>(name).Configure(configure));
}
