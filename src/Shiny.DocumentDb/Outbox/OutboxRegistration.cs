using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Shiny.DocumentDb;

/// <summary>Maps the outbox message type onto a store — one extension for every provider.</summary>
public static class OutboxStoreOptionsExtensions
{
    /// <summary>The table/collection outbox messages live in unless <c>AddOutbox</c> is told otherwise.</summary>
    public const string DefaultTableName = "outbox";

    /// <summary>
    /// Maps <see cref="OutboxMessage"/> onto this store: its own table (so infrastructure rows never mix into a
    /// business table, and the processor's hot poll never touches one), and the version property the claim
    /// protocol uses for optimistic concurrency.
    /// <code>
    /// services.AddDocumentStore(o =>
    /// {
    ///     o.DatabaseProvider = new SqliteDatabaseProvider(path);
    ///     o.AddOutbox();
    /// });
    /// services.AddDocumentOutbox&lt;MyDispatcher&gt;();
    /// </code>
    /// </summary>
    /// <param name="tableName">
    /// The table to store messages in. Pass <c>null</c> to leave them in the store's shared default table —
    /// the only sensible choice on a backend with no per-type storage unit.
    /// </param>
    public static IDocumentStoreOptions AddOutbox(this IDocumentStoreOptions options, string? tableName = DefaultTableName)
    {
        ArgumentNullException.ThrowIfNull(options);

        return options.ConfigureDocument<OutboxMessage>(cfg =>
        {
            if (tableName != null)
                cfg.Table = tableName;

            // The claim primitive: two processors that read the same message both write it back, and the loser
            // gets a ConcurrencyException. No leases table, no leader election, works on every store that
            // implements optimistic concurrency.
            cfg.MapVersionProperty(x => x.Version);
        });
    }
}

/// <summary>Registers the outbox processor and its operational surface.</summary>
public static class OutboxServiceCollectionExtensions
{
    /// <summary>
    /// Registers <typeparamref name="TDispatcher"/> (scoped, so it can take scoped dependencies), the background
    /// processor that drains the queue, and <see cref="IOutboxAdmin"/>.
    /// </summary>
    /// <remarks>
    /// Requires <c>options.AddOutbox()</c> on the store this runs against. The store's
    /// <see cref="IDocumentStore.SupportsTransactions"/> is checked once at host startup, and a backend that
    /// cannot make the guarantee fails there by name rather than silently degrading to a dual write.
    /// </remarks>
    public static IServiceCollection AddDocumentOutbox<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TDispatcher>(
        this IServiceCollection services,
        Action<OutboxOptions>? configure = null)
        where TDispatcher : class, IOutboxDispatcher
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddScoped<IOutboxDispatcher, TDispatcher>();
        return AddCore(services, configure);
    }

    /// <summary>
    /// Registers a delegate dispatcher — the one-liner form, for a transport that needs no injected state:
    /// <code>services.AddDocumentOutbox((msg, ct) => bus.Publish(msg.MessageType, msg.Payload, ct));</code>
    /// </summary>
    public static IServiceCollection AddDocumentOutbox(
        this IServiceCollection services,
        Func<OutboxMessage, CancellationToken, Task> dispatch,
        Action<OutboxOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(dispatch);
        services.TryAddSingleton<IOutboxDispatcher>(new DelegateOutboxDispatcher(dispatch));
        return AddCore(services, configure);
    }

    static IServiceCollection AddCore(IServiceCollection services, Action<OutboxOptions>? configure)
    {
        var options = new OutboxOptions();
        configure?.Invoke(options);
        options.Validate();

        services.TryAddSingleton(options);
        services.TryAddSingleton<IOutboxAdmin>(sp => new OutboxAdmin(
            sp.GetRequiredService<IDocumentStore>(),
            sp.GetRequiredService<OutboxOptions>(),
            sp.GetService<TimeProvider>() ?? TimeProvider.System));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxProcessor>());
        return services;
    }
}
