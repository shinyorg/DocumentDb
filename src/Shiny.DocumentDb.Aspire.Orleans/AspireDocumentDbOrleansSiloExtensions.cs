using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Orleans;

namespace Orleans.Hosting;

/// <summary>The Orleans system stores to back with the Aspire-provisioned Shiny.DocumentDb store.</summary>
[Flags]
public enum DocumentDbOrleansFeatures
{
    /// <summary>Grain storage (and, by name, <c>PubSubStore</c> if you add it separately).</summary>
    GrainStorage = 1,
    /// <summary>Reminder table (<c>IReminderTable</c>).</summary>
    Reminders = 2,
    /// <summary>Cluster membership (<c>IMembershipTable</c>). Needs a backend with multi-document transactions.</summary>
    Clustering = 4,
    /// <summary>Grain directory (<c>IGrainDirectory</c>).</summary>
    GrainDirectory = 8,

    /// <summary>
    /// Persistent streams (<c>IQueueAdapterFactory</c>). <b>Opt-in — deliberately not part of
    /// <see cref="All"/>.</b>
    /// </summary>
    /// <remarks>
    /// Streams have a narrower backend gate than the persistence stores: they require row-level pessimistic
    /// locking (PostgreSQL, SQL Server, MySQL, MariaDB, Oracle, CockroachDB) and the silo refuses to start
    /// without it. Folding that into <see cref="All"/> would turn a working SQLite or DuckDB Aspire app into
    /// one that will not boot, purely because it took a package update — and would also add a stream provider
    /// nobody asked for. Name it explicitly.
    /// </remarks>
    Streams = 16,

    /// <summary>
    /// The four persistence stores. Does <b>not</b> include <see cref="Streams"/> — see the note there.
    /// </summary>
    All = GrainStorage | Reminders | Clustering | GrainDirectory
}

/// <summary>
/// Bridges the Aspire-provisioned, keyed <see cref="IDocumentStore"/> (registered by
/// <c>Shiny.DocumentDb.Aspire.Client</c>'s <c>builder.AddDocumentStore(name)</c>) into the
/// <c>Shiny.DocumentDb.Orleans</c> silo providers, so Orleans persistence/reminders/clustering/directory all
/// run on the one Aspire-managed store (sharing its connection, health check, and telemetry).
/// </summary>
public static class AspireDocumentDbOrleansSiloExtensions
{
    /// <summary>
    /// Backs the selected Orleans system stores with the keyed Shiny.DocumentDb store named
    /// <paramref name="storeName"/>. Call <c>builder.AddDocumentStore(storeName)</c> (from
    /// <c>Shiny.DocumentDb.Aspire.Client</c>) on the host builder first — this wires each provider's
    /// <c>StoreFactory</c> to resolve that keyed store.
    /// </summary>
    /// <param name="features">
    /// Which stores to back. Defaults to the four persistence stores;
    /// <see cref="DocumentDbOrleansFeatures.Streams"/> must be named explicitly because its backend
    /// requirements are stricter.
    /// </param>
    /// <param name="configureStreams">
    /// Optional tuning for the stream provider (queue count, retention, poll intervals). Ignored unless
    /// <see cref="DocumentDbOrleansFeatures.Streams"/> is selected. The store itself always comes from
    /// Aspire — setting <c>StoreFactory</c> or <c>DatabaseProvider</c> here would defeat the point and is
    /// overwritten.
    /// </param>
    public static ISiloBuilder UseAspireDocumentDb(
        this ISiloBuilder silo,
        string storeName,
        DocumentDbOrleansFeatures features = DocumentDbOrleansFeatures.All,
        string grainStorageName = "Default",
        string grainDirectoryName = "Default",
        string streamProviderName = "Default",
        Action<DocumentDbStreamOptions>? configureStreams = null)
    {
        ArgumentNullException.ThrowIfNull(silo);
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);

        Func<IServiceProvider, IDocumentStore> factory =
            sp => sp.GetRequiredKeyedService<IDocumentStore>(storeName);

        if (features.HasFlag(DocumentDbOrleansFeatures.GrainStorage))
            silo.AddDocumentDbGrainStorage(grainStorageName, o => o.StoreFactory = factory);

        if (features.HasFlag(DocumentDbOrleansFeatures.Reminders))
            silo.AddDocumentDbReminders(o => o.StoreFactory = factory);

        if (features.HasFlag(DocumentDbOrleansFeatures.Clustering))
            silo.AddDocumentDbClustering(o => o.StoreFactory = factory);

        if (features.HasFlag(DocumentDbOrleansFeatures.GrainDirectory))
            silo.AddDocumentDbGrainDirectory(grainDirectoryName, o => o.StoreFactory = factory);

        if (features.HasFlag(DocumentDbOrleansFeatures.Streams))
        {
            silo.AddDocumentDbStreams(streamProviderName, o =>
            {
                // Caller tuning first, then the store — so the Aspire-provisioned store always wins over an
                // accidental DatabaseProvider in the callback.
                configureStreams?.Invoke(o);
                o.DatabaseProvider = null;
                o.StoreFactory = factory;
            });
        }

        return silo;
    }
}
