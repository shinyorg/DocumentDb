using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb;

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
    public static ISiloBuilder UseAspireDocumentDb(
        this ISiloBuilder silo,
        string storeName,
        DocumentDbOrleansFeatures features = DocumentDbOrleansFeatures.All,
        string grainStorageName = "Default",
        string grainDirectoryName = "Default")
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

        return silo;
    }
}
