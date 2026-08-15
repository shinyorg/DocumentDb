#if IOS || ANDROID || MACCATALYST
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shiny.Extensions.Stores;
using Shiny.Locations;

namespace Shiny.DocumentDb.Geofencing;

/// <summary>
/// Registration for GPS-driven geofencing over document regions. Only available on iOS, Android, and Mac
/// Catalyst — the other targets ship the types but have no platform GPS to drive them.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers document geofencing along with the platform GPS services it rides on.
    /// </summary>
    /// <typeparam name="TDelegate">The app's transition handler.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Registers the region sets and tunes the reading filters.</param>
    /// <exception cref="ArgumentException">No region set was registered.</exception>
    public static IServiceCollection AddDocumentGeofencing<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.Interfaces)] TDelegate
    >(this IServiceCollection services, Action<DocumentGeofenceConfig> configure) where TDelegate : class, IDocumentGeofenceDelegate
    {
        ArgumentNullException.ThrowIfNull(configure);

        var config = new DocumentGeofenceConfig();
        configure(config);

        if (config.RegionSets.Count == 0)
            throw new ArgumentException("At least one region set must be registered - call AddRegionSet<T>.", nameof(configure));

        services.AddSingleton(config);
        services.AddSingleton<IDocumentGeofenceDelegate, TDelegate>();

        services.AddSingleton(sp => new DocumentRegionEvaluator(
            ResolveStore(sp, config.StoreName),
            sp.GetRequiredKeyedService<IKeyValueStore>(StoreKeys.Default),
            sp.GetRequiredService<IDocumentGeofenceDelegate>(),
            config,
            sp.GetRequiredService<ILogger<DocumentRegionEvaluator>>()
        ));

        services.AddSingleton(sp => new DocumentGeofenceManager(
            ResolveStore(sp, config.StoreName),
            sp.GetRequiredService<IGpsManager>(),
            sp.GetRequiredKeyedService<IKeyValueStore>(StoreKeys.Default),
            sp.GetRequiredService<DocumentRegionEvaluator>(),
            config,
            sp.GetRequiredService<ILogger<DocumentGeofenceManager>>()
        ));
        services.AddSingleton<IDocumentGeofenceManager>(sp => sp.GetRequiredService<DocumentGeofenceManager>());
        services.AddSingleton<IShinyStartupTask>(sp => sp.GetRequiredService<DocumentGeofenceManager>());

        // Brings in the platform GPS manager and wires our reading listener as the app's IGpsDelegate.
        services.AddGps<DocumentGeofenceGpsDelegate>();

        return services;
    }


    static IDocumentStore ResolveStore(IServiceProvider sp, string? storeName)
        => storeName == null
            ? sp.GetRequiredService<IDocumentStore>()
            : sp.GetRequiredKeyedService<IDocumentStore>(storeName);
}
#endif
