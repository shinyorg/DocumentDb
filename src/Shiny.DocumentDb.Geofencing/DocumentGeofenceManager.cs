using Microsoft.Extensions.Logging;
using Shiny.Extensions.Stores;
using Shiny.Locations;

namespace Shiny.DocumentDb.Geofencing;

/// <summary>
/// Default <see cref="IDocumentGeofenceManager"/> — drives the GPS listener and restores monitoring after
/// the app is relaunched.
/// </summary>
public class DocumentGeofenceManager : IDocumentGeofenceManager, IShinyStartupTask
{
    const string StartedKey = "documentdb.geofence.started";

    readonly IDocumentStore store;
    readonly IGpsManager gpsManager;
    readonly IKeyValueStore state;
    readonly DocumentRegionEvaluator evaluator;
    readonly DocumentGeofenceConfig config;
    readonly ILogger logger;


    /// <summary>Creates the manager. Registered as a singleton by <c>AddDocumentGeofencing</c>.</summary>
    public DocumentGeofenceManager(
        IDocumentStore store,
        IGpsManager gpsManager,
        IKeyValueStore state,
        DocumentRegionEvaluator evaluator,
        DocumentGeofenceConfig config,
        ILogger<DocumentGeofenceManager> logger
    )
    {
        this.store = store;
        this.gpsManager = gpsManager;
        this.state = state;
        this.evaluator = evaluator;
        this.config = config;
        this.logger = logger;
    }


    /// <inheritdoc />
    public bool IsStarted => this.state.Get<bool>(StartedKey);


    /// <inheritdoc />
    public Task<AccessState> RequestAccess()
        => this.gpsManager.RequestAccess(this.config.GpsRequest);


    /// <inheritdoc />
    public async Task Start()
    {
        if (!this.store.SupportsSpatial)
            throw new NotSupportedException("The document store's provider does not support spatial queries, so it cannot back geofencing. Use SQLite, PostgreSQL, MySQL, SQL Server, Oracle, DuckDB, CosmosDB, or MongoDB.");

        if (this.IsStarted && this.gpsManager.CurrentListener != null)
            return;

        await this.gpsManager.StartListener(this.config.GpsRequest).ConfigureAwait(false);
        this.state.Set(StartedKey, true);
        this.logger.LogInformation("Document geofencing started against {Count} region set(s)", this.config.RegionSets.Count);
    }


    /// <inheritdoc />
    public async Task Stop()
    {
        await this.gpsManager.StopListener().ConfigureAwait(false);
        this.state.Remove(StartedKey);
        this.evaluator.Reset();
        this.logger.LogInformation("Document geofencing stopped");
    }


    /// <inheritdoc />
    public async Task<IReadOnlyList<DocumentCurrentRegion>> GetCurrent(CancellationToken cancellationToken = default)
    {
        var reading = await this.gpsManager.GetLastReading(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        if (reading == null)
            return Array.Empty<DocumentCurrentRegion>();

        var position = new GeoPoint(reading.Position.Latitude, reading.Position.Longitude);
        return await this.evaluator.Current(position, cancellationToken).ConfigureAwait(false);
    }


    // Restores the listener after the OS relaunches the app - the persisted region ids are still there, so
    // monitoring picks up where it left off instead of replaying entries.
    async void IShinyStartupTask.Start()
    {
        try
        {
            if (this.IsStarted && this.gpsManager.CurrentListener == null)
                await this.gpsManager.StartListener(this.config.GpsRequest).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.logger.LogWarning(ex, "Failed to restore document geofence monitoring");
        }
    }
}
