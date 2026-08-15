using Microsoft.Extensions.Logging;
using Shiny.Locations;

namespace Shiny.DocumentDb.Geofencing;

/// <summary>
/// The GPS listener behind document geofencing. Registered as the app's <see cref="IGpsDelegate"/> by
/// <c>AddDocumentGeofencing</c>; every reading that survives the configured distance/time filters is
/// evaluated against the monitored region sets.
/// </summary>
public class DocumentGeofenceGpsDelegate : GpsDelegate
{
    readonly DocumentRegionEvaluator evaluator;


    /// <summary>Creates the delegate and applies the reading filters from <paramref name="config"/>.</summary>
    public DocumentGeofenceGpsDelegate(
        DocumentRegionEvaluator evaluator,
        DocumentGeofenceConfig config,
        ILogger<DocumentGeofenceGpsDelegate> logger
    ) : base(logger)
    {
        this.evaluator = evaluator;

        this.MinimumDistance = config.MinimumDistance;
        this.MinimumTime = config.MinimumTime;
        this.MaximumDistance = config.MaximumDistance;
        this.MaximumTime = config.MaximumTime;
    }


    /// <inheritdoc />
    protected override async Task OnGpsReading(GpsReading reading)
    {
        var position = new GeoPoint(reading.Position.Latitude, reading.Position.Longitude);
        var changes = await this.evaluator.Evaluate(position).ConfigureAwait(false);

        if (changes.Count > 0)
            this.Logger.LogDebug("Geofence reading produced {Count} transition(s)", changes.Count);
    }
}
