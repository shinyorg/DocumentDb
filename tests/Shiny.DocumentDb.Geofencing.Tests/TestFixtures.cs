using Microsoft.Extensions.Logging.Abstractions;
using Shiny.DocumentDb.Geofencing;
using Shiny.Extensions.Stores;

namespace Shiny.DocumentDb.Geofencing.Tests;

/// <summary>A polygon region - the shape a state/city boundary or a delivery zone takes.</summary>
public class Zone
{
    public string Id { get; set; } = null!;
    public string Name { get; set; } = null!;
    public bool Active { get; set; } = true;
    public Geometry Boundary { get; set; } = null!;
}


/// <summary>A point region - only ever matchable in proximity mode.</summary>
public class Shop
{
    public string Id { get; set; } = null!;
    public string Name { get; set; } = null!;
    public GeoPoint Location { get; set; }
}


/// <summary>Collects every transition the evaluator raises, optionally throwing first.</summary>
public class RecordingDelegate : IDocumentGeofenceDelegate
{
    public List<DocumentRegionChange> Changes { get; } = new();
    public bool Throw { get; set; }

    public Task OnRegionChanged(DocumentRegionChange change)
    {
        this.Changes.Add(change);
        if (this.Throw)
            throw new InvalidOperationException("delegate blew up");

        return Task.CompletedTask;
    }
}


public static class Geo
{
    /// <summary>An axis-aligned box, as a closed ring.</summary>
    public static GeoPolygon Box(double minLat, double minLng, double maxLat, double maxLng) => new(new[]
    {
        new GeoPoint(minLat, minLng),
        new GeoPoint(minLat, maxLng),
        new GeoPoint(maxLat, maxLng),
        new GeoPoint(maxLat, minLng),
        new GeoPoint(minLat, minLng)
    });

    public static DocumentRegionEvaluator Evaluator(
        IDocumentStore store,
        IKeyValueStore state,
        IDocumentGeofenceDelegate del,
        DocumentGeofenceConfig config
    ) => new(store, state, del, config, NullLogger<DocumentRegionEvaluator>.Instance);
}
