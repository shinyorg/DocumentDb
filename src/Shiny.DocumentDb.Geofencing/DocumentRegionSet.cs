namespace Shiny.DocumentDb.Geofencing;

/// <summary>
/// A monitored layer of geofence regions — one document type, queried one way. Created by
/// <see cref="DocumentGeofenceConfig.AddRegionSet{T}"/>; the document type is erased into closures so the
/// monitor can hold heterogeneous sets (states, cities, custom zones) without reflection.
/// </summary>
public sealed class DocumentRegionSet
{
    internal DocumentRegionSet(
        string name,
        double? withinMeters,
        Func<IDocumentStore, GeoPoint, CancellationToken, Task<DocumentRegionMatch?>> match,
        Func<IDocumentStore, string, CancellationToken, Task<DocumentRegionMatch?>> byId
    )
    {
        this.Name = name;
        this.WithinMeters = withinMeters;
        this.Match = match;
        this.ById = byId;
    }

    /// <summary>The name this set was registered under — echoed back on every change event.</summary>
    public string Name { get; }

    /// <summary>
    /// When set, the device is considered inside a region while it is within this many meters of the
    /// stored geometry (proximity mode). When null the device must be *inside* the geometry (containment mode).
    /// </summary>
    public double? WithinMeters { get; }

    /// <summary>Runs the spatial query for a position and returns the winning region, or null when outside them all.</summary>
    internal Func<IDocumentStore, GeoPoint, CancellationToken, Task<DocumentRegionMatch?>> Match { get; }

    /// <summary>Re-reads a region by id — used to describe an exit whose region is no longer in memory.</summary>
    internal Func<IDocumentStore, string, CancellationToken, Task<DocumentRegionMatch?>> ById { get; }
}


/// <summary>
/// The region a position resolved to inside one <see cref="DocumentRegionSet"/>.
/// </summary>
/// <param name="Id">The region document's id.</param>
/// <param name="Name">A display name pulled from the document, when the set was registered with a name selector.</param>
/// <param name="Document">The region document itself.</param>
/// <param name="DistanceMeters">Distance from the position to the region — always 0 in containment mode.</param>
internal record DocumentRegionMatch(string Id, string? Name, object Document, double DistanceMeters);
