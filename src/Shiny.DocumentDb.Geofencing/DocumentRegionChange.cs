namespace Shiny.DocumentDb.Geofencing;

/// <summary>
/// A geofence transition — the device entered or exited one region of one monitored set.
/// </summary>
/// <param name="RegionSet">The set (document layer) the region belongs to.</param>
/// <param name="RegionId">The region document's id.</param>
/// <param name="RegionName">The display name, when the set was registered with a name selector.</param>
/// <param name="Region">
/// The region document. Null only when an exit is raised for a region that has since been deleted from the store.
/// </param>
/// <param name="Entered">True for an entry, false for an exit.</param>
/// <param name="Position">The GPS position that produced the transition.</param>
public record DocumentRegionChange(
    string RegionSet,
    string RegionId,
    string? RegionName,
    object? Region,
    bool Entered,
    GeoPoint Position
)
{
    /// <summary>
    /// The region document cast to its document type, or null when the type doesn't match (or the region
    /// was deleted before an exit could be described).
    /// </summary>
    public T? RegionAs<T>() where T : class => this.Region as T;
}


/// <summary>
/// The region the device is currently in for one monitored set. Returned by
/// <see cref="IDocumentGeofenceManager.GetCurrent"/>.
/// </summary>
/// <param name="RegionSet">The set that was queried.</param>
/// <param name="RegionId">The matched region's id, or null when the device is outside every region in the set.</param>
/// <param name="RegionName">The display name, when the set was registered with a name selector.</param>
/// <param name="Region">The matched region document, or null when outside.</param>
/// <param name="DistanceMeters">Distance to the matched region — 0 in containment mode.</param>
public record DocumentCurrentRegion(
    string RegionSet,
    string? RegionId,
    string? RegionName,
    object? Region,
    double DistanceMeters
)
{
    /// <summary>The region document cast to its document type, or null when outside / the type doesn't match.</summary>
    public T? RegionAs<T>() where T : class => this.Region as T;
}
