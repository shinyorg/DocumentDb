namespace Shiny.DocumentDb.Geofencing;

/// <summary>
/// Receives geofence transitions. Implement this and register it with
/// <c>AddDocumentGeofencing&lt;TDelegate&gt;</c>; it is resolved from the container as a singleton and is
/// invoked on a background thread, including while the app is backgrounded.
/// </summary>
public interface IDocumentGeofenceDelegate
{
    /// <summary>
    /// Invoked once per region transition. Moving straight from region A to region B in the same set
    /// raises two calls — the exit from A first, then the entry into B.
    /// </summary>
    /// <param name="change">The transition.</param>
    Task OnRegionChanged(DocumentRegionChange change);
}
