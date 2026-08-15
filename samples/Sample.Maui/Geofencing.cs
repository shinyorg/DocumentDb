using Shiny.DocumentDb.Geofencing;

namespace Sample.Maui;

/// <summary>
/// The app's geofence handler. Invoked on a background thread - including while the app is backgrounded -
/// once per region entered or exited, so it does no UI work itself; it appends to <see cref="GeofenceLog"/>
/// and the page renders from there.
/// </summary>
public class SampleGeofenceDelegate(GeofenceLog log) : IDocumentGeofenceDelegate
{
    public Task OnRegionChanged(DocumentRegionChange change)
    {
        // change.Region is the document itself, not just an identifier.
        var city = change.RegionAs<GeofenceZone>()?.City
            ?? change.RegionAs<Landmark>()?.City
            ?? "";

        log.Add(new GeofenceEntry(
            DateTime.Now.ToString("HH:mm:ss"),
            change.Entered ? "Entered" : "Exited",
            change.RegionSet,
            change.RegionName ?? change.RegionId,
            city
        ));
        return Task.CompletedTask;
    }
}


/// <summary>A capped, thread-safe transition log the geofence tab renders.</summary>
public class GeofenceLog
{
    const int MaxEntries = 100;

    readonly List<GeofenceEntry> entries = new();
    readonly object syncLock = new();

    /// <summary>Raised on the background thread that recorded the transition.</summary>
    public event EventHandler? Changed;

    public IReadOnlyList<GeofenceEntry> Entries
    {
        get
        {
            lock (this.syncLock)
                return this.entries.ToList();
        }
    }

    public void Add(GeofenceEntry entry)
    {
        lock (this.syncLock)
        {
            this.entries.Insert(0, entry);          // newest first
            if (this.entries.Count > MaxEntries)
                this.entries.RemoveRange(MaxEntries, this.entries.Count - MaxEntries);
        }
        this.Changed?.Invoke(this, EventArgs.Empty);
    }

    public void Clear()
    {
        lock (this.syncLock)
            this.entries.Clear();

        this.Changed?.Invoke(this, EventArgs.Empty);
    }
}


public record GeofenceEntry(string Time, string Verb, string RegionSet, string Region, string City);
