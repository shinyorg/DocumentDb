using Microsoft.Extensions.Logging;
using Shiny.Extensions.Stores;

namespace Shiny.DocumentDb.Geofencing;

/// <summary>
/// Turns GPS positions into region transitions: queries each registered set, diffs the result against the
/// region the device was last known to be in, and raises enter/exit through the app's delegate.
/// <para>
/// The "last region" per set is persisted, so a device that crosses a boundary, has its process killed, and
/// is relaunched by the OS does not replay an entry it already raised.
/// </para>
/// </summary>
public class DocumentRegionEvaluator
{
    const string KeyPrefix = "documentdb.geofence.";

    readonly IDocumentStore store;
    readonly IKeyValueStore state;
    readonly IDocumentGeofenceDelegate geofenceDelegate;
    readonly DocumentGeofenceConfig config;
    readonly ILogger logger;

    // Mirrors the persisted ids, but carries the document too - the persisted key only survives the id, and
    // an exit event should describe the region without a round-trip when the process hasn't been recycled.
    readonly Dictionary<string, DocumentRegionMatch> lastMatches = new();


    /// <summary>Creates the evaluator. Registered as a singleton by <c>AddDocumentGeofencing</c>.</summary>
    public DocumentRegionEvaluator(
        IDocumentStore store,
        IKeyValueStore state,
        IDocumentGeofenceDelegate geofenceDelegate,
        DocumentGeofenceConfig config,
        ILogger<DocumentRegionEvaluator> logger
    )
    {
        this.store = store;
        this.state = state;
        this.geofenceDelegate = geofenceDelegate;
        this.config = config;
        this.logger = logger;
    }


    /// <summary>
    /// Evaluates a position against every registered set and raises the resulting transitions.
    /// </summary>
    /// <param name="position">The position to evaluate.</param>
    /// <param name="cancellationToken">Cancels the pending queries.</param>
    /// <returns>The transitions that were raised, in the order they were delivered.</returns>
    public async Task<IReadOnlyList<DocumentRegionChange>> Evaluate(GeoPoint position, CancellationToken cancellationToken = default)
    {
        var changes = new List<DocumentRegionChange>();

        foreach (var set in this.config.RegionSets)
        {
            DocumentRegionMatch? current;
            try
            {
                current = await set.Match(this.store, position, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // One broken set (an unmapped type, a dropped connection) must not stop the others from
                // reporting - and must not poison this set's last-known region either.
                this.logger.LogError(ex, "Geofence query failed for region set '{RegionSet}'", set.Name);
                continue;
            }

            var previousId = this.GetPersistedId(set.Name);
            if (previousId == current?.Id)
                continue;

            if (previousId != null)
            {
                var previous = await this.ResolvePrevious(set, previousId, cancellationToken).ConfigureAwait(false);
                changes.Add(new DocumentRegionChange(
                    set.Name,
                    previousId,
                    previous?.Name,
                    previous?.Document,
                    Entered: false,
                    position
                ));
            }

            if (current != null)
            {
                changes.Add(new DocumentRegionChange(
                    set.Name,
                    current.Id,
                    current.Name,
                    current.Document,
                    Entered: true,
                    position
                ));
            }

            this.SetCurrent(set.Name, current);
        }

        foreach (var change in changes)
            await this.Fire(change).ConfigureAwait(false);

        return changes;
    }


    /// <summary>
    /// Resolves the region the position falls in for every set without raising events or recording state.
    /// </summary>
    /// <param name="position">The position to resolve.</param>
    /// <param name="cancellationToken">Cancels the pending queries.</param>
    public async Task<IReadOnlyList<DocumentCurrentRegion>> Current(GeoPoint position, CancellationToken cancellationToken = default)
    {
        var results = new List<DocumentCurrentRegion>(this.config.RegionSets.Count);

        foreach (var set in this.config.RegionSets)
        {
            var match = await set.Match(this.store, position, cancellationToken).ConfigureAwait(false);
            results.Add(new DocumentCurrentRegion(set.Name, match?.Id, match?.Name, match?.Document, match?.DistanceMeters ?? 0));
        }
        return results;
    }


    /// <summary>Forgets which region the device was in for every set, so the next reading re-enters from scratch.</summary>
    public void Reset()
    {
        foreach (var set in this.config.RegionSets)
            this.state.Remove(KeyPrefix + set.Name);

        this.lastMatches.Clear();
    }


    async Task Fire(DocumentRegionChange change)
    {
        try
        {
            await this.geofenceDelegate.OnRegionChanged(change).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.logger.LogError(
                ex,
                "Geofence delegate threw handling {Transition} of '{RegionId}' in set '{RegionSet}'",
                change.Entered ? "entry" : "exit",
                change.RegionId,
                change.RegionSet
            );
        }
    }


    async Task<DocumentRegionMatch?> ResolvePrevious(DocumentRegionSet set, string previousId, CancellationToken cancellationToken)
    {
        if (this.lastMatches.TryGetValue(set.Name, out var cached) && cached.Id == previousId)
            return cached;

        // The id outlived the process (or the cache) - read the region back so the exit still describes it.
        try
        {
            return await set.ById(this.store, previousId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.logger.LogWarning(ex, "Could not re-read exited region '{RegionId}' in set '{RegionSet}'", previousId, set.Name);
            return null;
        }
    }


    string? GetPersistedId(string setName)
    {
        var value = this.state.Get<string>(KeyPrefix + setName);
        return String.IsNullOrWhiteSpace(value) ? null : value;
    }


    void SetCurrent(string setName, DocumentRegionMatch? match)
    {
        if (match == null)
        {
            this.state.Remove(KeyPrefix + setName);
            this.lastMatches.Remove(setName);
        }
        else
        {
            this.state.Set(KeyPrefix + setName, match.Id);
            this.lastMatches[setName] = match;
        }
    }
}
