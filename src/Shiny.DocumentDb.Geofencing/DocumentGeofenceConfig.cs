using System.Linq.Expressions;
using Shiny.Locations;

namespace Shiny.DocumentDb.Geofencing;

/// <summary>
/// Configures which document types act as geofence regions and how aggressively GPS readings are processed.
/// </summary>
public class DocumentGeofenceConfig
{
    readonly List<DocumentRegionSet> regionSets = new();

    /// <summary>
    /// The keyed document store to query, or null for the default (unkeyed) store.
    /// </summary>
    public string? StoreName { get; set; }

    /// <summary>
    /// The GPS listener configuration. Defaults to realtime background tracking — the reading filters below
    /// are what keep the work (and the battery cost of processing) down, not the listener frequency.
    /// </summary>
    public GpsRequest GpsRequest { get; set; } = new(GpsBackgroundMode.Realtime);

    /// <summary>Minimum distance the device must move before a reading is evaluated. Default 300m.</summary>
    public Distance? MinimumDistance { get; set; } = Distance.FromMeters(300);

    /// <summary>Minimum time between evaluated readings. Default 1 minute.</summary>
    public TimeSpan? MinimumTime { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Safety net — once the device has moved this far a reading is always evaluated, even if
    /// <see cref="MinimumTime"/> hasn't elapsed. Unset by default.
    /// </summary>
    public Distance? MaximumDistance { get; set; }

    /// <summary>
    /// Safety net — once this much time has passed a reading is always evaluated, even if
    /// <see cref="MinimumDistance"/> hasn't been met. Unset by default.
    /// </summary>
    public TimeSpan? MaximumTime { get; set; }

    /// <summary>The registered region sets.</summary>
    public IReadOnlyList<DocumentRegionSet> RegionSets => this.regionSets;


    /// <summary>
    /// Registers a document type as a layer of geofence regions. The type must have a geometry mapped with
    /// <c>MapSpatialProperty</c>, and the store's provider must support spatial queries.
    /// <para>
    /// Register one set per logical layer (states, cities, delivery zones) — the monitor tracks the device's
    /// current region independently per set, so a device can be inside one region of each at the same time.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The region document type.</typeparam>
    /// <param name="name">A stable name for the set — echoed on every change and used as the persistence key.</param>
    /// <param name="idSelector">
    /// Returns the document's id. This must be the same value the store keys the document by, since it is
    /// used to re-read a region when describing an exit after an app restart.
    /// </param>
    /// <param name="nameSelector">Optional display name pulled onto each change event.</param>
    /// <param name="withinMeters">
    /// Null (default) for containment — the device is inside the region when the GPS point falls inside the
    /// stored geometry. Set it to switch to proximity, which is what point regions (a city centre, a store
    /// location) need since a GPS point never falls "inside" another point.
    /// </param>
    /// <param name="filter">Optional predicate narrowing which documents of this type are monitored.</param>
    public DocumentGeofenceConfig AddRegionSet<T>(
        string name,
        Func<T, string> idSelector,
        Func<T, string?>? nameSelector = null,
        double? withinMeters = null,
        Expression<Func<T, bool>>? filter = null
    ) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(idSelector);

        if (withinMeters is <= 0)
            throw new ArgumentOutOfRangeException(nameof(withinMeters), withinMeters, "Must be greater than zero.");

        if (this.regionSets.Any(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException($"A region set named '{name}' is already registered.", nameof(name));

        this.regionSets.Add(new DocumentRegionSet(
            name,
            withinMeters,
            async (store, point, ct) =>
            {
                Geometry position = point;

                // orderByDistanceFrom makes the store sort by proximity, so the first hit is the winner when
                // overlapping regions match (identical 0m distances in containment mode fall back to store order).
                var hits = withinMeters is null
                    ? await store.GeoIntersects<T>(position, orderByDistanceFrom: position, filter: filter, cancellationToken: ct).ConfigureAwait(false)
                    : await store.GeoWithinDistance<T>(position, withinMeters.Value, orderByDistanceFrom: position, filter: filter, cancellationToken: ct).ConfigureAwait(false);

                if (hits.Count == 0)
                    return null;

                var hit = hits[0];
                return new DocumentRegionMatch(idSelector(hit.Document), nameSelector?.Invoke(hit.Document), hit.Document, hit.DistanceMeters);
            },
            async (store, id, ct) =>
            {
                var doc = await store.Get<T>(id, cancellationToken: ct).ConfigureAwait(false);
                return doc is null ? null : new DocumentRegionMatch(idSelector(doc), nameSelector?.Invoke(doc), doc, 0);
            }
        ));
        return this;
    }
}
