using System.Diagnostics;

namespace Shiny.DocumentDb.Diagnostics;

/// <summary>
/// Implemented by the store bases so an <see cref="IDocumentSession"/> can open a <b>unit-of-work parent span</b>
/// carrying the store's resolved <c>db.system.name</c> (+ namespace). Every operation the session performs then
/// nests under it (via <see cref="Activity.Current"/>), so a unit of work reads as one correlated trace subtree.
/// Zero-cost when unobserved — returns null with no <see cref="ActivityListener"/>.
/// </summary>
interface IUnitScopeSource
{
    /// <summary>Starts a client span for a unit of work (<paramref name="operation"/> e.g. "unit_of_work"), or
    /// null when no telemetry listener is attached. The caller owns disposal.</summary>
    Activity? StartUnitActivity(string operation);

    /// <summary>Records the number of buffered writes flushed by one SaveChanges (the store knows its
    /// <c>db.system.name</c>/namespace). No-op when no meter is subscribed.</summary>
    void RecordUnitSize(long operations);
}
