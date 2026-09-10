namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// When a <see cref="DocumentStoreResourceBuilderExtensions.WithSeeder"/> callback runs.
/// </summary>
/// <remarks>
/// Either mode only ever seeds on one of two triggers: the <b>first time the database is set up</b>,
/// or a <b>destructive recreation</b>. The "has this store been seeded" marker lives in the database
/// itself, so destroying the data (dropping the container volume, deleting the SQLite file) also
/// destroys the marker and the next AppHost start seeds again.
/// </remarks>
public enum DocumentStoreSeedMode
{
    /// <summary>
    /// Seed only when the store has no seed marker — i.e. the database is brand new, or its data was
    /// destroyed since the last run. Steady-state AppHost restarts do nothing.
    /// </summary>
    FirstTimeOnly,

    /// <summary>
    /// Seed on first-time setup <b>and</b> on every subsequent start, wiping first. When an existing
    /// marker is found the callback is invoked with <see cref="DocumentStoreSeedContext.Recreate"/>
    /// set — the callback owns the wipe (it holds the store, so it can call
    /// <c>IDocumentMaintenance.ClearAll</c>); the AppHost never issues DDL of its own.
    /// </summary>
    Recreate
}
