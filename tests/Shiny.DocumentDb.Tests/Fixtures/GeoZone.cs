namespace Shiny.DocumentDb.Tests.Fixtures;

/// <summary>Shared spatial test model — a named area mapped as a full geometry.</summary>
public class GeoZone
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public Geometry? Area { get; set; }

    /// <summary>A second geometry that is deliberately never mapped — the fixture for the guard that keeps a
    /// spatial predicate from silently being answered against <see cref="Area"/>.</summary>
    public Geometry? Unmapped { get; set; }
}
