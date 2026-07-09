namespace Shiny.DocumentDb.Tests.Fixtures;

/// <summary>Shared spatial test model — a named area mapped as a full geometry.</summary>
public class GeoZone
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public Geometry? Area { get; set; }
}
