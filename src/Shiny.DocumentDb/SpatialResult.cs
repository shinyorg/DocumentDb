namespace Shiny.DocumentDb;

/// <summary>
/// A document with its computed distance from the query center point.
/// </summary>
public class SpatialResult<T> where T : class
{
    public required T Document { get; init; }
    public double DistanceMeters { get; init; }
}
