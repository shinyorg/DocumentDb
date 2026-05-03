namespace Shiny.DocumentDb;

/// <summary>
/// Represents a geographic bounding box for spatial queries.
/// </summary>
public readonly record struct GeoBoundingBox(
    double MinLatitude, double MinLongitude,
    double MaxLatitude, double MaxLongitude);
