namespace Shiny.DocumentDb.Internal;

class SpatialMapping
{
    public required Type DocumentType { get; init; }
    public required string PropertyName { get; init; }
    public required string JsonPath { get; set; }
    public required Func<object, GeoPoint> GetGeoPoint { get; init; }
}
