namespace Shiny.DocumentDb.Internal;

static class GeoBoundingBoxExtensions
{
    public static bool Contains(this GeoBoundingBox box, GeoPoint p)
        => p.Latitude >= box.MinLatitude && p.Latitude <= box.MaxLatitude &&
           p.Longitude >= box.MinLongitude && p.Longitude <= box.MaxLongitude;

    public static bool Intersects(this GeoBoundingBox a, GeoBoundingBox b)
        => a.MinLatitude <= b.MaxLatitude && a.MaxLatitude >= b.MinLatitude &&
           a.MinLongitude <= b.MaxLongitude && a.MaxLongitude >= b.MinLongitude;

    /// <summary>Expands the box by an approximate metric distance (degrees derived from meters).</summary>
    public static GeoBoundingBox ExpandByMeters(this GeoBoundingBox box, double meters)
    {
        const double metersPerDegLat = 111_320.0;
        var latExpand = meters / metersPerDegLat;
        var midLat = (box.MinLatitude + box.MaxLatitude) / 2.0;
        var cos = Math.Cos(midLat * Math.PI / 180.0);
        var lngExpand = cos <= 1e-12 ? 180.0 : meters / (metersPerDegLat * cos);

        return new GeoBoundingBox(
            box.MinLatitude - latExpand,
            box.MinLongitude - lngExpand,
            box.MaxLatitude + latExpand,
            box.MaxLongitude + lngExpand);
    }
}
