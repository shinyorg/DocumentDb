namespace Shiny.DocumentDb.Internal;

static class GeoMath
{
    const double EarthRadiusMeters = 6_371_000.0;

    /// <summary>
    /// Calculates the Haversine distance in meters between two geographic points.
    /// </summary>
    public static double HaversineDistance(GeoPoint a, GeoPoint b)
    {
        var dLat = ToRadians(b.Latitude - a.Latitude);
        var dLon = ToRadians(b.Longitude - a.Longitude);

        var aLat = ToRadians(a.Latitude);
        var bLat = ToRadians(b.Latitude);

        var h = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                Math.Cos(aLat) * Math.Cos(bLat) *
                Math.Sin(dLon / 2) * Math.Sin(dLon / 2);

        var c = 2 * Math.Atan2(Math.Sqrt(h), Math.Sqrt(1 - h));
        return EarthRadiusMeters * c;
    }

    /// <summary>
    /// Calculates a bounding box that contains all points within the given radius of the center.
    /// </summary>
    public static GeoBoundingBox BoundingBox(GeoPoint center, double radiusMeters)
    {
        var latDelta = radiusMeters / EarthRadiusMeters * (180.0 / Math.PI);
        var lngDelta = latDelta / Math.Cos(ToRadians(center.Latitude));

        return new GeoBoundingBox(
            MinLatitude: center.Latitude - latDelta,
            MinLongitude: center.Longitude - lngDelta,
            MaxLatitude: center.Latitude + latDelta,
            MaxLongitude: center.Longitude + lngDelta);
    }

    static double ToRadians(double degrees) => degrees * (Math.PI / 180.0);
}
