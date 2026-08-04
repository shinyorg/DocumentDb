using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb.Geo;

/// <summary>
/// Registration helpers for the reference geographic dataset.
/// </summary>
public static class GeoServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="GeoReferenceSeeder"/> so the embedded US/Canada regions and cities are
    /// written into the target store once at host startup.
    /// <para>
    /// Call <see cref="MapGeoReferenceData"/> in your <c>AddDocumentStore</c> options if you intend to run
    /// spatial queries against <see cref="GeoRegion.Boundary"/> / <see cref="GeoCity.Location"/>.
    /// </para>
    /// </summary>
    /// <param name="storeName">The keyed store to seed, or null for the default store.</param>
    public static IServiceCollection AddGeoReferenceSeeder(this IServiceCollection services, string? storeName = null)
        => services.AddDocumentSeeder(new GeoReferenceSeeder(), storeName);

    /// <summary>
    /// Maps the spatial properties of <see cref="GeoRegion"/> and <see cref="GeoCity"/> so containment /
    /// proximity queries work (on providers that support spatial indexing, e.g. SQLite and CosmosDB).
    /// </summary>
    public static DocumentStoreOptions MapGeoReferenceData(this DocumentStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.ConfigureDocument<GeoRegion>(cfg => cfg.MapSpatialProperty(nameof(GeoRegion.Boundary), r => r.Boundary));
        options.ConfigureDocument<GeoCity>(cfg => cfg.MapSpatialProperty(nameof(GeoCity.Location), c => c.Location));
        return options;
    }
}
