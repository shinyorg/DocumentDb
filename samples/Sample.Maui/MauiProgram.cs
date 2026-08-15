using System.Text.Json;
using Shiny;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Geofencing;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Sqlite.VectorSupport;

namespace Sample.Maui;

public static class MauiProgram
{
    public static MauiApp CreateMauiApp()
    {
        var builder = MauiApp.CreateBuilder();
        builder
            .UseMauiApp<App>()
            // Geofencing runs as a Shiny startup task (so monitoring restores after the OS relaunches the
            // app) and persists its current-region state in Shiny's key/value store.
            .UseShiny()
            .ConfigureFonts(fonts =>
            {
                fonts.AddFont("OpenSans-Regular.ttf", "OpenSansRegular");
            });

        var dbPath = Path.Combine(FileSystem.AppDataDirectory, "sample.db");
        var jsonContext = new AppJsonContext(new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        builder.Services.AddDocumentStore(opts =>
        {
            // SqliteVec.CreateProvider registers the bundled sqlite-vec native binary as a SQLite
            // auto-extension (the only approach that works on iOS) and returns a provider with
            // VectorExtensionPreloaded set, so vector search works on iOS/Android/desktop alike.
            opts.DatabaseProvider = SqliteVec.CreateProvider($"Data Source={dbPath}");
            opts.JsonSerializerOptions = jsonContext.Options;
            opts.UseReflectionFallback = false;
            opts.ConfigureDocument<Customer>(cfg => cfg.Table = cfg.TypeName);
            opts.ConfigureDocument<Order>(cfg => cfg.Table = cfg.TypeName);
            opts.ConfigureDocument<VectorNote>(cfg => cfg.Table = cfg.TypeName);
            opts.ConfigureDocument<GeofenceZone>(cfg => cfg.Table = cfg.TypeName);
            opts.ConfigureDocument<Landmark>(cfg => cfg.Table = cfg.TypeName);

            // AOT-safe spatial mapping (property name + delegate — no expression compilation). A geofence
            // region set can only monitor a type whose geometry is mapped here.
            opts.ConfigureDocument<GeofenceZone>(cfg => cfg.MapSpatialProperty(nameof(GeofenceZone.Boundary), z => z.Boundary));
            opts.ConfigureDocument<Landmark>(cfg => cfg.MapSpatialProperty(nameof(Landmark.Location), l => l.Location));

            // AOT-safe vector mapping (delegate overload — no expression compilation).
            opts.ConfigureDocument<VectorNote>(cfg => cfg.MapVectorProperty(
                "Embedding",
                n => n.Embedding,
                (n, v) => n.Embedding = v,
                dimensions: 4,
                metric: VectorDistance.Cosine));
        });

        builder.Services.AddSingleton<GeofenceLog>();

        // Background GPS drives one spatial query per reading against each region set. "zones" are polygons
        // matched by containment; "landmarks" are points, which only ever match by proximity - hence
        // withinMeters. Each set tracks its current region independently.
        builder.Services.AddDocumentGeofencing<SampleGeofenceDelegate>(cfg =>
        {
            cfg.MinimumDistance = Distance.FromMeters(250);
            cfg.MinimumTime = TimeSpan.FromSeconds(30);

            cfg.AddRegionSet<GeofenceZone>("zones", z => z.Id, z => z.Name, filter: z => z.Active)
               .AddRegionSet<Landmark>("landmarks", l => l.Id, l => l.Name, withinMeters: 1000);
        });

        builder.Services.AddTransient<MainPage>();

        return builder.Build();
    }
}
