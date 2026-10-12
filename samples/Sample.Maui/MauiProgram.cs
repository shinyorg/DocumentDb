using System.Text.Json;
using Shiny.DocumentDb;
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

            // AOT-safe vector mapping (delegate overload — no expression compilation).
            opts.ConfigureDocument<VectorNote>(cfg => cfg.MapVectorProperty(
                "Embedding",
                n => n.Embedding,
                (n, v) => n.Embedding = v,
                dimensions: 4,
                metric: VectorDistance.Cosine));
        });

        builder.Services.AddTransient<MainPage>();

        return builder.Build();
    }
}
