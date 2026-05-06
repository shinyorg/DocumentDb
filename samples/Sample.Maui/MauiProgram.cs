using System.Text.Json;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;

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
            opts.DatabaseProvider = new SqliteDatabaseProvider($"Data Source={dbPath}");
            opts.JsonSerializerOptions = jsonContext.Options;
            opts.UseReflectionFallback = false;
            opts.MapTypeToTable<Customer>();
            opts.MapTypeToTable<Order>();
        });

        builder.Services.AddTransient<MainPage>();

        return builder.Build();
    }
}
