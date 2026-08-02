using System.Text.Json;
using System.Text.Json.Serialization;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tui.Shell;

/// <summary>
/// The handful of preferences that belong to the terminal front end rather than to a connection.
/// </summary>
/// <remarks>
/// A plain JSON file in the data directory rather than a document in <c>admin.db</c>: that database is
/// the shared surface both front ends read, and "which theme did the terminal use last" is not
/// something the web app should find in there. Anything that <i>is</i> shared - profiles, AI settings,
/// saved queries - stays in the store.
/// </remarks>
public sealed class UiSettings
{
    static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    readonly string path;

    public UiSettings(AppPaths paths)
    {
        this.path = Path.Combine(paths.DataDirectory, "tui-settings.json");

        try
        {
            if (File.Exists(this.path) && JsonSerializer.Deserialize(File.ReadAllText(this.path), UiSettingsJsonContext.Default.UiSettingsFile) is { } loaded)
            {
                this.Theme = loaded.Theme ?? "dark";
                this.ExplorerRatio = loaded.ExplorerRatio ?? 0.24;
                this.PageSize = loaded.PageSize ?? 25;
            }
        }
        catch (Exception)
        {
            // Preferences are not worth failing a start over. Defaults it is.
        }
    }

    public string Theme { get; set; } = "dark";

    public double ExplorerRatio { get; set; } = 0.24;

    /// <summary>Rows per page in the browse grid. Matches the web front end's default.</summary>
    public int PageSize { get; set; } = 25;

    public void Save()
    {
        try
        {
            var file = new UiSettingsFile { Theme = this.Theme, ExplorerRatio = this.ExplorerRatio, PageSize = this.PageSize };
            File.WriteAllText(this.path, JsonSerializer.Serialize(file, UiSettingsJsonContext.Default.UiSettingsFile));
        }
        catch (Exception)
        {
            // Same reasoning as the load: losing a theme choice is not worth an error dialog.
        }
    }
}

public sealed class UiSettingsFile
{
    public string? Theme { get; set; }
    public double? ExplorerRatio { get; set; }
    public int? PageSize { get; set; }
}

[JsonSourceGenerationOptions(WriteIndented = true, PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(UiSettingsFile))]
public partial class UiSettingsJsonContext : JsonSerializerContext;
