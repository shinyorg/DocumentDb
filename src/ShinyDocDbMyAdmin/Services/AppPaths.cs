namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Resolves where the tool keeps its own state. Defaults to <c>~/.shinydocdbmyadmin</c>; override with
/// <c>ShinyDocDbMyAdmin:DataDirectory</c> in configuration or the <c>SHINYDOCDBMYADMIN_DATA</c> environment variable.
/// </summary>
public sealed class AppPaths
{
    public AppPaths(IConfiguration configuration)
    {
        var configured = configuration["ShinyDocDbMyAdmin:DataDirectory"]
                         ?? Environment.GetEnvironmentVariable("SHINYDOCDBMYADMIN_DATA");

        this.DataDirectory = string.IsNullOrWhiteSpace(configured)
            ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".shinydocdbmyadmin")
            : Path.GetFullPath(configured);

        this.UploadsDirectory = Path.Combine(this.DataDirectory, "uploads");

        Directory.CreateDirectory(this.DataDirectory);
        Directory.CreateDirectory(this.UploadsDirectory);
    }

    public string DataDirectory { get; }

    /// <summary>Where uploaded SQLite / DuckDB files live. One subdirectory per profile id.</summary>
    public string UploadsDirectory { get; }

    /// <summary>The tool's own document store - connection profiles and saved queries.</summary>
    public string ProfileDatabasePath => Path.Combine(this.DataDirectory, "admin.db");

    public string KeyFilePath => Path.Combine(this.DataDirectory, "secret.key");

    public string UploadDirectoryFor(string profileId) => Path.Combine(this.UploadsDirectory, profileId);
}
