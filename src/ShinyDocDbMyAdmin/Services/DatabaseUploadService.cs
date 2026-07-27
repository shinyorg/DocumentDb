using ShinyDocDbMyAdmin.Providers;
using Microsoft.AspNetCore.Components.Forms;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>Takes an uploaded SQLite / DuckDB file and parks it in the profile's own directory.</summary>
public sealed class DatabaseUploadService(AppPaths paths, ILogger<DatabaseUploadService> logger)
{
    /// <summary>Upload ceiling. Generous, because a document database of any age is easily hundreds of MB.</summary>
    public const long MaxUploadBytes = 2L * 1024 * 1024 * 1024;

    public async Task<UploadResult> Save(string profileId, ProviderKind provider, IBrowserFile file, CancellationToken ct = default)
    {
        var descriptor = ProviderCatalog.Get(provider);
        if (!descriptor.IsFileBased)
            throw new InvalidOperationException($"{descriptor.DisplayName} is a server connection, not a file.");

        if (file.Size > MaxUploadBytes)
            throw new InvalidOperationException($"{file.Name} is {Humanize(file.Size)}; the limit is {Humanize(MaxUploadBytes)}.");

        // Never trust the browser-supplied name for anything but its basename.
        var safeName = Path.GetFileName(file.Name);
        if (string.IsNullOrWhiteSpace(safeName))
            safeName = "database.db";

        var directory = paths.UploadDirectoryFor(profileId);
        Directory.CreateDirectory(directory);

        // One database per profile: replace whatever was there rather than accumulating orphans.
        foreach (var stale in Directory.EnumerateFiles(directory))
            File.Delete(stale);

        var destination = Path.Combine(directory, safeName);
        await using (var target = File.Create(destination))
        await using (var source = file.OpenReadStream(MaxUploadBytes, ct))
        {
            await source.CopyToAsync(target, ct);
        }

        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(destination, UnixFileMode.UserRead | UnixFileMode.UserWrite);

        logger.LogInformation("Stored uploaded database {File} ({Size}) for profile {Profile}", safeName, Humanize(file.Size), profileId);
        return new UploadResult(safeName, destination, file.Size);
    }

    public static string Humanize(long bytes) => bytes switch
    {
        < 1024 => $"{bytes} B",
        < 1024 * 1024 => $"{bytes / 1024.0:0.#} KB",
        < 1024L * 1024 * 1024 => $"{bytes / (1024.0 * 1024):0.#} MB",
        _ => $"{bytes / (1024.0 * 1024 * 1024):0.##} GB"
    };
}

public sealed record UploadResult(string FileName, string FullPath, long Bytes);
