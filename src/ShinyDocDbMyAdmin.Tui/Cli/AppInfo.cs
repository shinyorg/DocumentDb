using System.Reflection;

namespace ShinyDocDbMyAdmin.Tui.Cli;

/// <summary>What the tool says about itself - the version the <c>version</c> verb prints, and the home
/// page the splash points at.</summary>
public static class AppInfo
{
    public const string Site = "https://shinylib.net";

    /// <summary>
    /// The informational version with the build metadata cut off: Nerdbank.GitVersioning appends
    /// <c>+&lt;commit&gt;</c>, which is noise in a banner and in <c>shinydocdb version</c> alike.
    /// </summary>
    public static string Version { get; } = typeof(AppInfo).Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
        ?.InformationalVersion.Split('+')[0] ?? "unknown";
}
