using System.Reflection;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// What the tool says about itself — the version both front ends display, and the home page they link to.
/// </summary>
/// <remarks>
/// In the shared layer rather than in either front end because the two must not disagree about which
/// version they are: the web app's footer and <c>shinydocdb version</c> answer the same question, and
/// every project in the repo is stamped from the same <c>version.json</c>.
/// </remarks>
public static class AppInfo
{
    public const string Site = "https://shinylib.net";

    /// <summary>
    /// The informational version with the build metadata cut off: Nerdbank.GitVersioning appends
    /// <c>+&lt;commit&gt;</c>, which is noise in a banner, a footer and <c>shinydocdb version</c> alike.
    /// </summary>
    public static string Version { get; } = typeof(AppInfo).Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
        ?.InformationalVersion.Split('+')[0] ?? "unknown";
}
