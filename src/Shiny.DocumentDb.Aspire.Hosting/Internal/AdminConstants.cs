namespace Shiny.DocumentDb.Aspire.Hosting.Internal;

/// <summary>
/// The configuration keys ShinyDocDbMyAdmin reads, in environment-variable form.
/// </summary>
/// <remarks>
/// Both front ends are the same Core over the same <c>IConfiguration</c>, so the container and the
/// terminal tool take the same keys - which is why these live in one place rather than once per
/// resource.
/// </remarks>
internal static class AdminConstants
{
    /// <summary>Encrypts the secret-bearing parts of a saved connection.</summary>
    public const string SecretKeyEnvVar = "ShinyDocDbMyAdmin__SecretKey";

    /// <summary>Blocks every write path across every store the host hands over.</summary>
    public const string ReadOnlyEnvVar = "ShinyDocDbMyAdmin__ReadOnly";

    /// <summary>Removes the AI assistant from the front end entirely.</summary>
    public const string DisableAiEnvVar = "ShinyDocDbMyAdmin__DisableAi";

    /// <summary>
    /// Where the tool keeps its own state. The underscore-free spelling is deliberate: the terminal
    /// tool resolves its settings file from this variable <em>before</em> configuration is built, so
    /// the <c>ShinyDocDbMyAdmin__DataDirectory</c> form would move the profile store without moving
    /// the settings file that sits beside it.
    /// </summary>
    public const string DataDirectoryEnvVar = "SHINYDOCDBMYADMIN_DATA";
}
