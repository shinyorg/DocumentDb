namespace Shiny.DocumentDb.Aspire.Client.Internal;

internal static class DocumentStoreClientConstants
{
    /// <summary>The meter and ActivitySource name emitted by Shiny.DocumentDb.Diagnostics.</summary>
    public const string TelemetryName = "Shiny.DocumentDb";

    /// <summary>Config key (colon form) the host writes the provider discriminator to.</summary>
    public static string ProviderConfigKey(string name) => $"Shiny:DocumentDb:{name}:Provider";
}
