namespace Shiny.DocumentDb.Aspire.Hosting.Internal;

/// <summary>
/// The host↔client contract for the provider discriminator.
/// The hosting integration writes the value; the client integration reads it.
/// Keep the spelling of <see cref="DocumentProviderKind"/> identical across both halves.
/// </summary>
internal static class DocumentStoreConstants
{
    /// <summary>Config key (colon form): <c>Shiny:DocumentDb:{name}:Provider</c>.</summary>
    public static string ProviderConfigKey(string name) => $"Shiny:DocumentDb:{name}:Provider";

    /// <summary>Environment-variable form: <c>Shiny__DocumentDb__{name}__Provider</c>.</summary>
    public static string ProviderEnvVar(string name) => $"Shiny__DocumentDb__{name}__Provider";
}
