namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// The single switch that decides whether this build of the tool has an assistant at all.
/// </summary>
/// <remarks>
/// <para>
/// Set <c>ShinyDocDbMyAdmin:DisableAi</c> (env: <c>ShinyDocDbMyAdmin__DisableAi=true</c>) and the
/// feature is gone rather than merely hidden: no Assistant tab, no settings entry point, and the
/// <c>/ai</c> and <c>/assistant</c> routes refuse to render. This is for a public demo instance,
/// where a visible "configure AI" box invites a stranger to paste their own API key into a database
/// browser they do not control.
/// </para>
/// <para>
/// The AI services are also left out of the container when this is set, so the routes have nothing
/// to resolve even if a future page forgets to ask. Hiding a link is not the same as removing a
/// feature, and only one of the two survives someone typing the URL.
/// </para>
/// </remarks>
public sealed class AiAvailability
{
    public const string DisableKey = "ShinyDocDbMyAdmin:DisableAi";

    public AiAvailability(IConfiguration configuration, ILogger<AiAvailability> logger)
    {
        this.IsEnabled = !configuration.GetValue(DisableKey, false);

        if (!this.IsEnabled)
            logger.LogInformation("The assistant is disabled by {Key}; its pages and services are not registered.", DisableKey);
    }

    /// <summary>False when the deployment has switched the assistant off entirely.</summary>
    public bool IsEnabled { get; }

    /// <summary>
    /// Throws when a page that should never have been reachable is reached anyway. Pages call this
    /// first so a deep link produces a refusal rather than a half-rendered assistant.
    /// </summary>
    public void AssertEnabled()
    {
        if (!this.IsEnabled)
            throw new InvalidOperationException("The assistant is disabled on this deployment.");
    }
}
