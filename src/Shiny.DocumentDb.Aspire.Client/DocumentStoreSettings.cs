namespace Shiny.DocumentDb.Aspire.Client;

/// <summary>
/// Client-side settings for an Aspire-wired DocumentDb store. Values left null/false fall back to the
/// host-injected configuration (connection string + provider discriminator).
/// </summary>
public sealed class DocumentStoreSettings
{
    /// <summary>Overrides the host-injected <c>ConnectionStrings:&lt;name&gt;</c>.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Overrides the host-injected <c>Shiny:DocumentDb:&lt;name&gt;:Provider</c> discriminator.</summary>
    public DocumentProviderKind? Provider { get; set; }

    /// <summary>When true, the trivial store health check is not registered.</summary>
    public bool DisableHealthChecks { get; set; }

    /// <summary>When true, the <c>Shiny.DocumentDb</c> ActivitySource is not wired into tracing.</summary>
    public bool DisableTracing { get; set; }

    /// <summary>When true, the <c>Shiny.DocumentDb</c> meter is not wired into metrics.</summary>
    public bool DisableMetrics { get; set; }
}
