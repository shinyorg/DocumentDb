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

    /// <summary>
    /// When true, spatial storage uses the dependency-free envelope tier instead of the provider's native
    /// geometry column — no PostGIS on PostgreSQL, no <c>geometry</c> column or GiST/spatial index, and no
    /// native <c>ST_*</c> pushdown for <c>DocumentFunctions</c> spatial predicates in <c>Where</c> (the
    /// dedicated <c>Geo*</c> bounding-box methods work either way). Set this when you can't install or
    /// enable the backend's spatial extension. Ignored by providers with no native spatial tier.
    /// </summary>
    public bool PortableSpatial { get; set; }

    /// <summary>
    /// When true, the store is registered as a shared-table multi-tenant store: a TenantId column is
    /// added to the schema and every query is filtered by the current tenant. The tenant is resolved
    /// on each operation via <see cref="ITenantResolver"/>, which must be registered in the container.
    /// </summary>
    public bool MultiTenant { get; set; }
}
