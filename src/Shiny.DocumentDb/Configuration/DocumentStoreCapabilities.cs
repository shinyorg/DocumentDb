namespace Shiny.DocumentDb;

/// <summary>
/// What the backend behind a store's options can actually do. Declared by each options class, and read by
/// <see cref="DocumentConfigurationValidator"/> so mapping a feature the provider does not have is one clear
/// error at startup instead of a confusing failure at first use.
/// <para>
/// This is a statement about the <b>backend</b>, not about what has been mapped — do not confuse it with a
/// store's <c>SupportsVector</c>/<c>SupportsFullText</c>, which on the document providers report whether
/// anything is mapped.
/// </para>
/// </summary>
public sealed record DocumentStoreCapabilities
{
    /// <summary>The backend's display name, used in error messages ("SQLite", "LiteDB", …).</summary>
    public required string ProviderName { get; init; }

    /// <summary>The backend gives a document type its own storage unit — <c>cfg.Table</c> means something.</summary>
    public bool PerTypeStorageName { get; init; }

    /// <summary>Spatial (point / geometry) queries.</summary>
    public bool Spatial { get; init; }

    /// <summary>ANN vector search.</summary>
    public bool Vector { get; init; }

    /// <summary>Full-text search — natively, or through the in-memory fallback.</summary>
    public bool FullText { get; init; }

    /// <summary>Append-only system-time history.</summary>
    public bool Temporal { get; init; }

    /// <summary>Blob payloads in a sidecar.</summary>
    public bool Blobs { get; init; }

    /// <summary>Computed (derived) properties.</summary>
    public bool ComputedProperties { get; init; }
}
