using ShinyDocDbMyAdmin.Providers;

namespace ShinyDocDbMyAdmin.Models;

/// <summary>
/// The assistant's configuration for one connection. <see cref="ApiKey"/> is held encrypted and
/// only decrypted at the point a chat client is built.
/// </summary>
/// <remarks>
/// A side document keyed by <see cref="ProfileId"/> rather than a field on
/// <see cref="ConnectionProfile"/>, following <c>SavedQuery</c>. That is not tidiness: writing a
/// profile goes through <c>ProfileStore.AssertNotProvided</c>, so a host-provided (Aspire)
/// connection cannot have its profile document saved at all. Keeping AI settings in their own
/// collection means those connections can be given an assistant like any other.
/// </remarks>
public class AiConnectionSettings
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");

    /// <summary>The connection this configuration belongs to.</summary>
    public string ProfileId { get; set; } = "";

    public AiProviderKind Provider { get; set; }

    /// <summary>Model id, or on Azure the deployment name.</summary>
    public string Model { get; set; } = "";

    /// <summary>Resource address or base URL. Null where the SDK's own default is correct.</summary>
    public string? Endpoint { get; set; }

    /// <summary>Ciphertext. Use <c>ProfileStore.RevealApiKey</c> to get the usable value.</summary>
    public string? ApiKey { get; set; }

    /// <summary>
    /// Off by default and switchable without discarding the configuration, so turning the assistant
    /// off for a connection does not mean re-entering a key to turn it back on.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Lets the assistant create new documents on this connection. Off by default: the read surface
    /// is the safe default, and enabling a write is a deliberate per-connection decision. The
    /// underlying write path still refuses when the connection profile itself is read-only.
    /// </summary>
    public bool AllowInsert { get; set; }

    /// <summary>
    /// Lets the assistant modify existing documents on this connection. Off by default. Same
    /// interaction with the profile's <c>ReadOnly</c> flag as <see cref="AllowInsert"/>.
    /// </summary>
    public bool AllowUpdate { get; set; }

    /// <summary>
    /// Lets the assistant delete documents on this connection. Off by default. Same interaction
    /// with the profile's <c>ReadOnly</c> flag as <see cref="AllowInsert"/>.
    /// </summary>
    public bool AllowDelete { get; set; }

    /// <summary>True when any of the write tools is opted in on this connection.</summary>
    public bool AllowsAnyWrite => this.AllowInsert || this.AllowUpdate || this.AllowDelete;

    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? UpdatedAt { get; set; }

    /// <summary>True when there is enough here to build a client.</summary>
    public bool IsUsable()
    {
        if (!this.Enabled || string.IsNullOrWhiteSpace(this.Model))
            return false;

        var descriptor = AiProviderCatalog.Get(this.Provider);
        if (descriptor.RequiresApiKey && string.IsNullOrWhiteSpace(this.ApiKey))
            return false;

        return !descriptor.RequiresEndpoint || !string.IsNullOrWhiteSpace(this.Endpoint);
    }
}
