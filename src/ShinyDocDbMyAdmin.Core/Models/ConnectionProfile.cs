using ShinyDocDbMyAdmin.Providers;

namespace ShinyDocDbMyAdmin.Models;

/// <summary>
/// A saved connection. Stored as a document in the tool's own local Shiny.DocumentDb SQLite store -
/// <see cref="ConnectionString"/> and <see cref="Password"/> are held encrypted and only decrypted
/// when a connection is actually opened.
/// </summary>
public class ConnectionProfile
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public string Name { get; set; } = "";
    public ProviderKind Provider { get; set; }

    /// <summary>Ciphertext. Use <c>IProfileStore.Reveal</c> to get the usable value.</summary>
    public string ConnectionString { get; set; } = "";

    /// <summary>Ciphertext, only set for SQLCipher. Use <c>IProfileStore.Reveal</c>.</summary>
    public string? Password { get; set; }

    /// <summary>
    /// Set when the database file was uploaded through the UI rather than referenced in place -
    /// deleting the profile then also deletes the managed file.
    /// </summary>
    public string? UploadedFileName { get; set; }

    /// <summary>Blocks every write path in the UI. Set it on anything pointed at production.</summary>
    public bool ReadOnly { get; set; }

    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? LastOpenedAt { get; set; }
}

/// <summary>Plaintext form of a profile, produced only at the point a connection is opened.</summary>
public sealed record ResolvedProfile(
    ConnectionProfile Profile,
    string ConnectionString,
    string? Password,
    string? FilePath
)
{
    public string Id => this.Profile.Id;
    public string Name => this.Profile.Name;
    public ProviderKind Provider => this.Profile.Provider;
    public bool ReadOnly => this.Profile.ReadOnly;
    public ProviderDescriptor Descriptor => ProviderCatalog.Get(this.Profile.Provider);
}
