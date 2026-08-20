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

    /// <summary>
    /// Keys for <b>reading</b> field-level encrypted values. Ciphertext; use <c>IProfileStore.Reveal</c>.
    /// Empty by default, and the tool is fully functional without them - it reads, describes and protects
    /// envelopes with no key at all.
    /// </summary>
    /// <remarks>
    /// Supplying these puts data keys next to a connection string, in the same store, under the same
    /// instance key. That is a real trade an operator may want to make on a staging database and would
    /// never make on production, so it is per connection, opt-in, and closed entirely in demo mode. The
    /// tool never uses them to <i>write</i> an envelope, and never sends a decrypted value to the AI
    /// assistant.
    /// </remarks>
    public List<EncryptionKeyEntry> EncryptionKeys { get; set; } = [];

    /// <summary>
    /// Document types this connection's application has configured for soft delete, and where each keeps its
    /// flag. Declared by the operator, because <b>nothing in the database records it</b>:
    /// <c>AddSoftDelete</c> registers an interceptor and a query filter, and the flag is an ordinary JSON
    /// property - there is no column, table or index for the tool to read.
    /// </summary>
    /// <remarks>
    /// Until a type is declared here, this tool's delete is a real <c>DELETE</c> - which for an application
    /// that only ever flags is a broken invariant with no warning. Declaring a type turns the delete button
    /// into a flag write, gives Browse its live / deleted / all filter, and puts <c>soft delete</c> in the
    /// database verdict. The Structure tab suggests candidates; confirming one writes an entry here.
    /// </remarks>
    public List<SoftDeleteFlag> SoftDeleteFlags { get; set; } = [];

    /// <summary>
    /// Hides tables that are neither documents nor DocumentDb sidecars from the overview. On by default, so
    /// "ignore the tables that aren't ours" is a stated behaviour rather than an accident of filtering -
    /// which matters when this tool is pointed at a schema it shares with an application's own tables.
    /// </summary>
    public bool HideForeignTables { get; set; } = true;

    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? LastOpenedAt { get; set; }
}

/// <summary>
/// One declared soft-delete flag: the type, the JSON path its flag lives at, and which of the two shapes
/// <c>AddSoftDelete</c> accepts it is - which is what tells a restore whether to write <c>false</c> or
/// <c>null</c>.
/// </summary>
public class SoftDeleteFlag
{
    /// <summary>The stored <c>TypeName</c>, exactly as it appears in the documents table.</summary>
    public string TypeName { get; set; } = "";

    /// <summary>Dotted JSON path to the flag inside the document body, e.g. <c>isDeleted</c>.</summary>
    public string PropertyPath { get; set; } = "";

    public SoftDeleteFlagKind FlagKind { get; set; } = SoftDeleteFlagKind.Boolean;
}

/// <summary>The two flag shapes <c>SoftDeleteMapping.Build</c> accepts, and nothing else.</summary>
public enum SoftDeleteFlagKind
{
    /// <summary>A <c>bool</c>: deleted is <c>true</c>, restored is <c>false</c>.</summary>
    Boolean,

    /// <summary>A nullable timestamp: deleted is the time of deletion, restored is <c>null</c>.</summary>
    Timestamp
}

/// <summary>One entry in a connection's read-only key ring.</summary>
public class EncryptionKeyEntry
{
    /// <summary>The key id as it appears in the envelope - the <c>k1</c> in <c>enc:1:k1:…</c>.</summary>
    public string KeyId { get; set; } = "";

    /// <summary>Base64 AES-256 key material, <c>SecretProtector</c>-wrapped at rest.</summary>
    public string Key { get; set; } = "";
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
