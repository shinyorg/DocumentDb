using ShinyDocDbMyAdmin.Providers;

namespace ShinyDocDbMyAdmin.Models;

/// <summary>
/// The exported connection list. Plain JSON, so it can be read, diffed and edited before it is
/// imported somewhere else.
/// </summary>
/// <remarks>
/// <para>
/// Secrets are <b>left out by default</b>. A connection string is a credential, and an export that
/// carries them is a credential file - something you would not want to drop in a ticket, a chat or a
/// repository, which is exactly what an export invites. The default bundle describes which
/// connections exist and how they are configured; the person importing supplies the secrets.
/// </para>
/// <para>
/// Opting in encrypts them under a passphrase the exporter types, not under the instance's own key.
/// The instance key would make the file useless anywhere else - the one place an export is worth
/// having - so a portable secret needs a portable key.
/// </para>
/// </remarks>
public class ConnectionBundle
{
    /// <summary>Bumped when the shape changes, so an importer can refuse what it cannot read.</summary>
    public int Version { get; set; } = CurrentVersion;

    public const int CurrentVersion = 1;

    public DateTimeOffset ExportedAt { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>Free text, so a file found later says where it came from.</summary>
    public string? Source { get; set; }

    /// <summary>True when the secret fields hold passphrase-encrypted values rather than nulls.</summary>
    public bool IncludesSecrets { get; set; }

    /// <summary>How the passphrase was stretched, recorded so the format can change without guessing.</summary>
    public string? KeyDerivation { get; set; }

    /// <summary>Base64 KDF salt. Per export, so the same passphrase does not produce the same key twice.</summary>
    public string? Salt { get; set; }

    public List<BundledConnection> Connections { get; set; } = [];
}

/// <summary>One connection in an exported bundle.</summary>
public class BundledConnection
{
    /// <summary>
    /// Carried so a re-import onto the same instance can recognise the connection it came from
    /// rather than duplicating it. Import never trusts it for anything else.
    /// </summary>
    public string Id { get; set; } = "";

    public string Name { get; set; } = "";
    public ProviderKind Provider { get; set; }
    public bool ReadOnly { get; set; }

    /// <summary>Ciphertext under the export passphrase, or null when secrets were left out.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Ciphertext under the export passphrase. Only ever set for SQLCipher.</summary>
    public string? Password { get; set; }

    /// <summary>
    /// Set when the source connection pointed at a file uploaded through the UI. The file itself is
    /// not in the bundle - it is a database, not a setting - so this is a note to the importer about
    /// why the path will not resolve.
    /// </summary>
    public string? UploadedFileName { get; set; }

    public BundledAiSettings? Ai { get; set; }
    public List<BundledQuery> SavedQueries { get; set; } = [];
}

/// <summary>The assistant configuration travelling with its connection.</summary>
public class BundledAiSettings
{
    public AiProviderKind Provider { get; set; }
    public string Model { get; set; } = "";
    public string? Endpoint { get; set; }
    public bool Enabled { get; set; }

    /// <summary>Ciphertext under the export passphrase, or null when secrets were left out.</summary>
    public string? ApiKey { get; set; }
}

public class BundledQuery
{
    public string Name { get; set; } = "";
    public string Sql { get; set; } = "";
}
