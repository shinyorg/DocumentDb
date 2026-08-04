namespace Shiny.DocumentDb.Internal;

/// <summary>
/// The stored form of an encrypted value: <c>enc:&lt;version&gt;:&lt;keyId&gt;:&lt;base64 payload&gt;</c>.
/// </summary>
/// <remarks>
/// It is a JSON string on purpose — every provider stores one, backup/replication move it verbatim, and an
/// operator scanning a table can see at a glance which columns are protected and under which key.
/// </remarks>
static class EncryptionEnvelope
{
    public const string Prefix = "enc:";
    public const int CurrentVersion = 1;

    public static string Format(string keyId, ReadOnlySpan<byte> payload)
        => String.Concat(Prefix, CurrentVersion.ToString(), ":", keyId, ":", Convert.ToBase64String(payload));

    /// <summary>
    /// A cheap shape test used on every read — a value that is not an envelope is pre-encryption plaintext and is
    /// returned as-is, so turning encryption on for a populated store does not break its existing documents.
    /// </summary>
    public static bool IsEnvelope(string? value)
    {
        if (value == null || !value.StartsWith(Prefix, StringComparison.Ordinal))
            return false;

        // The version segment has to parse as a number before this is treated as an envelope, so an ordinary
        // value that happens to start with "enc:" is read back as the plaintext it is.
        var afterPrefix = value.AsSpan(Prefix.Length);
        var versionEnd = afterPrefix.IndexOf(':');
        if (versionEnd <= 0 || !Int32.TryParse(afterPrefix[..versionEnd], out _))
            return false;

        // …and a key id and a payload have to follow it.
        return afterPrefix[(versionEnd + 1)..].IndexOf(':') > 0;
    }

    public static (string KeyId, byte[] Payload) Parse(string envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        // enc : version : keyId : base64  — the payload is split off last so a base64 '+' or '/' is never an issue
        // and a key id containing ':' is rejected at construction rather than mis-parsed here.
        var parts = envelope.Split(':', 4);
        if (parts.Length != 4 || parts[0] != "enc")
            throw new FormatException("The value is not a Shiny.DocumentDb encryption envelope.");

        if (!Int32.TryParse(parts[1], out var version) || version != CurrentVersion)
            throw new FormatException($"Unsupported encryption envelope version '{parts[1]}'.");

        return (parts[2], Convert.FromBase64String(parts[3]));
    }
}
